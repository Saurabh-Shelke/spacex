import frappe
from frappe import _
from frappe.utils import flt
import logging

# Set up logging for debugging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def execute(filters=None):
    try:
        data, supplier_quotation_count = get_data(filters or {})
        columns = get_columns(supplier_quotation_count)

        # Filter out rows with all zero rate and amount when RFQ filter is applied
        if filters.get("rfq"):
            data = filter_zero_quotation_rows(data, supplier_quotation_count)

        return columns, data
    except Exception as e:
        logger.error(f"Error executing report: {str(e)}")
        frappe.throw(_("An error occurred while generating the report: {0}").format(str(e)))

def get_columns(supplier_quotation_count):
    columns = [
        {"label": _("Item Code"), "fieldname": "item_code", "fieldtype": "Link", "options": "Item", "width": 150},
        {"label": _("Item Description"), "fieldname": "description", "fieldtype": "Data", "width": 200},
        {"label": _("Qty"), "fieldname": "qty", "fieldtype": "Float", "width": 80},
        {"label": _("Units"), "fieldname": "uom", "fieldtype": "Data", "width": 80},
    ]
    
    for idx in range(1, supplier_quotation_count + 1):
        columns.extend([
            {"label": _(f"Partner Name {idx}"), "fieldname": f"partner_name_{idx}", "fieldtype": "Data", "width": 150},
            {"label": _(f"Quote Ref No. {idx}"), "fieldname": f"quote_ref_no_{idx}", "fieldtype": "Link", "options": "Supplier Quotation", "width": 200},
            {"label": _(f"Dt. {idx}"), "fieldname": f"date_{idx}", "fieldtype": "Date", "width": 130},
            {"label": _(f"Rate {idx}"), "fieldname": f"rate_{idx}", "fieldtype": "Currency", "width": 100},
            {"label": _(f"Amount {idx}"), "fieldname": f"amount_{idx}", "fieldtype": "Currency", "width": 120},
            {"label": _(f"Label {idx}"), "fieldname": f"label_{idx}", "fieldtype": "Data", "width": 80},
        ])
    
    return columns

def filter_zero_quotation_rows(data, supplier_quotation_count):
    filtered_data = []
    for row in data:
        # Skip the total row
        if row.get("description") == "TOTAL AMOUNT":
            continue
        non_zero_found = False
        for idx in range(1, supplier_quotation_count + 1):
            rate = flt(row.get(f"rate_{idx}"))
            amount = flt(row.get(f"amount_{idx}"))
            if rate != 0 or amount != 0:
                non_zero_found = True
                break
        if non_zero_found:
            filtered_data.append(row)
    
    # Recalculate total row
    total_row = {
        "item_code": "",
        "description": "TOTAL AMOUNT",
        "qty": sum(row.get("qty", 0) for row in filtered_data),
        "uom": ""
    }
    for idx in range(1, supplier_quotation_count + 1):
        total_row.update({
            f"rate_{idx}": sum(flt(row.get(f"rate_{idx}")) for row in filtered_data),
            f"amount_{idx}": sum(flt(row.get(f"amount_{idx}")) for row in filtered_data),
            f"label_{idx}": filtered_data[0].get(f"label_{idx}") if filtered_data else "",
        })

    filtered_data.append(total_row)
    return filtered_data

# The get_data and get_conditions functions remain unchanged from your original code
# Copy them below exactly as in your original implementation

# get_data(...)
# get_conditions(...)

def get_data(filters):
    data = []
    conditions = get_conditions(filters)
    
    try:
        # Get unique RFQ items with item_code and RFQ transaction_date
        # Remove rfq filter to fetch all RFQ items, filter will be applied in supplier_quotations_meta
        rfq_query = """
            SELECT DISTINCT
                rfi.item_code,
                rfi.description, 
                rfi.uom, 
                SUM(rfi.qty) as qty,
                rfq.name as rfq_name,
                rfq.transaction_date as rfq_date
            FROM `tabRequest for Quotation Item` rfi
            JOIN `tabRequest for Quotation` rfq ON rfq.name = rfi.parent
            WHERE rfq.docstatus = 1
            GROUP BY rfi.item_code, rfi.description, rfi.uom, rfq.name, rfq.transaction_date
            ORDER BY rfi.item_code
        """
        rfq_items = frappe.db.sql(rfq_query, as_dict=1)
        logger.debug(f"Fetched {len(rfq_items)} unique RFQ items: {rfq_items}")
    except Exception as e:
        logger.error(f"Error fetching RFQ items: {str(e)}")
        frappe.throw(_("Failed to fetch RFQ items: {0}").format(str(e)))
    
    if not rfq_items:
        logger.warning("No RFQ items found for the given filters")
        return data, 0

    total_qty = sum(item.qty or 0 for item in rfq_items)

    # Map RFQ names to their dates and items
    rfq_date_map = {}
    rfq_item_map = {}
    for item in rfq_items:
        rfq_name = item.rfq_name
        if rfq_name not in rfq_date_map:
            rfq_date_map[rfq_name] = item.rfq_date
        if rfq_name not in rfq_item_map:
            rfq_item_map[rfq_name] = []
        rfq_item_map[rfq_name].append(item)
    logger.debug(f"RFQ Date Map: {rfq_date_map}")
    logger.debug(f"RFQ Item Map: {rfq_item_map}")

    try:
        # Fetch all Supplier Quotations with their linked RFQ via Supplier Quotation Item
        supplier_quotations_meta = frappe.db.sql("""
            SELECT DISTINCT
                sq.name as quote_ref_no,
                sq.supplier as partner_name,
                COALESCE(sq.grand_total, 0) as supplier_total,
                sqi.request_for_quotation as rfq_name
            FROM `tabSupplier Quotation` sq
            JOIN `tabSupplier Quotation Item` sqi ON sqi.parent = sq.name
            WHERE sq.docstatus = 1 AND sqi.request_for_quotation IS NOT NULL {conditions}
            ORDER BY sq.name
        """.format(conditions=conditions), filters, as_dict=1)
        logger.debug(f"Fetched {len(supplier_quotations_meta)} Supplier Quotations: {supplier_quotations_meta}")
    except Exception as e:
        logger.error(f"Error fetching Supplier Quotations metadata: {str(e)}")
        frappe.throw(_("Failed to fetch Supplier Quotations metadata: {0}").format(str(e)))

    if not supplier_quotations_meta:
        logger.warning("No Supplier Quotations found for the given filters")
        return data, 0

    # Add RFQ date to supplier_quotations_meta
    for sq in supplier_quotations_meta:
        rfq_name = sq.get("rfq_name")
        sq["date"] = rfq_date_map.get(rfq_name)
        if not sq["date"]:
            logger.warning(f"No RFQ date found for Supplier Quotation {sq['quote_ref_no']}, RFQ {rfq_name}")

    supplier_quotation_names = [sq["quote_ref_no"] for sq in supplier_quotations_meta]
    try:
        supplier_quotation_items = frappe.db.sql("""
            SELECT 
                sqi.parent as quote_ref_no,
                sqi.item_code,
                sqi.description,
                sqi.rate,
                sqi.amount
            FROM `tabSupplier Quotation Item` sqi
            WHERE sqi.parent IN %s
            ORDER BY sqi.item_code
        """, (tuple(supplier_quotation_names),), as_dict=1)
        logger.debug(f"Fetched {len(supplier_quotation_items)} Supplier Quotation items: {supplier_quotation_items}")
    except Exception as e:
        logger.error(f"Error fetching Supplier Quotation items: {str(e)}")
        frappe.throw(_("Failed to fetch Supplier Quotation items: {0}").format(str(e)))

    # Use Quote Ref No as the key to handle multiple quotations
    supplier_data = {}
    for sq_meta in supplier_quotations_meta:
        quote_ref_no = sq_meta["quote_ref_no"]
        supplier_data[quote_ref_no] = {
            "partner_name": sq_meta["partner_name"],
            "quote_ref_no": quote_ref_no,
            "date": sq_meta["date"],
            "rfq_name": sq_meta["rfq_name"],
            "items": [],
            "total": sq_meta["supplier_total"] or 0
        }

    for sqi in supplier_quotation_items:
        quote_ref_no = sqi["quote_ref_no"]
        if quote_ref_no in supplier_data:
            supplier_data[quote_ref_no]["items"].append({
                "item_code": sqi["item_code"],
                "description": sqi["description"],
                "rate": sqi["rate"] or 0,
                "amount": sqi["amount"] or 0
            })

    for quote_ref_no in supplier_data:
        supplier_data[quote_ref_no]["total_rate"] = sum(item["rate"] or 0 for item in supplier_data[quote_ref_no]["items"])
        logger.debug(f"Quote Ref No: {quote_ref_no}, Total Rate: {supplier_data[quote_ref_no]['total_rate']}")

    # Sort Supplier Quotations by total amount (similar to provided code)
    sorted_supplier_quotations = sorted(
        supplier_data.items(),
        key=lambda x: x[1]["total"] or 0
    )
    supplier_quotation_count = len(sorted_supplier_quotations)
    if supplier_quotation_count == 0:
        logger.warning("No supplier quotations found after processing")
        return data, 0

    # Assign L-series labels to each Supplier Quotation
    for idx, (quote_ref_no, _) in enumerate(sorted_supplier_quotations, 1):
        supplier_data[quote_ref_no]["label"] = f"L{idx}"

    logger.debug(f"Sorted Supplier Quotations with Labels: {[(q[0], q[1]['partner_name'], q[1]['date'], q[1]['label']) for q in sorted_supplier_quotations]}")

    # Group data by item_code to avoid duplicates
    item_rows = {}
    for rfq_name, items in rfq_item_map.items():
        for item in items:
            item_code = item.item_code
            if item_code not in item_rows:
                item_rows[item_code] = {
                    "item_code": item_code,
                    "description": item.description or "",
                    "qty": 0,
                    "uom": item.uom or "",
                }
            item_rows[item_code]["qty"] += item.qty or 0

    # Populate the rows with Supplier Quotation data
    for item_code, row_data in item_rows.items():
        row = {
            "item_code": row_data["item_code"],
            "description": row_data["description"],
            "qty": row_data["qty"],
            "uom": row_data["uom"],
        }
        for idx, (quote_ref_no, s_data) in enumerate(sorted_supplier_quotations, 1):
            item_data = next(
                (i for i in s_data["items"] if i["item_code"] == item_code),
                {"rate": 0, "amount": 0}
            )
            row.update({
                f"partner_name_{idx}": s_data["partner_name"],
                f"quote_ref_no_{idx}": s_data["quote_ref_no"],
                f"date_{idx}": s_data["date"],
                f"rate_{idx}": item_data.get("rate", 0),
                f"amount_{idx}": item_data.get("amount", 0),
                f"label_{idx}": s_data["label"],
            })
        logger.debug(f"Row for item_code {item_code}: {row}")
        data.append(row)

    total_row = {
        "item_code": "",
        "description": "TOTAL AMOUNT",
        "qty": total_qty,
        "uom": ""
    }
    for idx, (quote_ref_no, s_data) in enumerate(sorted_supplier_quotations, 1):
        total_row.update({
            f"rate_{idx}": s_data["total_rate"] or 0,
            f"amount_{idx}": s_data["total"] or 0,
            f"label_{idx}": s_data["label"],
        })
    data.append(total_row)

    return data, supplier_quotation_count

def get_conditions(filters):
    conditions = []
    if filters.get("rfq"):
        conditions.append("sqi.request_for_quotation = %(rfq)s")
    if filters.get("from_date"):
        conditions.append("sq.transaction_date >= %(from_date)s")
    if filters.get("to_date"):
        conditions.append("sq.transaction_date <= %(to_date)s")
    if filters.get("supplier"):
        conditions.append("sq.supplier = %(supplier)s")
    
    return " AND " + " AND ".join(conditions) if conditions else ""
