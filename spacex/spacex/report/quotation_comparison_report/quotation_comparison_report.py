import frappe
from frappe import _
import logging

# Set up logging for debugging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def execute(filters=None):
    try:
        data, supplier_count = get_data(filters or {})
        columns = get_columns(supplier_count)
        return columns, data
    except Exception as e:
        logger.error(f"Error executing report: {str(e)}")
        frappe.throw(_("An error occurred while generating the report: {0}").format(str(e)))

def get_columns(supplier_count):
    columns = [
        {"label": _("Item Description"), "fieldname": "description", "fieldtype": "Data", "width": 200},
        {"label": _("Qty"), "fieldname": "qty", "fieldtype": "Float", "width": 80},
        {"label": _("Units"), "fieldname": "uom", "fieldtype": "Data", "width": 80},
    ]
    
    for idx in range(1, supplier_count + 1):
        columns.extend([
            {"label": _(f"Partner Name {idx}"), "fieldname": f"partner_name_{idx}", "fieldtype": "Data", "width": 200},
            {"label": _(f"Quote Ref No. {idx}"), "fieldname": f"quote_ref_no_{idx}", "fieldtype": "Link", "options": "Supplier Quotation", "width": 200},
            {"label": _(f"Dt. {idx}"), "fieldname": f"date_{idx}", "fieldtype": "Date", "width": 150},
            {"label": _(f"Supply Rate {idx}"), "fieldname": f"rate_{idx}", "fieldtype": "Currency", "width": 130},
            {"label": _(f"Supply Amount {idx}"), "fieldname": f"amount_{idx}", "fieldtype": "Currency", "width": 150},
            {"label": _(f"Label {idx}"), "fieldname": f"label_{idx}", "fieldtype": "Data", "width": 80},
        ])
    
    return columns

def get_data(filters):
    data = []
    conditions = get_conditions(filters)
    
    try:
        rfq_items = frappe.db.sql("""
            SELECT 
                rfi.description, 
                rfi.uom, 
                rfi.qty, 
                rfi.idx as sr_no,
                rfi.item_code
            FROM `tabRequest for Quotation Item` rfi
            JOIN `tabRequest for Quotation` rfq ON rfq.name = rfi.parent
            WHERE rfq.docstatus = 1
            ORDER BY rfi.idx
        """, filters, as_dict=1)
        logger.debug(f"Fetched {len(rfq_items)} RFQ items")
    except Exception as e:
        logger.error(f"Error fetching RFQ items: {str(e)}")
        frappe.throw(_("Failed to fetch RFQ items: {0}").format(str(e)))
    
    if not rfq_items:
        logger.warning("No RFQ items found for the given filters")
        return data, 0

    total_qty = sum(item.qty or 0 for item in rfq_items)

    try:
        supplier_quotations_meta = frappe.db.sql("""
            SELECT DISTINCT
                sq.name as quote_ref_no,
                sq.supplier as partner_name,
                sq.transaction_date as date,
                COALESCE(sq.grand_total, 0) as supplier_total
            FROM `tabSupplier Quotation` sq
            JOIN `tabRequest for Quotation Supplier` rfqs ON rfqs.supplier = sq.supplier
            JOIN `tabRequest for Quotation` rfq ON rfq.name = rfqs.parent
            WHERE sq.docstatus = 1 AND rfq.docstatus = 1 {conditions}
            ORDER BY sq.supplier
        """.format(conditions=conditions), filters, as_dict=1)
        logger.debug(f"Fetched {len(supplier_quotations_meta)} distinct Supplier Quotations")
    except Exception as e:
        logger.error(f"Error fetching Supplier Quotations metadata: {str(e)}")
        frappe.throw(_("Failed to fetch Supplier Quotations metadata: {0}").format(str(e)))

    if not supplier_quotations_meta:
        logger.warning("No Supplier Quotations found for the given filters")
        return data, 0

    supplier_quotation_names = [sq["quote_ref_no"] for sq in supplier_quotations_meta]
    try:
        supplier_quotation_items = frappe.db.sql("""
            SELECT 
                sqi.parent as quote_ref_no,
                sqi.description,
                sqi.rate,
                sqi.amount,
                sqi.item_code
            FROM `tabSupplier Quotation Item` sqi
            WHERE sqi.parent IN %s
            ORDER BY sqi.idx
        """, (tuple(supplier_quotation_names),), as_dict=1)
        logger.debug(f"Fetched {len(supplier_quotation_items)} Supplier Quotation items")
    except Exception as e:
        logger.error(f"Error fetching Supplier Quotation items: {str(e)}")
        frappe.throw(_("Failed to fetch Supplier Quotation items: {0}").format(str(e)))

    supplier_data = {}
    for sq_meta in supplier_quotations_meta:
        supplier = sq_meta["partner_name"]
        supplier_data[supplier] = {
            "quote_ref_no": sq_meta["quote_ref_no"],
            "date": sq_meta["date"],
            "items": [],
            "total": sq_meta["supplier_total"] or 0
        }

    for sqi in supplier_quotation_items:
        quote_ref_no = sqi["quote_ref_no"]
        supplier = next(
            (s for s, data in supplier_data.items() if data["quote_ref_no"] == quote_ref_no),
            None
        )
        if supplier:
            existing_item = next(
                (item for item in supplier_data[supplier]["items"] if item["item_code"] == sqi["item_code"]),
                None
            )
            if not existing_item:
                supplier_data[supplier]["items"].append({
                    "description": sqi["description"],
                    "item_code": sqi["item_code"],
                    "rate": sqi["rate"] or 0,
                    "amount": sqi["amount"] or 0
                })

    for supplier in supplier_data:
        supplier_data[supplier]["total_rate"] = sum(item["rate"] or 0 for item in supplier_data[supplier]["items"])
        logger.debug(f"Supplier: {supplier}, Total Rate: {supplier_data[supplier]['total_rate']}")

    sorted_suppliers = sorted(
        supplier_data.items(),
        key=lambda x: x[1]["total"] or 0
    )
    for idx, (supplier, _) in enumerate(sorted_suppliers, 1):
        supplier_data[supplier]["label"] = f"L{idx}"

    supplier_count = len(sorted_suppliers)
    if supplier_count == 0:
        logger.warning("No suppliers found after processing")
        return data, 0

    for item in rfq_items:
        row = {
            "description": item.description or "",
            "qty": item.qty or 0,
            "uom": item.uom or "",
        }
        for idx, (supplier, s_data) in enumerate(sorted_suppliers, 1):
            item_data = next(
                (i for i in s_data["items"] if i["item_code"] == item.item_code),
                {"rate": 0, "amount": 0}
            )
            row.update({
                f"partner_name_{idx}": supplier,
                f"quote_ref_no_{idx}": s_data["quote_ref_no"],
                f"date_{idx}": s_data["date"],
                f"rate_{idx}": item_data.get("rate", 0),
                f"amount_{idx}": item_data.get("amount", 0),
                f"label_{idx}": s_data["label"]
            })
        data.append(row)

    total_row = {
        "description": "TOTAL AMOUNT",
        "qty": total_qty,
        "uom": ""
    }
    for idx, (supplier, s_data) in enumerate(sorted_suppliers, 1):
        total_row.update({
            f"rate_{idx}": s_data["total_rate"] or 0,
            f"amount_{idx}": s_data["total"] or 0,
            f"label_{idx}": s_data["label"]
        })
    data.append(total_row)

    return data, supplier_count

def get_conditions(filters):
    conditions = []
    if filters.get("rfq"):
        conditions.append("rfq.name = %(rfq)s")
    if filters.get("from_date"):
        conditions.append("sq.transaction_date >= %(from_date)s")
    if filters.get("to_date"):
        conditions.append("sq.transaction_date <= %(to_date)s")
    if filters.get("supplier"):
        conditions.append("sq.supplier = %(supplier)s")
    
    return " AND " + " AND ".join(conditions) if conditions else ""


