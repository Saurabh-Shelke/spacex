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

        # Always replace Qty with Quoted Qty 1 values
        for row in data:
            if row.get("description") != "TOTAL AMOUNT":
                row["qty"] = row.get("quoted_qty_1", 0)

        # Always update total row qty to sum of Quoted Qty 1
        for row in data:
            if row.get("description") == "TOTAL AMOUNT":
                row["qty"] = sum(r.get("quoted_qty_1", 0) for r in data if r.get("description") != "TOTAL AMOUNT")

        columns = get_columns(supplier_quotation_count)

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
        {"label": _("Qty"), "fieldname": "qty", "fieldtype": "Float", "width": 80},  # label remains "Qty"
        {"label": _("Units"), "fieldname": "uom", "fieldtype": "Data", "width": 80},
    ]
    
    for idx in range(1, supplier_quotation_count + 1):
        columns.extend([
            {"label": _(f"Partner Name {idx}"), "fieldname": f"partner_name_{idx}", "fieldtype": "Data", "width": 150},
            # {"label": _(f"Quoted Qty {idx}"), "fieldname": f"quoted_qty_{idx}", "fieldtype": "Float", "width": 100},
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
        if row.get("description") == "TOTAL AMOUNT":
            continue
        non_zero_found = False
        for idx in range(1, supplier_quotation_count + 1):
            if flt(row.get(f"rate_{idx}")) != 0 or flt(row.get(f"amount_{idx}")) != 0:
                non_zero_found = True
                break
        if non_zero_found:
            filtered_data.append(row)

    total_row = {
        "item_code": "",
        "description": "TOTAL AMOUNT",
        "qty": sum(row.get("quoted_qty_1", 0) for row in filtered_data),
        "uom": ""
    }
    for idx in range(1, supplier_quotation_count + 1):
        total_row.update({
            f"quoted_qty_{idx}": sum(flt(row.get(f"quoted_qty_{idx}", 0)) for row in filtered_data),
            f"rate_{idx}": sum(flt(row.get(f"rate_{idx}")) for row in filtered_data),
            f"amount_{idx}": sum(flt(row.get(f"amount_{idx}")) for row in filtered_data),
            f"label_{idx}": filtered_data[0].get(f"label_{idx}") if filtered_data else "",
        })

    filtered_data.append(total_row)
    return filtered_data


def get_data(filters):
    data = []
    conditions = get_conditions(filters)

    rfq_items = frappe.db.sql("""
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
    """, as_dict=1)

    if not rfq_items:
        return data, 0

    total_qty = sum(item.qty or 0 for item in rfq_items)

    rfq_date_map, rfq_item_map = {}, {}
    for item in rfq_items:
        rfq_date_map[item.rfq_name] = item.rfq_date
        rfq_item_map.setdefault(item.rfq_name, []).append(item)

    supplier_quotations_meta = frappe.db.sql(f"""
        SELECT DISTINCT
            sq.name as quote_ref_no,
            sq.supplier as partner_name,
            COALESCE(sq.grand_total, 0) as supplier_total,
            sqi.request_for_quotation as rfq_name
        FROM `tabSupplier Quotation` sq
        JOIN `tabSupplier Quotation Item` sqi ON sqi.parent = sq.name
        WHERE sq.docstatus = 1 AND sqi.request_for_quotation IS NOT NULL {conditions}
        ORDER BY sq.name
    """, filters, as_dict=1)

    for sq in supplier_quotations_meta:
        sq["date"] = rfq_date_map.get(sq.get("rfq_name"))

    if not supplier_quotations_meta:
        return data, 0

    supplier_quotation_names = [sq["quote_ref_no"] for sq in supplier_quotations_meta]
    supplier_quotation_items = frappe.db.sql("""
        SELECT 
            sqi.parent as quote_ref_no,
            sqi.item_code,
            sqi.description,
            sqi.rate,
            sqi.amount,
            sqi.qty
        FROM `tabSupplier Quotation Item` sqi
        WHERE sqi.parent IN %s
        ORDER BY sqi.item_code
    """, (tuple(supplier_quotation_names),), as_dict=1)

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
                "amount": sqi["amount"] or 0,
                "qty": sqi["qty"] or 0
            })

    for quote_ref_no in supplier_data:
        supplier_data[quote_ref_no]["total_rate"] = sum(item["rate"] or 0 for item in supplier_data[quote_ref_no]["items"])

    sorted_supplier_quotations = sorted(
        supplier_data.items(),
        key=lambda x: x[1]["total"] or 0
    )
    supplier_quotation_count = len(sorted_supplier_quotations)

    for idx, (quote_ref_no, _) in enumerate(sorted_supplier_quotations, 1):
        supplier_data[quote_ref_no]["label"] = f"L{idx}"

    item_rows = {}
    for rfq_name, items in rfq_item_map.items():
        for item in items:
            item_code = item.item_code
            item_rows.setdefault(item_code, {
                "item_code": item_code,
                "description": item.description or "",
                "qty": 0,
                "uom": item.uom or "",
            })
            item_rows[item_code]["qty"] += item.qty or 0

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
                {"rate": 0, "amount": 0, "qty": 0}
            )
            row.update({
                f"partner_name_{idx}": s_data["partner_name"],
                f"quoted_qty_{idx}": flt(item_data.get("qty", 0)),
                f"quote_ref_no_{idx}": s_data["quote_ref_no"],
                f"date_{idx}": s_data["date"],
                f"rate_{idx}": item_data.get("rate", 0),
                f"amount_{idx}": item_data.get("amount", 0),
                f"label_{idx}": s_data["label"],
            })
        data.append(row)

    total_row = {
        "item_code": "",
        "description": "TOTAL AMOUNT",
        "qty": sum(row.get("quoted_qty_1", 0) for row in data),
        "uom": ""
    }
    for idx, (quote_ref_no, s_data) in enumerate(sorted_supplier_quotations, 1):
        total_row.update({
            f"quoted_qty_{idx}": sum(flt(row.get(f"quoted_qty_{idx}", 0)) for row in data),
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
