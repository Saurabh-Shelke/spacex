frappe.query_reports["Quotation Comparison Report"] = {
    filters: [
        {
            fieldname: "rfq",
            label: "RFQ",
            fieldtype: "Link",
            options: "Request for Quotation"
        },
        {
            fieldname: "supplier",
            label: "Supplier",
            fieldtype: "Link",
            options: "Supplier"
        },
        {
            fieldname: "from_date",
            label: "From Date",
            fieldtype: "Date",
        },
        {
            fieldname: "to_date",
            label: "To Date",
            fieldtype: "Date",
			
        }
    ]
};

