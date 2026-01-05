# reports/invoice_lines_all.py
import io
import csv
import json
import requests
from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse, JSONResponse

router = APIRouter(prefix="/reports/invoice_lines_all", tags=["reports-invoice-lines-all"])


def qbo_query_all(
    realm_id: str,
    query: str,
    access_token: str,
    qbo_api_base: str,
    page_size: int = 1000,
) -> list[dict]:
    results: list[dict] = []
    start = 1

    while True:
        paged_query = f"{query} STARTPOSITION {start} MAXRESULTS {page_size}"
        url = f"{qbo_api_base}/v3/company/{realm_id}/query?minorversion=75"

        r = requests.post(
            url,
            headers={
                "Authorization": f"Bearer {access_token}",
                "Accept": "application/json",
                "Content-Type": "text/plain",
            },
            data=paged_query,
            timeout=60,
        )

        if r.status_code == 401:
            raise RuntimeError(f"AUTH_401: {r.text}")
        if r.status_code >= 400:
            raise RuntimeError(f"QBO_QUERY_FAILED ({r.status_code}): {r.text}")

        payload = r.json() if r.content else {}
        qr = payload.get("QueryResponse", {})
        batch = qr.get("Invoice", []) or []
        results.extend(batch)

        if len(batch) < page_size:
            break

        start += page_size

    return results


def safe_json(val: Any) -> str:
    # For CSV: flatten nested dict/list safely into a JSON string
    return json.dumps(val, ensure_ascii=False, default=str) if val is not None else ""


def flatten_invoice_lines(invoice: dict) -> list[dict]:
    """
    Returns a list of rows (dicts), one per invoice line.
    Each row includes:
      - Parent invoice fields (InvoiceId, DocNumber, TxnDate, CustomerRef, etc.)
      - Full Line object (Line_json)
      - Some common extracted fields for convenience (Amount, DetailType, ItemRef, Qty, UnitPrice, etc.)
    """
    rows: list[dict] = []

    inv_id = invoice.get("Id", "")
    doc = invoice.get("DocNumber", "")
    txn_date = invoice.get("TxnDate", "")
    customer_ref = invoice.get("CustomerRef") or {}
    customer_id = customer_ref.get("value", "")
    customer_name = customer_ref.get("name", "")
    po_num = invoice.get("PurchaseOrderRef", "")
    meta = invoice.get("MetaData") or {}

    lines = invoice.get("Line") or []
    if not isinstance(lines, list):
        return rows

    for idx, line in enumerate(lines, start=1):
        detail_type = line.get("DetailType", "")
        amount = line.get("Amount", "")
        line_id = line.get("Id", "")  # may be absent

        # Try to extract item-ish fields when SalesItemLineDetail exists
        sales_detail = line.get("SalesItemLineDetail") or {}
        item_ref = sales_detail.get("ItemRef") or {}
        item_id = item_ref.get("value", "")
        item_name = item_ref.get("name", "")
        qty = sales_detail.get("Qty", "")
        unit_price = sales_detail.get("UnitPrice", "")
        tax_code_ref = sales_detail.get("TaxCodeRef") or {}
        tax_code = tax_code_ref.get("value", "")

        row = {
            # Parent invoice identifiers
            "RealmId": "",  # filled later in endpoint
            "InvoiceId": inv_id,
            "DocNumber": doc,
            "TxnDate": txn_date,
            "CustomerId": customer_id,
            "CustomerName": customer_name,

            # Helpful invoice-level context (optional)
            "PurchaseOrderRef": po_num,
            "InvoiceMeta_CreateTime": meta.get("CreateTime", ""),
            "InvoiceMeta_LastUpdatedTime": meta.get("LastUpdatedTime", ""),

            # Line identifiers / ordering
            "LineIndex": idx,
            "LineId": line_id,

            # Common line fields extracted
            "DetailType": detail_type,
            "Amount": amount,
            "Description": line.get("Description", ""),

            # Common SalesItemLineDetail extracted
            "ItemId": item_id,
            "ItemName": item_name,
            "Qty": qty,
            "UnitPrice": unit_price,
            "TaxCode": tax_code,

            # Raw line object
            "Line_json": line,

            # Parent raw invoice (optional; can be big—keep for now since you said “all”)
            "Invoice_json": invoice,
        }

        rows.append(row)

    return rows


@router.get("/year/{year}")
def download_invoice_lines_for_year(request: Request, realmId: str, year: int, format: str = "json"):
    get_valid_access_token = request.app.state.get_valid_access_token
    qbo_api_base = request.app.state.qbo_api_base

    # 1) Get valid token (auto-refresh inside your existing helper)
    try:
        access_token = get_valid_access_token(realmId)
    except RuntimeError as e:
        msg = str(e)
        if msg.startswith("RECONNECT_REQUIRED"):
            return JSONResponse(
                {"error": "reconnect_required", "connect_url": "/connect", "message": msg},
                status_code=401,
            )
        return JSONResponse({"error": "auth_failed", "message": msg}, status_code=500)

    # 2) Query all invoices in the year, sorted by TxnDate desc
    start_date = f"{year}-01-01"
    end_date = f"{year}-12-31"
    q = (
        "SELECT * FROM Invoice "
        f"WHERE TxnDate >= '{start_date}' AND TxnDate <= '{end_date}' "
        "ORDER BY TxnDate DESC"
    )

    invoices = qbo_query_all(realmId, q, access_token, qbo_api_base)

    # 3) Flatten lines
    all_lines: list[dict] = []
    for inv in invoices:
        rows = flatten_invoice_lines(inv)
        for r in rows:
            r["RealmId"] = realmId
        all_lines.extend(rows)

    # 4) Return JSON (default) or CSV
    if format.lower() == "json":
        buf = io.BytesIO()
        buf.write(json.dumps(all_lines, indent=2, ensure_ascii=False, default=str).encode("utf-8"))
        buf.seek(0)
        filename = f"invoice_lines_all_{year}_{realmId}.json"
        return StreamingResponse(
            buf,
            media_type="application/json",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    if format.lower() == "csv":
        # For “all”, CSV needs a stable set of columns.
        # We'll keep key extracted columns + JSON blobs for the complex parts.
        fieldnames = [
            "RealmId",
            "InvoiceId",
            "DocNumber",
            "TxnDate",
            "CustomerId",
            "CustomerName",
            "PurchaseOrderRef",
            "InvoiceMeta_CreateTime",
            "InvoiceMeta_LastUpdatedTime",
            "LineIndex",
            "LineId",
            "DetailType",
            "Amount",
            "Description",
            "ItemId",
            "ItemName",
            "Qty",
            "UnitPrice",
            "TaxCode",
            "Line_json",
            "Invoice_json",
        ]

        text_buf = io.StringIO()
        writer = csv.DictWriter(text_buf, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()

        for r in all_lines:
            row = dict(r)
            row["Line_json"] = safe_json(r.get("Line_json"))
            row["Invoice_json"] = safe_json(r.get("Invoice_json"))
            writer.writerow(row)

        data = text_buf.getvalue().encode("utf-8-sig")
        buf = io.BytesIO(data)
        buf.seek(0)

        filename = f"invoice_lines_all_{year}_{realmId}.csv"
        return StreamingResponse(
            buf,
            media_type="text/csv",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    return JSONResponse({"error": "invalid_format", "allowed": ["json", "csv"]}, status_code=400)
