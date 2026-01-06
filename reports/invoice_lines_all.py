# reports/invoice_lines_all.py
import io
import csv
import json
import requests
from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse, JSONResponse

router = APIRouter(prefix="/reports/invoice_lines_all", tags=["reports-invoice-lines-all"])


def ref_value(ref: dict | None) -> str:
    if isinstance(ref, dict):
        return (ref.get("value") or "").strip()
    return ""

def ref_name(ref: dict | None) -> str:
    if isinstance(ref, dict):
        return (ref.get("name") or "").strip()
    return ""

def ref_json(ref: dict | None) -> str:
    # Optional: if you want a JSON string in CSV too
    import json
    return json.dumps(ref, ensure_ascii=False) if isinstance(ref, dict) else ""

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

        print("QBO QUERY (first page):", paged_query[:500]) # For debugging ONLY

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
    rows: list[dict] = []

    # Parent invoice fields requested
    invoice_id = invoice.get("Id", "")
    doc_number = invoice.get("DocNumber", "")
    txn_date = invoice.get("TxnDate", "")

    customer_ref = invoice.get("CustomerRef") or {}
    customer_name = customer_ref.get("name", "")

    # Extract P.O. Number custom field value from CustomField[]
    po_number_id = ""
    custom_fields = invoice.get("CustomField") or []
    if isinstance(custom_fields, list):
        for cf in custom_fields:
            if not isinstance(cf, dict):
                continue
            # QBO often stores the label in Name (or DefinitionId ties to config)
            if (cf.get("Name") or "").strip().lower() in ("p.o. number", "po number", "p.o. #", "po #"):
                # Value can be in StringValue for text custom fields
                po_number_id = cf.get("StringValue") or cf.get("value") or ""
                break

    # keep the rest as you already had
    meta = invoice.get("MetaData") or {}
    lines = invoice.get("Line") or []
    if not isinstance(lines, list):
        return rows

    for idx, line in enumerate(lines, start=1):
        # ... (no change to your line parsing)

        row = {
            # Requested parent fields:
            "Id": invoice_id,
            "DocNumber": doc_number,
            "TxnDate": txn_date,
            "CustomerName": customer_name,
            "P.O. NumberId": po_number_id,
            
            # Line identifiers / ordering
            "LineIndex": idx,
            "LineId": line.get("Id", ""),

            # keep the rest of your fields...
            "DetailType": line.get("DetailType", ""),
            "Amount": line.get("Amount", ""),
            "Description": line.get("Description", ""),

            # keep your SalesItemLineDetail extraction, etc.
            # ...
            "Line_json": line,
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
        "ORDERBY TxnDate DESC"
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
            # requested invoice parent fields
            "Id",
            "DocNumber",
            "TxnDate",
            "CustomerName",
            "P.O. NumberId",
            "SalesTermName",
        
            # rest of your line fields
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
            row["SalesTermRef"] = invoice.get("SalesTermRef")
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
