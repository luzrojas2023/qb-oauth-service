# reports/invoice_lines.py
import io
import csv
import json
import requests
from typing import Any
from calendar import monthrange
from datetime import date

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse, JSONResponse

router = APIRouter(prefix="/reports/invoice_lines", tags=["reports-invoice-lines"])


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

def extract_work_order(description: str) -> str:
    """
    Extracts Work Order value from multi-line description.

    Looks for: "IRT WO#:" and returns text after it up to end-of-line.
    If not found, returns "".
    """
    if not description:
        return ""

    marker = "IRT WO#:"
    idx = description.find(marker)
    if idx == -1:
        return ""

    # Take everything after the marker
    after = description[idx + len(marker):]

    # If description is multi-line, take only the first line after the marker
    # (work order is typically on the same line)
    first_line = after.splitlines()[0] if after else ""

    return first_line.strip()

def qbo_query_all(
    realm_id: str,
    query: str,
    access_token: str,
    qbo_api_base: str,
    page_size: int = 1000,
) -> list[dict]:
    """
    Runs a QBO query and fetches ALL pages using GET with `query=` param.
    This avoids QBO's common "QueryParserError: null" issue seen with POST bodies.
    """
    results: list[dict] = []
    start = 1

    while True:
        paged_query = f"{query} STARTPOSITION {start} MAXRESULTS {page_size}"
        url = f"{qbo_api_base}/v3/company/{realm_id}/query"

        r = requests.get(
            url,
            params={"query": paged_query, "minorversion": "75"},
            headers={
                "Authorization": f"Bearer {access_token}",
                "Accept": "application/json",
            },
            timeout=60,
        )

        if r.status_code == 401:
            raise RuntimeError(f"AUTH_401: {r.text}")
        if r.status_code >= 400:
            # helpful debug (no tokens leaked)
            raise RuntimeError(
                f"QBO_QUERY_FAILED ({r.status_code}): {r.text} | sent_query={paged_query}"
            )

        payload = r.json() if r.content else {}
        q = payload.get("QueryResponse", {})
        batch = q.get("Invoice", []) or []
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
    po_number = ""
    custom_fields = invoice.get("CustomField") or []
    if isinstance(custom_fields, list):
        for cf in custom_fields:
            if not isinstance(cf, dict):
                continue
            # QBO often stores the label in Name (or DefinitionId ties to config)
            if (cf.get("Name") or "").strip().lower() in ("p.o. number", "po number", "p.o. #", "po #"):
                # Value can be in StringValue for text custom fields
                po_number = cf.get("StringValue") or cf.get("value") or ""
                break

    sales_term_ref = invoice.get("SalesTermRef") or {}
    sales_term_name = sales_term_ref.get("name", "")    
    
    # keep the rest as you already had
    meta = invoice.get("MetaData") or {}
    lines = invoice.get("Line") or []
    if not isinstance(lines, list):
        return rows

    for idx, line in enumerate(lines, start=1):
        # ... (no change to your line parsing)

        # ONLY keep SalesItemLineDetail lines
        if line.get("DetailType") != "SalesItemLineDetail":
            continue

        # Extract SalesItemLineDetail
        sales_item_line_detail = line.get("SalesItemLineDetail") or {}
        item_ref = sales_item_line_detail.get("ItemRef") or {}
        item_name = item_ref.get("name", "") or ""
        
        # Remove leading "FAA Repair:" if present
        prefix = "FAA Repair:"
        clean_name = item_name.strip()
        if clean_name.startswith(prefix):
            item_name = clean_name[len(prefix):].strip()
        else:
            item_name = clean_name
    
        # Force large numeric-looking Item values to be treated as text in Excel
        if item_name.isdigit() and len(item_name) >= 10:
            item_name = f'="{item_name}"'
        
        unit_price = sales_item_line_detail.get("UnitPrice", "")
        qty = sales_item_line_detail.get("Qty", "")

        # Extract WO # when Description has it
        descr = line.get("Description", "") or ""
        work_order = extract_work_order(descr)

        row = {
            # Requested parent fields:
            "InvoiceId": invoice_id,
            "DocNumber": doc_number,
            "TxnDate": txn_date,
            "CustomerName": customer_name,
            "P.O. Number": po_number,
                        
            # Line identifiers / ordering
            "LineId": line.get("Id", ""),

            # keep the rest of your fields...
            "Amount": line.get("Amount", ""),
            "Description": descr,

            # Work Order number when existing in record
            "Work Order": work_order,

            "Item": item_name,
            "Unit Price": unit_price,
            "Qty": qty,
           
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
   
    # Group ordering by Transaction Date
    def _parse_txn_date(s: str):
        # TxnDate format from QBO is usually YYYY-MM-DD
        try:
            return date.fromisoformat(s)
        except Exception:
            return date.min

    #Within-group ordering
    def _parse_line_id(v):
        # LineId is often numeric string; sort numerically when possible
        try:
            return int(v)
        except Exception:
            return 10**18  # push non-numeric/missing to end
    """
    all_lines.sort(
        key=lambda r: (
            _parse_txn_date(r.get("TxnDate", "")),  # primary: date ASC
            r.get("InvoiceId", ""),                 # secondary: group
            _parse_line_id(r.get("LineId", "")),    # within group
        )
    )
    """
    all_lines.sort(
        key=lambda r: (
            r.get("InvoiceId", ""),          # group key
            _parse_txn_date(r.get("TxnDate", "")),   # group ordering (ASC)
            _parse_line_id(r.get("LineId", "")),     # within-group ordering (ASC)
        )
    )
    
    # 4) Return JSON (default) or CSV
    if format.lower() == "json":
        buf = io.BytesIO()
        buf.write(json.dumps(all_lines, indent=2, ensure_ascii=False, default=str).encode("utf-8"))
        buf.seek(0)
        filename = f"invoice_lines_{year}_{realmId}.json"
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
            #"InvoiceId",
            "DocNumber",
            "TxnDate",
            "CustomerName",
            "P.O. Number",
                    
            # rest of your line fields
            "LineId",
            "Amount",
            "Description",
            "Work Order",
            "Item",
            "Unit Price",
            "Qty",

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

        filename = f"invoice_lines_{year}_{realmId}.csv"
        return StreamingResponse(
            buf,
            media_type="text/csv",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    return JSONResponse({"error": "invalid_format", "allowed": ["json", "csv"]}, status_code=400)


@router.get("/month/{year}/{month}")
def download_invoice_lines_for_month(
    request: Request,
    realmId: str,
    year: int,
    month: int,
    format: str = "json",
):
    get_valid_access_token = request.app.state.get_valid_access_token
    qbo_api_base = request.app.state.qbo_api_base

    # 1) Get valid token
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

    # Validate month
    if month < 1 or month > 12:
        return JSONResponse({"error": "invalid_month"}, status_code=400)

    # 2) Build correct start/end dates for that month
    last_day = monthrange(year, month)[1]
    start_date = f"{year}-{month:02d}-01"
    end_date = f"{year}-{month:02d}-{last_day:02d}"

    # Same query style you're already using in year endpoint
    q = (
        "SELECT * FROM Invoice "
        f"WHERE TxnDate >= '{start_date}' AND TxnDate <= '{end_date}' "
        "ORDER BY TxnDate DESC"
    )

    invoices = qbo_query_all(realmId, q, access_token, qbo_api_base)

    # Flatten lines (same as your working year endpoint)
    all_lines: list[dict] = []
    for inv in invoices:
        rows = flatten_invoice_lines(inv)
        all_lines.extend(rows)

    # Group ordering by Transaction Date
    def _parse_txn_date(s: str):
        # TxnDate format from QBO is usually YYYY-MM-DD
        try:
            return date.fromisoformat(s)
        except Exception:
            return date.min

    #Within-group ordering
    def _parse_line_id(v):
        # LineId is often numeric string; sort numerically when possible
        try:
            return int(v)
        except Exception:
            return 10**18  # push non-numeric/missing to end
    """
    all_lines.sort(
        key=lambda r: (
            _parse_txn_date(r.get("TxnDate", "")),  # primary: date ASC
            r.get("InvoiceId", ""),                 # secondary: group
            _parse_line_id(r.get("LineId", "")),    # within group
        )
    )
    """
    all_lines.sort(
        key=lambda r: (
            r.get("InvoiceId", ""),          # group key
            _parse_txn_date(r.get("TxnDate", "")),   # group ordering (ASC)
            _parse_line_id(r.get("LineId", "")),     # within-group ordering (ASC)
        )
    )

    # Return JSON (default)
    if format.lower() == "json":
        buf = io.BytesIO()
        buf.write(json.dumps(all_lines, indent=2, ensure_ascii=False, default=str).encode("utf-8"))
        buf.seek(0)
        filename = f"invoice_lines_{year}_{month:02d}_{realmId}.json"
        return StreamingResponse(
            buf,
            media_type="application/json",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    # CSV (same fieldnames as your year report)
    if format.lower() == "csv":
        fieldnames = [
            "DocNumber",
            "TxnDate",
            "CustomerName",
            "P.O. Number",
            "LineId",
            "Amount",
            "Description",
            "Work Order",
            "Item",
            "Unit Price",
            "Qty",
        ]

        text_buf = io.StringIO()
        writer = csv.DictWriter(text_buf, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()

        for r in all_lines:
            writer.writerow(r)

        data = text_buf.getvalue().encode("utf-8-sig")
        buf = io.BytesIO(data)
        buf.seek(0)

        filename = f"invoice_lines_{year}_{month:02d}_{realmId}.csv"
        return StreamingResponse(
            buf,
            media_type="text/csv",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    return JSONResponse({"error": "invalid_format"}, status_code=400)
