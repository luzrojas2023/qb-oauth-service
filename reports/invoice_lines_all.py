# reports/invoice_lines_all.py
import io
import csv
import json
import requests
from typing import Any
from decimal import Decimal, InvalidOperation
from collections import defaultdict

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

def to_decimal(val: Any) -> Decimal:
    if val in (None, "", " "):
        return Decimal("0")
    try:
        return Decimal(str(val))
    except (InvalidOperation, ValueError, TypeError):
        return Decimal("0")

def fetch_item_family_map(request: Request, item_ids: list[str]) -> dict[str, str]:
    """
    Looks up family_code in Supabase item_catalog using qbo_item_id.
    """
    supabase = request.app.state.supabase

    clean_ids = sorted({str(x).strip() for x in item_ids if str(x).strip()})
    if not clean_ids:
        return {}

    family_map: dict[str, str] = {}

    chunk_size = 500
    for i in range(0, len(clean_ids), chunk_size):
        chunk = clean_ids[i:i + chunk_size]

        resp = (
            supabase
            .table("item_catalog")
            .select("qbo_item_id,family_code")
            .in_("qbo_item_id", chunk)
            .execute()
        )

        rows = resp.data or []
        for r in rows:
            item_id = str(r.get("qbo_item_id", "")).strip()
            family_code = (r.get("family_code") or "").strip()
            if item_id:
                family_map[item_id] = family_code or "UNASSIGNED"

    return family_map

def attach_family_codes(request: Request, all_lines: list[dict]) -> list[dict]:
    item_ids = [
        str(r.get("ItemId", "")).strip()
        for r in all_lines
        if str(r.get("ItemId", "")).strip()
    ]

    family_map = fetch_item_family_map(request, item_ids)

    for r in all_lines:
        item_id = str(r.get("ItemId", "")).strip()
        r["FamilyCode"] = family_map.get(item_id, "UNASSIGNED")

    return all_lines

def build_invoice_query(start_date: str, end_date: str, customer_id: str | None = None) -> str:
    query = (
        f"SELECT * FROM Invoice "
        f"WHERE TxnDate >= '{start_date}' "
        f"AND TxnDate <= '{end_date}'"
    )

    if customer_id is not None and str(customer_id).strip():
        safe_customer_id = str(customer_id).strip()
        query += f" AND CustomerRef = '{safe_customer_id}'"

    query += " ORDER BY TxnDate ASC, Id ASC"

    return query


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
        payload.get("QueryResponse", {})
        batch = q.get("Invoice", []) or []
        results.extend(batch)

        if len(batch) < page_size:
            break

        start += page_size

    return results


def safe_filter_invoices_by_customer(invoices: list, customer_id: str | None = None) -> list:
    """
    Safety filter applied after QBO returns invoices.
    Keeps all invoices if customer_id is not provided.
    """
    if customer_id is None or not str(customer_id).strip():
        return invoices

    target_customer_id = str(customer_id).strip()

    filtered = []
    for invoice in invoices:
        customer_ref = invoice.get("CustomerRef") or {}
        invoice_customer_id = str(customer_ref.get("value", "")).strip()

        if invoice_customer_id == target_customer_id:
            filtered.append(invoice)

    return filtered


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
            "Invoice Id": invoice_id,
            "DocNumber": doc_number,
            "TxnDate": txn_date,
            "CustomerName": customer_name,
            "P.O. Number": po_number,
            "SalesTerm": sales_term_name,
            
            # Line identifiers / ordering
            "LineIndex": idx,
            "LineId": line.get("Id", ""),

            # keep the rest of your fields...
            "DetailType": line.get("DetailType", ""),
            "Amount": line.get("Amount", ""),
            "Description": descr,

            # Work Order number when existing in record
            "Work Order": work_order,

            "Item": item_name,
            "Unit Price": unit_price,
            "Qty": qty,
                          
            # keep your SalesItemLineDetail extraction, etc.
            # ...
            "Line_json": line,
            "Invoice_json": invoice,
        }

        rows.append(row)

    return rows


@router.get("/year/{year}")
def download_invoice_lines_for_year(
    request: Request,
    realmId: str,
    year: int,
    format: str = "json",
    customer_id: str | None = None,
):
    get_valid_access_token = request.app.state.get_valid_access_token
    qbo_api_base = request.app.state.qbo_api_base

    if customer_id is not None:
        customer_id = customer_id.strip()
        if customer_id == "":
            return JSONResponse(
                {"error": "invalid_customer_id", "message": "customer_id cannot be empty"},
                status_code=400,
            )

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

    # 2) Query all invoices in the year, sorted by TxnDate asc, Id asc
    start_date = f"{year}-01-01"
    end_date = f"{year}-12-31"

    q = build_invoice_query(start_date, end_date, customer_id=customer_id)
    
    invoices = qbo_query_all(realmId, q, access_token, qbo_api_base)
    invoices = safe_filter_invoices_by_customer(invoices, customer_id=customer_id)

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

        if customer_id:
            filename = f"invoice_lines_{year}_{realmId}_customer_{customer_id}.json"
        else:
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
            "Invoice Id",
            "DocNumber",
            "TxnDate",
            "CustomerName",
            "P.O. Number",
            "SalesTerm",
        
            # rest of your line fields
            "LineIndex",
            "LineId",
            "DetailType",
            "Amount",
            "Description",

            "Work Order",

            "Item",
            "Unit Price",
            "Qty",

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

        if customer_id:
            filename = f"invoice_lines_{year}_{realmId}_customer_{customer_id}.csv"
        else:
            filename = f"invoice_lines_{year}_{realmId}.csv"
        
        return StreamingResponse(
            buf,
            media_type="text/csv",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    return JSONResponse({"error": "invalid_format", "allowed": ["json", "csv"]}, status_code=400)
