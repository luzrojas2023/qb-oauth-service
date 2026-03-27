# reports/invoice_lines.py
import io
import csv
import json
import requests
from typing import Any
from calendar import monthrange
from datetime import date
from decimal import Decimal, InvalidOperation
from collections import defaultdict
from db import get_conn
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

def qualifies_robert(customer_name: str, item_code: str) -> bool:
    # Combo 1 you provided
    if customer_name.strip().lower() == "air france":
        return item_code in {"S906-70196-3", "362A6411P4"}
    # TODO: add Robert combo #2 later
    return False

def qualifies_evert(customer_name: str, item_code: str) -> bool:
    cn = (customer_name or "").lower()

    # Evert customer substring list (includes KLM; overlap is fine)
    keywords = ["ajw technique", "klm", "aar", "lufthansa", "csi", "austrian", "fokker", "ametek", "muirhead"]
    if any(k in cn for k in keywords):
        return True

    # Skysmart special cases: for now, just include all Skysmart lines,
    # OR restrict to certain items later (you can decide).
    if "skysmart" in cn:
        return True

    return False

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
    Looks up family_code in item_catalog using qbo_item_id
    through the existing Postgres connection.
    """
    clean_ids = sorted({str(x).strip() for x in item_ids if str(x).strip()})
    if not clean_ids:
        return {}

    family_map: dict[str, str] = {}

    chunk_size = 500
    for i in range(0, len(clean_ids), chunk_size):
        chunk = clean_ids[i:i + chunk_size]

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    select qbo_item_id, family_code
                    from item_catalog
                    where qbo_item_id = any(%s)
                    """,
                    (chunk,)
                )
                rows = cur.fetchall() or []

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

def group_lines_by_family(all_lines: list[dict], include_customer: bool = False) -> list[dict]:
    grouped: dict[tuple, dict] = {}

    for r in all_lines:
        customer_name = str(r.get("CustomerName", "")).strip()
        family_code = str(r.get("FamilyCode", "UNASSIGNED")).strip() or "UNASSIGNED"

        # When include_customer=True, grouping happens within each customer
        key = (customer_name, family_code) if include_customer else (family_code,)

        if key not in grouped:
            grouped[key] = {
                "CustomerName": customer_name,
                "FamilyCode": family_code,
                "Item": str(r.get("Item", "")).strip(),
                "TotalQty": Decimal("0"),
                "TotalSales": Decimal("0"),
            }

        grouped[key]["TotalQty"] += to_decimal(r.get("Qty"))
        grouped[key]["TotalSales"] += to_decimal(r.get("Amount"))

    results: list[dict] = []
    for g in grouped.values():
        if include_customer:
            results.append({
                "CustomerName": g["CustomerName"],
                "FamilyCode": g["FamilyCode"],
                "Item": g["Item"],
                "TotalQty": float(g["TotalQty"]),
                "TotalSales": float(g["TotalSales"]),
            })
        else:
            results.append({
                "FamilyCode": g["FamilyCode"],
                "Item": g["Item"],
                "TotalQty": float(g["TotalQty"]),
                "TotalSales": float(g["TotalSales"]),
            })

    if include_customer:
        results.sort(key=lambda x: (x["CustomerName"], x["FamilyCode"], x["Item"]))
    else:
        results.sort(key=lambda x: (x["FamilyCode"], x["Item"]))

    return results

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
        q = payload.get("QueryResponse", {})
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
    customer_id = customer_ref.get("value", "")
    
    
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

    # Extract Terms
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
        item_id = item_ref.get("value", "")
        
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

        line_amount = line.get("Amount", "")
        if to_decimal(line_amount) == 0:
            continue

        # Extract WO # when Description has it
        descr = line.get("Description", "") or ""
        work_order = extract_work_order(descr)

        row = {
            # Requested parent fields:
            "InvoiceId": invoice_id,
            "DocNumber": doc_number,
            "TxnDate": txn_date,
            "CustomerId": customer_id,
            "CustomerName": customer_name,
            "P.O. Number": po_number,
                        
            # Line identifiers / ordering
            "LineId": line.get("Id", ""),

            # keep the rest of your fields...
            "Amount": line_amount,
            "Description": descr,

            # Work Order number when existing in record
            "Work Order": work_order,

            "ItemId": item_id,
            "Item": item_name,
            "Unit Price": unit_price,
            "Qty": qty,
           
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


@router.get("/month/{year}/{month}")
def download_invoice_lines_for_month(
    request: Request,
    realmId: str,
    year: int,
    month: int,
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

    q = build_invoice_query(start_date, end_date, customer_id=customer_id)

    invoices = qbo_query_all(realmId, q, access_token, qbo_api_base)
    invoices = safe_filter_invoices_by_customer(invoices, customer_id=customer_id)

    # 3) Flatten lines
    all_lines: list[dict] = []
    for inv in invoices:
        rows = flatten_invoice_lines(inv)
        all_lines.extend(rows)

    # 4) Return JSON (default) or CSV
    if format.lower() == "json":
        buf = io.BytesIO()
        buf.write(json.dumps(all_lines, indent=2, ensure_ascii=False, default=str).encode("utf-8"))
        buf.seek(0)
        
        if customer_id:
            filename = f"invoice_lines_{year}_{month:02d}_{realmId}_customer_{customer_id}.json"
        else:
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

        if customer_id:
            filename = f"invoice_lines_{year}_{month:02d}_{realmId}_customer_{customer_id}.csv"
        else:
            filename = f"invoice_lines_{year}_{month:02d}_{realmId}.csv"
        
        return StreamingResponse(
            buf,
            media_type="text/csv",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    return JSONResponse({"error": "invalid_format"}, status_code=400)

@router.get("/quarter/{year}/{quarter}")
def download_invoice_lines_for_quarter(
    request: Request,
    realmId: str,
    year: int,
    quarter: int,
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

    if quarter not in (1, 2, 3, 4):
        return JSONResponse({"error": "invalid_quarter", "allowed": [1, 2, 3, 4]}, status_code=400)

    if quarter == 1:
        start_date = f"{year}-01-01"
        end_date = f"{year}-03-31"
    elif quarter == 2:
        start_date = f"{year}-04-01"
        end_date = f"{year}-06-30"
    elif quarter == 3:
        start_date = f"{year}-07-01"
        end_date = f"{year}-09-30"
    else:
        start_date = f"{year}-10-01"
        end_date = f"{year}-12-31"

    q = build_invoice_query(start_date, end_date, customer_id=customer_id)

    invoices = qbo_query_all(realmId, q, access_token, qbo_api_base)
    invoices = safe_filter_invoices_by_customer(invoices, customer_id=customer_id)

    all_lines: list[dict] = []
    for inv in invoices:
        rows = flatten_invoice_lines(inv)
        for r in rows:
            r["RealmId"] = realmId
        all_lines.extend(rows)

    if format.lower() == "json":
        buf = io.BytesIO()
        buf.write(json.dumps(all_lines, indent=2, ensure_ascii=False, default=str).encode("utf-8"))
        buf.seek(0)

        if customer_id:
            filename = f"invoice_lines_{year}_Q{quarter}_{realmId}_customer_{customer_id}.json"
        else:
            filename = f"invoice_lines_{year}_Q{quarter}_{realmId}.json"

        return StreamingResponse(
            buf,
            media_type="application/json",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

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

        if customer_id:
            filename = f"invoice_lines_{year}_Q{quarter}_{realmId}_customer_{customer_id}.csv"
        else:
            filename = f"invoice_lines_{year}_Q{quarter}_{realmId}.csv"

        return StreamingResponse(
            buf,
            media_type="text/csv",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    return JSONResponse({"error": "invalid_format", "allowed": ["json", "csv"]}, status_code=400)

@router.get("/grouped/year/{year}")
def download_invoice_lines_grouped_by_family_for_year(
    request: Request,
    realmId: str,
    year: int,
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

    start_date = f"{year}-01-01"
    end_date = f"{year}-12-31"

    q = build_invoice_query(start_date, end_date, customer_id=customer_id)

    invoices = qbo_query_all(realmId, q, access_token, qbo_api_base)
    invoices = safe_filter_invoices_by_customer(invoices, customer_id=customer_id)

    all_lines: list[dict] = []
    for inv in invoices:
        rows = flatten_invoice_lines(inv)
        for r in rows:
            r["RealmId"] = realmId
        all_lines.extend(rows)

    all_lines = attach_family_codes(request, all_lines)
    grouped_rows = group_lines_by_family(all_lines, include_customer=(customer_id is None))
    
    customer_name = ""
    if customer_id and all_lines:
        customer_name = str(all_lines[0].get("CustomerName", "")).strip()

    text_buf = io.StringIO()

    if customer_id:
        text_buf.write(f"{customer_name}\n")
        text_buf.write(f"{year}\n\n")
    else:
        text_buf.write(f"{year}\n\n")

    if customer_id:
        fieldnames = [
            "FamilyCode",
            "Item",
            "TotalQty",
            "TotalSales",
        ]
    else:
        fieldnames = [
            "CustomerName",
            "FamilyCode",
            "Item",
            "TotalQty",
            "TotalSales",
        ]

    writer = csv.DictWriter(text_buf, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()

    for r in grouped_rows:
        writer.writerow(r)

    data = text_buf.getvalue().encode("utf-8-sig")
    buf = io.BytesIO(data)
    buf.seek(0)

    if customer_id:
        filename = f"invoice_lines_grouped_family_{year}_{realmId}_customer_{customer_id}.csv"
    else:
        filename = f"invoice_lines_grouped_family_{year}_{realmId}.csv"

    return StreamingResponse(
        buf,
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )

@router.get("/grouped/month/{year}/{month}")
def download_invoice_lines_grouped_by_family_for_month(
    request: Request,
    realmId: str,
    year: int,
    month: int,
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

    if month < 1 or month > 12:
        return JSONResponse({"error": "invalid_month"}, status_code=400)

    last_day = monthrange(year, month)[1]
    start_date = f"{year}-{month:02d}-01"
    end_date = f"{year}-{month:02d}-{last_day:02d}"

    q = build_invoice_query(start_date, end_date, customer_id=customer_id)

    invoices = qbo_query_all(realmId, q, access_token, qbo_api_base)
    invoices = safe_filter_invoices_by_customer(invoices, customer_id=customer_id)

    all_lines: list[dict] = []
    for inv in invoices:
        rows = flatten_invoice_lines(inv)
        for r in rows:
            r["RealmId"] = realmId
        all_lines.extend(rows)

    all_lines = attach_family_codes(request, all_lines)
    grouped_rows = group_lines_by_family(all_lines, include_customer=(customer_id is None))
    
    customer_name = ""
    if customer_id and all_lines:
        customer_name = str(all_lines[0].get("CustomerName", "")).strip()

    period_label = f"{year}-{month:02d}"

    text_buf = io.StringIO()

    if customer_id:
        text_buf.write(f"{customer_name}\n")
        text_buf.write(f"{period_label}\n\n")
    else:
        text_buf.write(f"{period_label}\n\n")

    if customer_id:
        fieldnames = [
            "FamilyCode",
            "Item",
            "TotalQty",
            "TotalSales",
        ]
    else:
        fieldnames = [
            "CustomerName",
            "FamilyCode",
            "Item",
            "TotalQty",
            "TotalSales",
        ]

    writer = csv.DictWriter(text_buf, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()

    for r in grouped_rows:
        writer.writerow(r)

    data = text_buf.getvalue().encode("utf-8-sig")
    buf = io.BytesIO(data)
    buf.seek(0)

    if customer_id:
        filename = f"invoice_lines_grouped_family_{year}_{month:02d}_{realmId}_customer_{customer_id}.csv"
    else:
        filename = f"invoice_lines_grouped_family_{year}_{month:02d}_{realmId}.csv"

    return StreamingResponse(
        buf,
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )

@router.get("/grouped/quarter/{year}/{quarter}")
def download_invoice_lines_grouped_by_family_for_quarter(
    request: Request,
    realmId: str,
    year: int,
    quarter: int,
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

    if quarter not in (1, 2, 3, 4):
        return JSONResponse({"error": "invalid_quarter", "allowed": [1, 2, 3, 4]}, status_code=400)

    if quarter == 1:
        start_date = f"{year}-01-01"
        end_date = f"{year}-03-31"
    elif quarter == 2:
        start_date = f"{year}-04-01"
        end_date = f"{year}-06-30"
    elif quarter == 3:
        start_date = f"{year}-07-01"
        end_date = f"{year}-09-30"
    else:
        start_date = f"{year}-10-01"
        end_date = f"{year}-12-31"

    q = build_invoice_query(start_date, end_date, customer_id=customer_id)

    invoices = qbo_query_all(realmId, q, access_token, qbo_api_base)
    invoices = safe_filter_invoices_by_customer(invoices, customer_id=customer_id)

    all_lines: list[dict] = []
    for inv in invoices:
        rows = flatten_invoice_lines(inv)
        for r in rows:
            r["RealmId"] = realmId
        all_lines.extend(rows)

    all_lines = attach_family_codes(request, all_lines)
    grouped_rows = group_lines_by_family(all_lines, include_customer=(customer_id is None))

    customer_name = ""
    if customer_id and all_lines:
        customer_name = str(all_lines[0].get("CustomerName", "")).strip()

    period_label = f"{year} Q{quarter}"

    text_buf = io.StringIO()

    if customer_id:
        text_buf.write(f"{customer_name}\n")
        text_buf.write(f"{period_label}\n\n")
    else:
        text_buf.write(f"{period_label}\n\n")

    if customer_id:
        fieldnames = [
            "FamilyCode",
            "Item",
            "TotalQty",
            "TotalSales",
        ]
    else:
        fieldnames = [
            "CustomerName",
            "FamilyCode",
            "Item",
            "TotalQty",
            "TotalSales",
        ]

    writer = csv.DictWriter(text_buf, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()

    for r in grouped_rows:
        writer.writerow(r)

    data = text_buf.getvalue().encode("utf-8-sig")
    buf = io.BytesIO(data)
    buf.seek(0)

    if customer_id:
        filename = f"invoice_lines_grouped_family_{year}_Q{quarter}_{realmId}_customer_{customer_id}.csv"
    else:
        filename = f"invoice_lines_grouped_family_{year}_Q{quarter}_{realmId}.csv"

    return StreamingResponse(
        buf,
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )

@router.get("/grouped-lines/year/{year}")
def download_invoice_lines_with_family_for_year(
    request: Request,
    realmId: str,
    year: int,
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

    start_date = f"{year}-01-01"
    end_date = f"{year}-12-31"

    q = build_invoice_query(start_date, end_date, customer_id=customer_id)

    invoices = qbo_query_all(realmId, q, access_token, qbo_api_base)
    invoices = safe_filter_invoices_by_customer(invoices, customer_id=customer_id)

    all_lines: list[dict] = []
    for inv in invoices:
        rows = flatten_invoice_lines(inv)  # already excludes Amount = 0
        for r in rows:
            r["RealmId"] = realmId
        all_lines.extend(rows)

    # 🔥 attach family (NO grouping)
    all_lines = attach_family_codes(request, all_lines)

    all_lines.sort(key=lambda x: (
    str(x.get("CustomerName", "")) if customer_id is None else "",
    str(x.get("FamilyCode", "")),
    str(x.get("Item", "")),
    str(x.get("TxnDate", "")),
))

    # CSV output
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
        "FamilyCode",   # 👈 NEW FIELD
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

    if customer_id:
        filename = f"invoice_lines_with_family_{year}_{realmId}_customer_{customer_id}.csv"
    else:
        filename = f"invoice_lines_with_family_{year}_{realmId}.csv"

    return StreamingResponse(
        buf,
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )

@router.get("/grouped-lines/month/{year}/{month}")
def download_invoice_lines_with_family_for_month(
    request: Request,
    realmId: str,
    year: int,
    month: int,
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

    if month < 1 or month > 12:
        return JSONResponse({"error": "invalid_month"}, status_code=400)

    last_day = monthrange(year, month)[1]
    start_date = f"{year}-{month:02d}-01"
    end_date = f"{year}-{month:02d}-{last_day:02d}"

    q = build_invoice_query(start_date, end_date, customer_id=customer_id)

    invoices = qbo_query_all(realmId, q, access_token, qbo_api_base)
    invoices = safe_filter_invoices_by_customer(invoices, customer_id=customer_id)

    all_lines: list[dict] = []
    for inv in invoices:
        rows = flatten_invoice_lines(inv)
        for r in rows:
            r["RealmId"] = realmId
        all_lines.extend(rows)

    all_lines = attach_family_codes(request, all_lines)

    all_lines.sort(key=lambda x: (
        str(x.get("CustomerName", "")) if customer_id is None else "",
        str(x.get("FamilyCode", "")),
        str(x.get("Item", "")),
        str(x.get("TxnDate", "")),
    ))

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
        "FamilyCode",
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

    if customer_id:
        filename = f"invoice_lines_with_family_{year}_{month:02d}_{realmId}_customer_{customer_id}.csv"
    else:
        filename = f"invoice_lines_with_family_{year}_{month:02d}_{realmId}.csv"

    return StreamingResponse(
        buf,
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )

@router.get("/grouped-lines/quarter/{year}/{quarter}")
def download_invoice_lines_with_family_for_quarter(
    request: Request,
    realmId: str,
    year: int,
    quarter: int,
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

    if quarter not in (1, 2, 3, 4):
        return JSONResponse({"error": "invalid_quarter"}, status_code=400)

    if quarter == 1:
        start_date = f"{year}-01-01"
        end_date = f"{year}-03-31"
    elif quarter == 2:
        start_date = f"{year}-04-01"
        end_date = f"{year}-06-30"
    elif quarter == 3:
        start_date = f"{year}-07-01"
        end_date = f"{year}-09-30"
    else:
        start_date = f"{year}-10-01"
        end_date = f"{year}-12-31"

    q = build_invoice_query(start_date, end_date, customer_id=customer_id)

    invoices = qbo_query_all(realmId, q, access_token, qbo_api_base)
    invoices = safe_filter_invoices_by_customer(invoices, customer_id=customer_id)

    all_lines: list[dict] = []
    for inv in invoices:
        rows = flatten_invoice_lines(inv)
        for r in rows:
            r["RealmId"] = realmId
        all_lines.extend(rows)

    all_lines = attach_family_codes(request, all_lines)

    all_lines.sort(key=lambda x: (
        str(x.get("CustomerName", "")) if customer_id is None else "",
        str(x.get("FamilyCode", "")),
        str(x.get("Item", "")),
        str(x.get("TxnDate", "")),
    ))

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
        "FamilyCode",
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

    if customer_id:
        filename = f"invoice_lines_with_family_{year}_Q{quarter}_{realmId}_customer_{customer_id}.csv"
    else:
        filename = f"invoice_lines_with_family_{year}_Q{quarter}_{realmId}.csv"

    return StreamingResponse(
        buf,
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
