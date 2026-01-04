# reports/invoices.py
import io
import csv
import json
import requests
from fastapi import APIRouter
from fastapi.responses import StreamingResponse, JSONResponse

router = APIRouter(prefix="/reports/invoices", tags=["reports-invoices"])

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


from fastapi import Request

@router.get("/year/{year}")
def download_invoices_for_year(request: Request, realmId: str, year: int, format: str = "json"):
    get_valid_access_token = request.app.state.get_valid_access_token
    qbo_api_base = request.app.state.qbo_api_base

    # 1) Get a valid token (auto-refresh inside)
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

    q = (
        "SELECT * FROM Invoice "
        f"WHERE TxnDate >= '{start_date}' AND TxnDate <= '{end_date}' "
        "ORDERBY TxnDate DESC"
    )

    invoices = qbo_query_all(realmId, q, access_token, qbo_api_base)

    if format.lower() == "json":
        buf = io.BytesIO()
        buf.write(json.dumps(invoices, indent=2, default=str).encode("utf-8"))
        buf.seek(0)
        filename = f"invoices_{year}_{realmId}.json"
        return StreamingResponse(
            buf,
            media_type="application/json",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    if format.lower() == "csv":
        text_buf = io.StringIO()
        writer = csv.writer(text_buf)
    
        headers = [
            # Core identifiers
            "Id",
            "DocNumber",
            "TxnDate",
    
            # CustomerRef split
            #"CustomerId",
            "CustomerName",

            # CustomField_json split
            "P.O. Number",
    
            # Amounts
            "TotalAmt",
            "Balance",
    
            "DueDate",
    
            # MetaData split (and exclude MetaData from Raw fields)
            "MetaData_LastModifiedByRef_value",
            "MetaData_LastUpdatedTime",

            # CustomField_json split
            "Sales Rep",
    
            # Core identifiers
            "SyncToken",

            # Status/flags commonly used in reporting
            "PrivateNote",
        ]
        writer.writerow(headers)

        def safe_get(d: dict, path: list[str], default=None):
            """Safely get nested values without KeyError."""
            cur = d or {}
            for key in path:
                if not isinstance(cur, dict):
                    return default
                cur = cur.get(key)
                if cur is None:
                    return default
            return cur

        def safe_json(obj):
            return json.dumps(obj, ensure_ascii=False) if obj is not None else ""
    
        for inv in invoices:
            customer_ref = inv.get("CustomerRef") or {}
            meta = inv.get("MetaData") or {}

            # Customer split
            #customer_id = customer_ref.get("value")
            customer_name = customer_ref.get("name")
    
            # MetaData split
            meta_create_time = meta.get("CreateTime")
            meta_last_modified_by_ref_value = (meta.get("LastModifiedByRef") or {}).get("value")
            meta_last_updated_time = meta.get("LastUpdatedTime")
    
            custom_fields = inv.get("CustomField") or []

            po_number = ""
            sales_rep = ""
            
            for cf in custom_fields:
                name = (cf.get("Name") or "").strip()
                # QBO custom fields can store in StringValue, NumberValue, or sometimes a generic "value"
                val = (
                    cf.get("StringValue")
                    or cf.get("NumberValue")
                    or cf.get("Value")
                    or cf.get("value")
                    or ""
                )
            
                if name.lower() in ("p.o. number", "po number", "p.o number", "po#", "po"):
                    po_number = str(val)
                elif name.lower() in ("sales rep", "salesrep", "sales representative", "sales agent"):
                    sales_rep = str(val)
    
            writer.writerow([
                inv.get("Id"),
                inv.get("DocNumber"),
                inv.get("TxnDate"),
    
                customer_name,
                po_number,
    
                inv.get("TotalAmt"),
                inv.get("Balance"),
    
                inv.get("DueDate"),
      
                meta_last_modified_by_ref_value,
                meta_last_updated_time,

                sales_rep,

                inv.get("SyncToken"),
                inv.get("PrivateNote"),
            ])

        data = text_buf.getvalue().encode("utf-8-sig")
        buf = io.BytesIO(data)
        buf.seek(0)
    
        filename = f"invoices_{year}_{realmId}.csv"
        return StreamingResponse(
            buf,
            media_type="text/csv",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    return JSONResponse({"error": "invalid_format", "allowed": ["json", "csv"]}, status_code=400)

