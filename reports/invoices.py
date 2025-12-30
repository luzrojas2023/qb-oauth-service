# reports/invoices.py
import io
import csv
import json
import requests
from fastapi import APIRouter
from fastapi.responses import StreamingResponse, JSONResponse

router = APIRouter(prefix="/reports/invoices", tags=["reports-invoices"])


def qbo_query_all(realm_id: str, query: str, access_token: str, qbo_api_base: str, page_size: int = 1000) -> list[dict]:
    """
    Runs a QBO SQL-like query and fetches ALL pages.
    QBO Query API supports MAXRESULTS (<=1000) and STARTPOSITION (1-based).
    """
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
        "SELECT * FROM Invoice"
        f" WHERE TxnDate >= '{start_date}' AND TxnDate <= '{end_date}'"
        " ORDER BY TxnDate DESC"
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
            "Id", "DocNumber", "TxnDate", "CustomerRef", "TotalAmt", "Balance",
            "Line_json", "MetaData_json", "Raw_json",
        ]
        writer.writerow(headers)

        for inv in invoices:
            writer.writerow([
                inv.get("Id"),
                inv.get("DocNumber"),
                inv.get("TxnDate"),
                json.dumps(inv.get("CustomerRef"), ensure_ascii=False),
                inv.get("TotalAmt"),
                inv.get("Balance"),
                json.dumps(inv.get("Line"), ensure_ascii=False),
                json.dumps(inv.get("MetaData"), ensure_ascii=False),
                json.dumps(inv, ensure_ascii=False),
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

