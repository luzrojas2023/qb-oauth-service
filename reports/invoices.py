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


@router.get("/year/{year}")
def download_invoices_for_year(
    realmId: str,
    year: int,
    format: str = "json",
):
    """
    Download ALL invoices for a calendar year, sorted by TxnDate DESC.
    format: json (default) or csv

    NOTE: This module expects these to be attached to app.state from main.py:
      - app.state.get_valid_access_token(realmId) -> access_token
      - app.state.qbo_api_base -> str
    """
    # We can't access app.state here directly unless we read it from request.app,
    # so we accept it via FastAPI dependency injection by using Request in signature:
    # (Implemented below as wrapper to keep endpoint clean)
    raise NotImplementedError("This function should be wrapped by request-aware handler.")
