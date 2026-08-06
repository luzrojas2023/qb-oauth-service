import os
import secrets
import requests

from fastapi import (
    APIRouter,
    Depends,
    Header,
    HTTPException,
    Request,
)

from pydantic import BaseModel

PO_NUMBER_DEFINITION_ID = "1"
WO_SO_NUMBER_DEFINITION_ID = "3"

def require_api_key(
    x_api_key: str | None = Header(
        default=None,
        alias="x-api-key",
    ),
) -> None:
    expected_api_key = os.getenv("APP_API_KEY")

    if not expected_api_key:
        raise HTTPException(
            status_code=500,
            detail="APP_API_KEY is not configured.",
        )

    if (
        x_api_key is None
        or not secrets.compare_digest(
            x_api_key,
            expected_api_key,
        )
    ):
        raise HTTPException(
            status_code=401,
            detail="Invalid or missing API key.",
        )

router = APIRouter(
    prefix="/updates/invoices",
    tags=["Invoice Updates"],
)

class InvoiceCustomFieldsUpdateRequest(BaseModel):
    po_number: str | None = None
    wo_so_number: str | None = None

@router.get("/{doc_number}")
def get_invoice_for_update(
    doc_number: str,
    realmId: str,
    request: Request,
):
    """
    Read-only lookup endpoint.

    Finds a QBO invoice whose DocNumber matches
    the AvSight invoice number.
    """

    doc_number = doc_number.strip()

    if not doc_number:
        raise HTTPException(
            status_code=400,
            detail="Invoice number is required.",
        )

    get_valid_access_token = (
        request.app.state.get_valid_access_token
    )

    qbo_api_base = request.app.state.qbo_api_base

    try:
        access_token = get_valid_access_token(realmId)
    except RuntimeError as exc:
        message = str(exc)

        if message.startswith("RECONNECT_REQUIRED"):
            raise HTTPException(
                status_code=401,
                detail={
                    "error": "reconnect_required",
                    "connect_url": "/connect",
                    "message": message,
                },
            ) from exc

        raise HTTPException(
            status_code=500,
            detail={
                "error": "auth_failed",
                "message": message,
            },
        ) from exc

    escaped_doc_number = doc_number.replace(
        "\\",
        "\\\\",
    ).replace(
        "'",
        "\\'",
    )

    query = (
        "SELECT * FROM Invoice "
        f"WHERE DocNumber = '{escaped_doc_number}' "
        "MAXRESULTS 2"
    )

    url = (
        f"{qbo_api_base}/v3/company/"
        f"{realmId}/query"
    )

    response = requests.get(
        url,
        headers={
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
        },
        params={
            "query": query,
            "minorversion": "75",
        },
        timeout=30,
    )

    if not response.ok:
        raise HTTPException(
            status_code=502,
            detail={
                "error": "qbo_lookup_failed",
                "qbo_status_code": response.status_code,
                "qbo_response": response.text[:2000],
            },
        )

    payload = response.json()

    invoices = (
        payload
        .get("QueryResponse", {})
        .get("Invoice", [])
    )

    if not invoices:
        raise HTTPException(
            status_code=404,
            detail=(
                f"Invoice {doc_number} was not found "
                "in QuickBooks."
            ),
        )

    if len(invoices) > 1:
        raise HTTPException(
            status_code=409,
            detail=(
                f"More than one QuickBooks invoice "
                f"matched {doc_number}."
            ),
        )

    return invoices[0]

@router.patch("/by-doc-number/{doc_number}/custom-fields")
def update_invoice_custom_fields(
    doc_number: str,
    payload: InvoiceCustomFieldsUpdateRequest,
    realmId: str,
    request: Request,
    _: None = Depends(require_api_key),
):
    """
    Finds a QBO invoice by DocNumber and updates the
    P.O. Number and/or WO/SO Number custom fields.
    """

    doc_number = doc_number.strip()

    if not doc_number:
        raise HTTPException(
            status_code=400,
            detail="Invoice DocNumber is required.",
        )

    po_number = (
        payload.po_number.strip()
        if payload.po_number is not None
        else None
    )

    wo_so_number = (
        payload.wo_so_number.strip()
        if payload.wo_so_number is not None
        else None
    )

    if po_number is None and wo_so_number is None:
        raise HTTPException(
            status_code=400,
            detail=(
                "At least one value is required: "
                "po_number or wo_so_number."
            ),
        )

    get_valid_access_token = (
        request.app.state.get_valid_access_token
    )
    qbo_api_base = request.app.state.qbo_api_base

    try:
        access_token = get_valid_access_token(realmId)
    except RuntimeError as exc:
        message = str(exc)

        if message.startswith("RECONNECT_REQUIRED"):
            raise HTTPException(
                status_code=401,
                detail={
                    "error": "reconnect_required",
                    "connect_url": "/connect",
                    "message": message,
                },
            ) from exc

        raise HTTPException(
            status_code=500,
            detail={
                "error": "auth_failed",
                "message": message,
            },
        ) from exc

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

    # Step 1: Find the invoice using its DocNumber.
    escaped_doc_number = (
        doc_number
        .replace("\\", "\\\\")
        .replace("'", "\\'")
    )

    query = (
        "SELECT * FROM Invoice "
        f"WHERE DocNumber = '{escaped_doc_number}' "
        "MAXRESULTS 2"
    )

    query_url = (
        f"{qbo_api_base}/v3/company/"
        f"{realmId}/query"
    )

    query_response = requests.get(
        query_url,
        headers=headers,
        params={
            "query": query,
            "minorversion": "75",
        },
        timeout=30,
    )

    if not query_response.ok:
        raise HTTPException(
            status_code=502,
            detail={
                "error": "qbo_invoice_lookup_failed",
                "qbo_status_code": query_response.status_code,
                "qbo_response": query_response.text[:2000],
            },
        )

    query_payload = query_response.json()

    invoices = (
        query_payload
        .get("QueryResponse", {})
        .get("Invoice", [])
    )

    if not invoices:
        return {
            "success": True,
            "updated": False,
            "result": "invoice_not_found",
            "invoice_id": None,
            "doc_number": doc_number,
            "sync_token": None,
            "po_number": None,
            "wo_so_number": None,
            "message": (
                f"Invoice {doc_number} was not found in QuickBooks. "
                "No update was performed."
            ) 
        }

    if len(invoices) > 1:
        raise HTTPException(
            status_code=409,
            detail=(
                f"More than one QuickBooks invoice "
                f"matched DocNumber {doc_number}."
            ),
        )

    current_invoice = invoices[0]

    invoice_id = current_invoice.get("Id")
    sync_token = current_invoice.get("SyncToken")

    if not invoice_id:
        raise HTTPException(
            status_code=502,
            detail="QuickBooks returned no invoice Id.",
        )

    if sync_token is None:
        raise HTTPException(
            status_code=502,
            detail="QuickBooks returned no SyncToken.",
        )

    # Step 2: Build only the custom fields supplied.
    custom_fields = []

    if po_number is not None:
        custom_fields.append(
            {
                "DefinitionId": PO_NUMBER_DEFINITION_ID,
                "Name": "P.O. Number",
                "Type": "StringType",
                "StringValue": po_number,
            }
        )

    if wo_so_number is not None:
        custom_fields.append(
            {
                "DefinitionId": WO_SO_NUMBER_DEFINITION_ID,
                "Name": "WO/SO Number",
                "Type": "StringType",
                "StringValue": wo_so_number,
            }
        )

    update_payload = {
        "Id": invoice_id,
        "SyncToken": sync_token,
        "sparse": True,
        "CustomField": custom_fields,
    }

    # Step 3: Send the sparse update to QBO.
    update_url = (
        f"{qbo_api_base}/v3/company/"
        f"{realmId}/invoice"
    )

    update_response = requests.post(
        update_url,
        headers=headers,
        params={
            "operation": "update",
            "minorversion": "75",
        },
        json=update_payload,
        timeout=30,
    )

    if not update_response.ok:
        raise HTTPException(
            status_code=502,
            detail={
                "error": "qbo_invoice_update_failed",
                "qbo_status_code": update_response.status_code,
                "qbo_response": update_response.text[:2000],
            },
        )

    updated_invoice = update_response.json().get("Invoice")

    if not updated_invoice:
        raise HTTPException(
            status_code=502,
            detail="QuickBooks returned no updated Invoice object.",
        )

    # Step 4: Read the returned custom-field values.
    returned_po_number = None
    returned_wo_so_number = None

    for custom_field in updated_invoice.get("CustomField", []):
        definition_id = custom_field.get("DefinitionId")

        if definition_id == PO_NUMBER_DEFINITION_ID:
            returned_po_number = custom_field.get("StringValue")

        elif definition_id == WO_SO_NUMBER_DEFINITION_ID:
            returned_wo_so_number = custom_field.get("StringValue")

    return {
        "success": True,
        "updated": True,
        "result": "invoice_updated",
        "invoice_id": updated_invoice.get("Id"),
        "doc_number": updated_invoice.get("DocNumber"),
        "sync_token": updated_invoice.get("SyncToken"),
        "po_number": returned_po_number,
        "wo_so_number": returned_wo_so_number,
        "message": "QuickBooks invoice custom fields were updated.",
    }