import os
import base64
import secrets
from urllib.parse import urlencode

import requests
from fastapi import FastAPI, Request
from fastapi.responses import RedirectResponse, JSONResponse
from dotenv import load_dotenv
from db import get_conn
from datetime import datetime, timedelta, timezone

load_dotenv()

app = FastAPI()

AUTHORIZE_URL = "https://appcenter.intuit.com/connect/oauth2"
TOKEN_URL = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"

def require_env(name: str) -> str:
    val = os.getenv(name)
    if not val:
        raise RuntimeError(f"Missing env var: {name}")
    return val

def intuit_env() -> str:
    # default to sandbox if not set
    return os.getenv("INTUIT_ENV", "sandbox").lower().strip()

def intuit_is_prod() -> bool:
    return intuit_env() in ("prod", "production")

# This one matters for data calls (Invoices, Customers, Items, etc.)
QBO_API_BASE = (
    "https://quickbooks.api.intuit.com"
    if intuit_is_prod()
    else "https://sandbox-quickbooks.api.intuit.com"
)

def get_intuit_client_id() -> str:
    if intuit_is_prod():
        return require_env("INTUIT_CLIENT_ID")
    return require_env("INTUIT_CLIENT_ID_SANDBOX")

def get_intuit_client_secret() -> str:
    if intuit_is_prod():
        return require_env("INTUIT_CLIENT_SECRET")
    return require_env("INTUIT_CLIENT_SECRET_SANDBOX")

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/db-health")
def db_health():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("select 1 as ok;")
            return cur.fetchone()

@app.get("/auth/status")
def auth_status(realmId: str):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "select expires_at, refresh_expires_at, updated_at from qbo_tokens where realm_id=%s",
                (realmId,),
            )
            row = cur.fetchone() or {}

    return {"realmId": realmId, **row}

def refresh_access_token(realm_id: str) -> str:
    """
    Uses stored refresh_token to fetch a new access_token.
    Updates qbo_tokens with new access_token, refresh_token (if rotated), expires_at, refresh_expires_at.
    Returns the new access_token.
    """
    client_id = get_intuit_client_id()
    client_secret = get_intuit_client_secret()

    # 1) read refresh_token from DB
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("select refresh_token from qbo_tokens where realm_id=%s", (realm_id,))
            row = cur.fetchone()
    if not row or not row.get("refresh_token"):
        raise RuntimeError("RECONNECT_REQUIRED: no refresh token stored")

    refresh_token = row["refresh_token"]
    basic = base64.b64encode(f"{client_id}:{client_secret}".encode("utf-8")).decode("utf-8")

    # 2) call Intuit refresh
    token_resp = requests.post(
        TOKEN_URL,
        headers={
            "Authorization": f"Basic {basic}",
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
        },
        data={
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        },
        timeout=30,
    )

    data = token_resp.json() if token_resp.content else {}

    if token_resp.status_code >= 400:
        # invalid_grant means refresh token expired/revoked -> must re-authorize (admin)
        err = (data.get("error") or "").lower()
        if "invalid_grant" in err:
            raise RuntimeError("RECONNECT_REQUIRED: invalid_grant (refresh token expired/revoked)")
        raise RuntimeError(f"token_refresh_failed: {data}")

    new_access_token = data.get("access_token")
    new_refresh_token = data.get("refresh_token")  # Intuit often rotates refresh tokens
    if not new_access_token:
        raise RuntimeError(f"token_refresh_failed: missing access_token: {data}")

    expires_in = int(data.get("expires_in", 3600))
    expires_at = datetime.now(timezone.utc) + timedelta(seconds=expires_in)

    refresh_expires_in = data.get("x_refresh_token_expires_in")
    refresh_expires_at = None
    if refresh_expires_in:
        refresh_expires_at = datetime.now(timezone.utc) + timedelta(seconds=int(refresh_expires_in))

    # 3) update DB (keep old refresh token if Intuit didn't rotate it)
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                update qbo_tokens
                   set access_token=%s,
                       refresh_token=%s,
                       expires_at=%s,
                       refresh_expires_at=%s,
                       updated_at=now()
                 where realm_id=%s
                """,
                (
                    new_access_token,
                    new_refresh_token or refresh_token,
                    expires_at,
                    refresh_expires_at,
                    realm_id,
                ),
            )
        conn.commit()

    return new_access_token

def get_valid_access_token(realm_id: str, refresh_skew_seconds: int = 300) -> str:
    """
    Returns a currently-valid access token.
    Refreshes automatically if it expires within refresh_skew_seconds (default 5 minutes).
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("select access_token, expires_at from qbo_tokens where realm_id=%s", (realm_id,))
            row = cur.fetchone()

    if not row or not row.get("access_token") or not row.get("expires_at"):
        raise RuntimeError("RECONNECT_REQUIRED: missing stored tokens")

    expires_at = row["expires_at"]
    now = datetime.now(timezone.utc)

    # refresh early
    if expires_at <= now + timedelta(seconds=refresh_skew_seconds):
        return refresh_access_token(realm_id)

    return row["access_token"]

@app.get("/connect")
def connect(request: Request):
    """
    Step 1: Redirect the browser to Intuit's consent screen.
    """
    client_id = get_intuit_client_id()
    redirect_uri = require_env("INTUIT_REDIRECT_URI")
    scope = os.getenv("INTUIT_SCOPE", "com.intuit.quickbooks.accounting")

    state = secrets.token_hex(16)

    # Store the state in a cookie (simple CSRF protection).
    # Later in /oauth/callback we verify it matches.
    params = {
        "client_id": client_id,
        "response_type": "code",
        "scope": scope,
        "redirect_uri": redirect_uri,
        "state": state,
    }
    url = f"{AUTHORIZE_URL}?{urlencode(params)}"

    resp = RedirectResponse(url=url)
    resp.set_cookie(
        key="qbo_oauth_state",
        value=state,
        httponly=True,
        samesite="lax",
        secure=True,  # Render is https -> set TRUE in production later
        max_age=10 * 60,
    )
    return resp

@app.get("/oauth/callback")
def oauth_callback(request: Request, code: str | None = None, realmId: str | None = None, state: str | None = None):
    """
    Step 2: Intuit redirects here with ?code=...&realmId=...&state=...
    We exchange code -> access_token + refresh_token.
    """
    if not code or not realmId or not state:
        return JSONResponse({"error": "missing_params", "code": code, "realmId": realmId, "state": state}, status_code=400)

    cookie_state = request.cookies.get("qbo_oauth_state")
    if not cookie_state or cookie_state != state:
        return JSONResponse({"error": "invalid_state"}, status_code=400)

    client_id = get_intuit_client_id()
    client_secret = get_intuit_client_secret()
    redirect_uri = require_env("INTUIT_REDIRECT_URI")

    basic = base64.b64encode(f"{client_id}:{client_secret}".encode("utf-8")).decode("utf-8")

    token_resp = requests.post(
        TOKEN_URL,
        headers={
            "Authorization": f"Basic {basic}",
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
        },
        data={
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": redirect_uri,
        },
        timeout=30,
    )

    data = token_resp.json() if token_resp.content else {}
    if token_resp.status_code >= 400:
        return JSONResponse({"error": "token_exchange_failed", "details": data}, status_code=500)

    # Compute expiry timestamps
    expires_in = int(data.get("expires_in", 3600))
    expires_at = datetime.now(timezone.utc) + timedelta(seconds=expires_in)
    
    refresh_expires_in = data.get("x_refresh_token_expires_in")
    refresh_expires_at = None
    if refresh_expires_in:
        refresh_expires_at = datetime.now(timezone.utc) + timedelta(seconds=int(refresh_expires_in))
    
    access_token = data.get("access_token")
    refresh_token = data.get("refresh_token")
    
    if not access_token or not refresh_token:
        return JSONResponse({"error": "missing_tokens_in_response", "details": data}, status_code=500)
    
    # Store tokens safely in Supabase (qbo_tokens)
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                insert into qbo_tokens (
                  realm_id, access_token, refresh_token, expires_at, refresh_expires_at, updated_at
                )
                values (%s, %s, %s, %s, %s, now())
                on conflict (realm_id)
                do update set
                  access_token = excluded.access_token,
                  refresh_token = excluded.refresh_token,
                  expires_at = excluded.expires_at,
                  refresh_expires_at = excluded.refresh_expires_at,
                  updated_at = now();
            """, (realmId, access_token, refresh_token, expires_at, refresh_expires_at))
        conn.commit()
    
    # IMPORTANT: Do not return tokens
    return {"connected": True, "realmId": realmId}

@app.get("/qbo/company-info")
def company_info(realmId: str):
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

    url = f"{QBO_API_BASE}/v3/company/{realmId}/companyinfo/{realmId}?minorversion=75"
    r = requests.get(
        url,
        headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"},
        timeout=30,
    )
    return r.json()
