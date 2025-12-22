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

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/db-health")
def db_health():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("select 1 as ok;")
            return cur.fetchone()

@app.get("/connect")
def connect(request: Request):
    """
    Step 1: Redirect the browser to Intuit's consent screen.
    """
    client_id = require_env("INTUIT_CLIENT_ID")
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
        secure=False,  # Render is https -> set TRUE in production later
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

    client_id = require_env("INTUIT_CLIENT_ID")
    client_secret = require_env("INTUIT_CLIENT_SECRET")
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
