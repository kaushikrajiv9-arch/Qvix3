#!/usr/bin/env python3
"""
rh_token_manager.py — Auto-refresh Robinhood OAuth2 tokens.

Uses ROBINHOOD_REFRESH_TOKEN to obtain a fresh access token from Robinhood's
OAuth2 endpoint without any browser interaction or MFA. Writes both new tokens
back to .env (refresh tokens rotate on each use).

Run daily at 5:30 AM ET via LaunchAgent (com.qvix.rh_token_refresh.plist).
Also imported by robinhood_bridge.py to auto-refresh on startup when the
access token is within 2h of expiry.
"""

import base64
import json
import logging
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

BASE_DIR = Path(__file__).parent
ENV_FILE  = BASE_DIR / ".env"

load_dotenv(ENV_FILE)

RH_TOKEN_URL  = "https://api.robinhood.com/oauth2/token/"
RH_CLIENT_ID  = "c82SH0WZOsabOXGP2sxqcj34FxkvfnWRZBKlBjFS"


def _decode_jwt_exp(token: str):
    parts = token.split(".")
    if len(parts) != 3:
        return None
    try:
        payload = json.loads(base64.urlsafe_b64decode(parts[1] + "=="))
        exp = payload.get("exp")
        return datetime.fromtimestamp(exp, tz=timezone.utc) if exp else None
    except Exception:
        return None


def token_hours_remaining(access_token: str) -> float:
    exp = _decode_jwt_exp(access_token)
    if exp is None:
        return -1.0
    return (exp - datetime.now(tz=timezone.utc)).total_seconds() / 3600


def token_needs_refresh(access_token: str, threshold_hours: float = 2.0) -> bool:
    return token_hours_remaining(access_token) < threshold_hours


def _call_token_endpoint(refresh_token: str) -> dict:
    device_token = os.getenv("ROBINHOOD_DEVICE_TOKEN", "")
    resp = requests.post(
        RH_TOKEN_URL,
        data={
            "grant_type":    "refresh_token",
            "refresh_token": refresh_token,
            "client_id":     RH_CLIENT_ID,
            "device_token":  device_token,
            "expires_in":    86400,
            "scope":         "internal",
        },
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept":        "application/json",
        },
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()


def _write_tokens_to_env(access_token: str, refresh_token: str) -> None:
    env_text = ENV_FILE.read_text()
    for key, val in [
        ("ROBINHOOD_ACCESS_TOKEN",  access_token),
        ("ROBINHOOD_REFRESH_TOKEN", refresh_token),
    ]:
        pattern = rf"^{key}=.*$"
        replacement = f"{key}={val}"
        if re.search(pattern, env_text, re.MULTILINE):
            env_text = re.sub(pattern, replacement, env_text, flags=re.MULTILINE)
        else:
            env_text += f"\n{replacement}"
    ENV_FILE.write_text(env_text)


def run_refresh(force: bool = False) -> bool:
    """Refresh the Robinhood access token. Returns True on success or if not needed."""
    load_dotenv(ENV_FILE, override=True)
    refresh_token = os.getenv("ROBINHOOD_REFRESH_TOKEN", "")
    if not refresh_token:
        logging.error("ROBINHOOD_REFRESH_TOKEN not set — cannot auto-refresh")
        return False

    access_token = os.getenv("ROBINHOOD_ACCESS_TOKEN", "")
    if not force and access_token and not token_needs_refresh(access_token):
        hours = token_hours_remaining(access_token)
        logging.info(f"RH token valid ({hours:.1f}h remaining) — no refresh needed")
        return True

    logging.info("Refreshing Robinhood access token...")
    try:
        data = _call_token_endpoint(refresh_token)
    except requests.HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else "?"
        body   = exc.response.text[:200] if exc.response is not None else ""
        logging.error(f"Token refresh HTTP {status}: {body}")
        return False
    except Exception as exc:
        logging.error(f"Token refresh error: {exc}")
        return False

    new_access  = data.get("access_token", "")
    new_refresh = data.get("refresh_token", refresh_token)

    if not new_access:
        logging.error(f"No access_token in response — keys: {list(data.keys())}")
        return False

    _write_tokens_to_env(new_access, new_refresh)

    exp     = _decode_jwt_exp(new_access)
    exp_str = exp.strftime("%Y-%m-%d %H:%M UTC") if exp else "unknown"
    logging.info(f"RH token refreshed — expires {exp_str}")

    # Re-export into current process so callers see the new token immediately.
    os.environ["ROBINHOOD_ACCESS_TOKEN"]  = new_access
    os.environ["ROBINHOOD_REFRESH_TOKEN"] = new_refresh
    return True


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [rh_token_manager] %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    force = "--force" in sys.argv
    ok = run_refresh(force=force)
    sys.exit(0 if ok else 1)
