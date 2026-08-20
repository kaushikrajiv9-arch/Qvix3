#!/usr/bin/env python3
"""
One-shot Monday morning Discord reminder: TJX $149P Aug 21 flow check.
Fires via launchd (com.qvix.tjx_reminder) at 9:00 AM ET on Aug 17, 2026.
Reads DISCORD_WEBHOOK_URL from .env, posts one embed, exits.
"""

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")

from discord_format import get_discord_webhook_url
WEBHOOK = get_discord_webhook_url()
FIRED_FLAG = Path(__file__).parent / "logs" / "tjx_reminder_fired.flag"


def main():
    if not WEBHOOK:
        print("ERROR: DISCORD_WEBHOOK_ID or ~/.qvix_discord_tokens.json not configured — aborting", file=sys.stderr)
        sys.exit(1)

    if FIRED_FLAG.exists():
        print(f"Already fired (flag: {FIRED_FLAG}) — skipping duplicate send")
        sys.exit(0)

    now_et = datetime.now(timezone.utc).strftime("%I:%M %p ET")

    embed = {
        "title": "🔍  TJX FLOW CHECK — $149P Aug 21",
        "color": 0xE67E22,
        "description": (
            "Earnings **Wed Aug 19 premarket** | Est EPS $1.18 | IV Rank 75.3%\n\n"
            "Pull the TJX $149P Aug 21 flow before or at open and check whether "
            "Friday's conviction is still building, plateaued, or has reversed."
        ),
        "fields": [
            {
                "name": "📌 Friday Close Context (Aug 14)",
                "value": (
                    "**$149P** mark: $1.85  (+42% from Thu close of $1.30)\n"
                    "Volume: **1,207** vs OI: **194** → **6.22× vol/OI** (fresh, new positioning)\n"
                    "Break-even at expiry: **$147.15** (−3.26% from $152.11 spot)\n"
                    "Market-priced earnings move: **±4.5%** (ATM straddle $6.82)\n"
                    "→ $149P profitable on any decline > 3.26%; straddle-implied floor ~$145.30"
                ),
                "inline": False,
            },
            {
                "name": "✅ Conviction BUILDING — green light",
                "value": (
                    "• $149P vol adding on open, mark holds above $1.85\n"
                    "• New OI printing above 194 (positions opened, not rolled)\n"
                    "• Net put premium stays positive (puts on ask side > bid)"
                ),
                "inline": True,
            },
            {
                "name": "⚠️ Conviction FADING — reassess",
                "value": (
                    "• Mark drops significantly from $1.85 on heavy vol\n"
                    "• $150P OI (anchor at 2,985) starts unwinding\n"
                    "• Net call premium turns positive (bulls re-entering)"
                ),
                "inline": True,
            },
            {
                "name": "📊 Anchor OI to Watch",
                "value": (
                    "$150P — **2,985 OI** (largest in chain, existing institutional hedge)\n"
                    "$145P — **807 OI**, vol=376 Friday (straddle-floor lottery layer)\n"
                    "$140P — **1,029 OI** (deep OTM, cheap tail hedge)"
                ),
                "inline": False,
            },
            {
                "name": "⏱️ Decision Window",
                "value": (
                    "Last session before Wed premarket print.\n"
                    "If flow is still building → enter before 10 AM.\n"
                    "Theta burns ~$0.30/contract/day — time decay accelerates into Wed."
                ),
                "inline": False,
            },
        ],
        "footer": {"text": f"QVIX · Auto-reminder  ·  {now_et}"},
        "timestamp": datetime.utcnow().isoformat() + "Z",
    }

    try:
        r = requests.post(
            WEBHOOK,
            json={"username": "QVIX · Flow Alert", "embeds": [embed]},
            timeout=10,
        )
        if r.status_code in (200, 204):
            FIRED_FLAG.parent.mkdir(exist_ok=True)
            FIRED_FLAG.write_text(datetime.utcnow().isoformat())
            print(f"TJX reminder posted ({r.status_code})")
        else:
            print(f"Discord post failed: {r.status_code} {r.text[:200]}", file=sys.stderr)
            sys.exit(1)
    except Exception as exc:
        print(f"Discord post exception: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
