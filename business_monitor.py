#!/usr/bin/env python3
"""
business_monitor.py — Weekly business progress tracker.

Reads business_tracker.json, computes milestone completion and overdue
status for each initiative, identifies the #1 focus action for the week,
and posts a Discord embed. Also writes logs/business_status.json for the
dashboard panel.

Usage:
  python3 business_monitor.py              # terminal report
  python3 business_monitor.py --discord    # terminal + Discord embed
"""

import json
import os
import sys
from datetime import date, datetime, timezone, timedelta
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

BASE_DIR    = Path(__file__).parent
TRACKER     = BASE_DIR / "business_tracker.json"
STATUS_OUT  = BASE_DIR / "logs" / "business_status.json"
DISCORD_URL = os.getenv("DISCORD_WEBHOOK_URL", "")

STATUS_COLOR = {"planning": 0x6366F1, "building": 0x06B6D4,
                "launched": 0x10B981, "researching": 0xF59E0B, "paused": 0x94A3B8}
STATUS_EMOJI = {"planning": "📋", "building": "🔨",
                "launched": "🚀", "researching": "🔬", "paused": "⏸"}


def load_tracker() -> dict:
    if not TRACKER.exists():
        print(f"ERROR: {TRACKER} not found.", file=sys.stderr)
        sys.exit(1)
    return json.loads(TRACKER.read_text())


def milestone_stats(initiative: dict) -> dict:
    ms       = initiative.get("milestones", [])
    total    = len(ms)
    done     = sum(1 for m in ms if m.get("done"))
    today    = date.today().isoformat()
    overdue  = [m for m in ms if not m.get("done") and m.get("due", "9999") < today]
    upcoming = [m for m in ms if not m.get("done") and m.get("due", "9999") >= today]
    pct      = round(done / total * 100) if total else 0
    return {
        "total": total, "done": done, "pct": pct,
        "overdue": overdue, "upcoming": upcoming,
    }


def days_to_launch(initiative: dict) -> int:
    tl = initiative.get("target_launch", "")
    if not tl:
        return 999
    try:
        return (date.fromisoformat(tl) - date.today()).days
    except ValueError:
        return 999


def pick_weekly_focus(initiatives: list) -> dict:
    """Return the single highest-priority initiative with the most urgent next action."""
    # Priority: first overdue milestone, then nearest launch, then priority order
    scored = []
    for ini in initiatives:
        if ini.get("status") == "paused":
            continue
        stats = milestone_stats(ini)
        dtl   = days_to_launch(ini)
        score = ini.get("priority", 9) * 1000 + (dtl if dtl > 0 else -100)
        if stats["overdue"]:
            score -= 5000  # bubble overdue items to top
        scored.append((score, ini))
    if not scored:
        return {}
    return min(scored, key=lambda x: x[0])[1]


def format_terminal(data: dict) -> str:
    lines = [f"Business Monitor — {date.today().isoformat()}", ""]
    for ini in data["initiatives"]:
        stats = milestone_stats(ini)
        dtl   = days_to_launch(ini)
        bar   = ("█" * (stats["pct"] // 10)).ljust(10, "░")
        launch_txt = f"  |  {dtl}d to launch" if dtl < 999 else ""
        lines.append(f"  {STATUS_EMOJI.get(ini['status'], '•')} {ini['name']:<28} [{bar}] {stats['pct']:3}%{launch_txt}")
        if stats["overdue"]:
            lines.append(f"    ⚠ OVERDUE: {', '.join(m['name'] for m in stats['overdue'])}")
        lines.append(f"    → {ini['next_action']}")
        if ini.get("blockers"):
            for b in ini["blockers"]:
                lines.append(f"    🚧 {b}")
        lines.append("")

    focus = data.get("_focus", {})
    if focus:
        lines.append("━" * 52)
        lines.append(f"THIS WEEK'S FOCUS: {focus['name']}")
        lines.append(f"  → {focus['next_action']}")
        lines.append("")

    mrr_total = sum(ini.get("current_mrr", 0) for ini in data["initiatives"])
    mrr_target = sum(ini.get("monthly_revenue_target", 0) for ini in data["initiatives"])
    lines.append(f"Total MRR: ${mrr_total:,}  /  Target: ${mrr_target:,}/mo")
    return "\n".join(lines)


def format_discord_embed(data: dict) -> dict:
    focus    = data.get("_focus", {})
    mrr      = sum(ini.get("current_mrr", 0) for ini in data["initiatives"])
    mrr_tgt  = sum(ini.get("monthly_revenue_target", 0) for ini in data["initiatives"])
    any_over = any(milestone_stats(i)["overdue"] for i in data["initiatives"])
    color    = 0xF59E0B if any_over else 0x6366F1

    fields = []

    for ini in data["initiatives"]:
        stats = milestone_stats(ini)
        dtl   = days_to_launch(ini)
        bar   = ("█" * (stats["pct"] // 10)).ljust(10, "░")
        lines = [f"`[{bar}]` {stats['pct']}% · {stats['done']}/{stats['total']} milestones"]
        if stats["overdue"]:
            lines.append("⚠️ **OVERDUE:** " + ", ".join(m["name"] for m in stats["overdue"]))
        if dtl < 999:
            lines.append(f"📅 {dtl} days to launch")
        lines.append(f"→ {ini['next_action']}")
        if ini.get("blockers"):
            lines.append("🚧 " + " · ".join(ini["blockers"])[:120])
        fields.append({
            "name":   f"{STATUS_EMOJI.get(ini['status'], '•')} {ini['name']}",
            "value":  "\n".join(lines)[:1024],
            "inline": False,
        })

    if focus:
        fields.append({
            "name":   "🎯 THIS WEEK'S FOCUS",
            "value":  f"**{focus['name']}**\n→ {focus['next_action']}",
            "inline": False,
        })

    return {
        "title":       f"📊 Business Monitor — {date.today().strftime('%b %d, %Y')}",
        "description": f"MRR: **${mrr:,}** / ${mrr_tgt:,} target  ·  {len(data['initiatives'])} initiatives active",
        "color":       color,
        "fields":      fields,
        "footer":      {"text": "Update milestones: edit business_tracker.json  ·  python3 business_monitor.py --discord"},
    }


def write_status_json(data: dict) -> None:
    STATUS_OUT.parent.mkdir(exist_ok=True)
    payload = {
        "ts":          datetime.now(timezone.utc).isoformat(),
        "initiatives": [
            {
                "id":       ini["id"],
                "name":     ini["name"],
                "status":   ini["status"],
                "priority": ini["priority"],
                "stats":    milestone_stats(ini),
                "dtl":      days_to_launch(ini),
                "mrr":      ini.get("current_mrr", 0),
                "mrr_tgt":  ini.get("monthly_revenue_target", 0),
                "next":     ini.get("next_action", ""),
                "blockers": ini.get("blockers", []),
            }
            for ini in data["initiatives"]
        ],
        "focus_id": data.get("_focus", {}).get("id", ""),
    }
    STATUS_OUT.write_text(json.dumps(payload, indent=2))


def post_discord(embed: dict) -> None:
    import requests
    try:
        r = requests.post(DISCORD_URL, json={"embeds": [embed]}, timeout=10)
        r.raise_for_status()
    except Exception as exc:
        print(f"Discord post failed: {exc}", file=sys.stderr)


def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description="QVIX business progress monitor")
    parser.add_argument("--discord", action="store_true", help="Post to Discord")
    args = parser.parse_args()

    data = load_tracker()
    data["_focus"] = pick_weekly_focus(data["initiatives"])

    print(format_terminal(data))
    write_status_json(data)

    if args.discord:
        if not DISCORD_URL:
            print("DISCORD_WEBHOOK_URL not set.", file=sys.stderr)
        else:
            post_discord(format_discord_embed(data))
            print("Discord embed posted.")


if __name__ == "__main__":
    main()
