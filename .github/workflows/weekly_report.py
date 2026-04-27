"""
CSM Weekly Intelligence Report — Automated Pipeline
Monster Lead Group / Loansure

Pulls Looker + HubSpot data, generates HTML report via Claude API,
posts summary to Slack, and saves report to GitHub.

Setup:
    pip install requests anthropic schedule python-dotenv

Run manually:
    python weekly_report.py --now

Run on schedule (every Monday 9 AM):
    python weekly_report.py
"""

import os
import sys
import json
import requests
import anthropic
import schedule
import time
from datetime import datetime, timedelta
from collections import defaultdict
from dotenv import load_dotenv

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────
LOOKER_BASE_URL      = os.getenv("LOOKER_BASE_URL", "https://monsterlg.looker.com")
LOOKER_CLIENT_ID     = os.getenv("LOOKER_CLIENT_ID")
LOOKER_CLIENT_SECRET = os.getenv("LOOKER_CLIENT_SECRET")
ANTHROPIC_API_KEY    = os.getenv("ANTHROPIC_API_KEY")
HUBSPOT_TOKEN        = os.getenv("HUBSPOT_ACCESS_TOKEN")
SLACK_WEBHOOK_URL    = os.getenv("SLACK_WEBHOOK_URL")
SLACK_CHANNEL_ID     = os.getenv("SLACK_CHANNEL_ID")
GITHUB_TOKEN         = os.getenv("GITHUB_TOKEN")
GITHUB_REPO          = os.getenv("GITHUB_REPO", "bduensing/customergrowthteam")

# Churn thresholds
AT_RISK_WEEKS = 4
CHURN_WEEKS   = 8

# Team roster
CSM_TEAM = {
    "Molly Thomas":    "U080T9KQY69",
    "Ashley Weik":     "U01FXGDKZD5",
    "Meghan Clothier": "U08S86GT0SW",
    "Ellie Alvarez":   "U03RVMW6QD8",
}

EXCLUDE_COMPANIES = ["-Sales Boomerang"]
EXCLUDE_CSMS      = ["-Others", "-NULL"]


# ── Looker ────────────────────────────────────────────────────────────────────

def looker_login():
    print("  Authenticating with Looker...")
    resp = requests.post(
        f"{LOOKER_BASE_URL}/api/4.0/login",
        data={"client_id": LOOKER_CLIENT_ID, "client_secret": LOOKER_CLIENT_SECRET},
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=30
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def looker_query(token, fields, filters, sorts=None, limit=5000):
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload  = {
        "model":   "General_Reporting",
        "view":    "order",
        "fields":  fields,
        "filters": filters,
        "sorts":   sorts or [],
        "limit":   str(limit),
    }
    resp = requests.post(
        f"{LOOKER_BASE_URL}/api/4.0/queries/run/json",
        headers=headers,
        json=payload,
        timeout=60
    )
    resp.raise_for_status()
    return resp.json()


def pull_looker_data():
    """Pull 8 weeks of weekly revenue by CSM and company."""
    token = looker_login()

    # Calculate 8-week window
    today     = datetime.now()
    # Go back to last Monday
    last_mon  = today - timedelta(days=today.weekday())
    start     = last_mon - timedelta(weeks=8)
    date_filter = f"{start.strftime('%Y-%m-%d')} to {last_mon.strftime('%Y-%m-%d')}"

    print(f"  Pulling Looker data ({date_filter})...")

    rows = looker_query(
        token,
        fields=[
            "order.week",
            "order.csm_name",
            "order.company_name",
            "order.revenue",
        ],
        filters={"order.week": date_filter},
        sorts=["order.week desc"],
    )

    # Filter out exclusions
    rows = [
        r for r in rows
        if r.get("order.company_name") not in EXCLUDE_COMPANIES
        and r.get("order.csm_name") not in EXCLUDE_CSMS
        and r.get("order.revenue") is not None
    ]

    print(f"  ✓ {len(rows)} rows from Looker")

    # Organize by week → CSM → company
    weeks = sorted(set(r["order.week"] for r in rows), reverse=True)[:8]
    
    by_csm_week = defaultdict(lambda: defaultdict(float))
    by_company  = defaultdict(lambda: defaultdict(float))
    by_week     = defaultdict(float)

    for r in rows:
        week    = r["order.week"]
        csm     = r["order.csm_name"] or "Unknown"
        company = r["order.company_name"] or "Unknown"
        rev     = float(r["order.revenue"] or 0)

        by_csm_week[csm][week]        += rev
        by_company[company]["total"]  += rev
        by_company[company][week]     += rev
        by_company[company]["csm"]     = csm
        by_week[week]                 += rev

    return {
        "weeks":        weeks,
        "by_csm_week":  dict(by_csm_week),
        "by_company":   dict(by_company),
        "by_week":      dict(by_week),
        "raw":          rows,
    }


# ── HubSpot ───────────────────────────────────────────────────────────────────

def hs_get(path, params=None):
    resp = requests.get(
        f"https://api.hubapi.com/{path}",
        headers={"Authorization": f"Bearer {HUBSPOT_TOKEN}"},
        params=params or {},
        timeout=30
    )
    resp.raise_for_status()
    return resp.json()


def pull_hubspot_data(companies):
    """Pull recent notes and calls for flagged companies."""
    print("  Pulling HubSpot data...")
    notes = []
    calls = []

    try:
        # Pull recent notes (last 30 days)
        cutoff = int((datetime.now() - timedelta(days=30)).timestamp() * 1000)
        n_resp = hs_get("crm/v3/objects/notes", {
            "limit": 100,
            "properties": "hs_note_body,hs_timestamp,hubspot_owner_id",
            "filterGroups": json.dumps([{
                "filters": [{"propertyName": "hs_timestamp", "operator": "GTE", "value": str(cutoff)}]
            }])
        })
        notes = n_resp.get("results", [])

        # Pull recent calls
        c_resp = hs_get("crm/v3/objects/calls", {
            "limit": 50,
            "properties": "hs_call_body,hs_call_duration,hs_timestamp,hubspot_owner_id",
        })
        calls = c_resp.get("results", [])

    except Exception as e:
        print(f"  ⚠️  HubSpot error (non-fatal): {e}")

    print(f"  ✓ {len(notes)} notes, {len(calls)} calls from HubSpot")
    return {"notes": notes, "calls": calls}


# ── Churn Analysis ────────────────────────────────────────────────────────────

def analyze_churn(looker_data):
    """Classify each company by activity status."""
    weeks      = looker_data["weeks"]
    by_company = looker_data["by_company"]
    results    = {}

    for company, data in by_company.items():
        # Count consecutive dark weeks from most recent
        dark_streak = 0
        for w in weeks:
            if data.get(w, 0) == 0:
                dark_streak += 1
            else:
                break

        if dark_streak >= CHURN_WEEKS:
            status = "churned"
        elif dark_streak >= AT_RISK_WEEKS:
            status = "at_risk"
        elif dark_streak >= 2:
            status = "watch"
        else:
            status = "active"

        last_active = next((w for w in weeks if data.get(w, 0) > 0), None)
        last_rev    = data.get(last_active, 0) if last_active else 0

        results[company] = {
            "status":      status,
            "dark_streak": dark_streak,
            "last_active": last_active,
            "last_rev":    last_rev,
            "csm":         data.get("csm", "Unknown"),
            "total":       data.get("total", 0),
        }

    return results


# ── Claude Report Generation ───────────────────────────────────────────────────

def generate_report(looker_data, hubspot_data, churn_data):
    """Use Claude API to generate the full HTML report."""
    print("  Generating report via Claude API...")

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    weeks  = looker_data["weeks"]
    today  = datetime.now().strftime("%B %d, %Y")

    # Build data summary for Claude
    week_totals = [
        {"week": w, "total": looker_data["by_week"].get(w, 0)}
        for w in weeks
    ]

    csm_summary = []
    for csm in CSM_TEAM:
        csm_weeks = looker_data["by_csm_week"].get(csm, {})
        week_revs = [csm_weeks.get(w, 0) for w in weeks]
        curr      = week_revs[0] if week_revs else 0
        prev      = week_revs[1] if len(week_revs) > 1 else 0
        wow_pct   = ((curr - prev) / prev * 100) if prev else 0
        csm_summary.append({
            "csm":     csm,
            "current": curr,
            "prev":    prev,
            "wow_pct": round(wow_pct, 1),
            "weeks":   week_revs,
        })

    flagged = {
        k: v for k, v in churn_data.items()
        if v["status"] in ("at_risk", "watch", "churned")
    }

    notes_text = "\n".join([
        n.get("properties", {}).get("hs_note_body", "")[:200]
        for n in hubspot_data["notes"][:10]
    ])

    prompt = f"""You are generating the Monster Lead Group Customer Growth Team Weekly Intelligence Report for {today}.

Here is the data:

WEEKLY DEPARTMENT TOTALS (most recent first):
{json.dumps(week_totals, indent=2)}

CSM PERFORMANCE SUMMARY:
{json.dumps(csm_summary, indent=2)}

FLAGGED ACCOUNTS (at-risk / watch / churned):
{json.dumps(flagged, indent=2)}

RECENT HUBSPOT NOTES (sample):
{notes_text}

Generate a complete, professional HTML report with:
1. A clean header with Monster Lead Group branding (dark navy #1a2332, gold accent #c9a84c)
2. Department snapshot card — total revenue this week, WoW%, active client count, flagged accounts count
3. CSM scorecard for each of: Molly Thomas, Ashley Weik, Meghan Clothier, Ellie Alvarez
   - 8-week revenue table
   - WoW% change with color coding (green up, red down)
   - Account flags for their at-risk/watch accounts
4. Risk register — expandable cards for each flagged account with:
   - Status badge (🔴 At-Risk / ⚠️ Watch / 💀 Churned)
   - Weeks dark, last revenue, CSM owner
   - Any relevant HubSpot context
5. Priority action list (numbered, color coded by urgency)
6. Footer with data sources and generation timestamp

Use Chart.js for an 8-week trend line chart by CSM.
Make it mobile-friendly, professional, and executive-ready.
Return ONLY the complete HTML — no markdown, no explanation."""

    message = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=8000,
        messages=[{"role": "user", "content": prompt}]
    )

    html = message.content[0].text
    print(f"  ✓ Report generated ({len(html):,} chars)")
    return html


# ── Save & Distribute ─────────────────────────────────────────────────────────

def save_report_locally(html, date_str):
    filename = f"csm_report_{date_str}.html"
    filepath = os.path.expanduser(f"~/Downloads/{filename}")
    with open(filepath, "w") as f:
        f.write(html)
    print(f"  ✓ Saved locally: {filepath}")
    return filepath


def upload_to_github_gist(html, date_str):
    """Upload report as a secret GitHub Gist and return the URL."""
    if not GITHUB_TOKEN:
        print("  ⚠️  No GitHub token — skipping Gist upload")
        return None

    try:
        resp = requests.post(
            "https://api.github.com/gists",
            headers={
                "Authorization": f"Bearer {GITHUB_TOKEN}",
                "Accept": "application/vnd.github+json",
            },
            json={
                "description": f"CSM Weekly Report — {date_str}",
                "public": False,
                "files": {
                    f"csm_report_{date_str}.html": {"content": html}
                }
            },
            timeout=30
        )
        resp.raise_for_status()
        raw_url = resp.json()["files"][f"csm_report_{date_str}.html"]["raw_url"]
        view_url = f"https://htmlpreview.github.io/?{raw_url}"
        print(f"  ✓ Gist uploaded: {view_url}")
        return view_url
    except Exception as e:
        print(f"  ⚠️  GitHub Gist error: {e}")
        return None


def post_to_slack(looker_data, churn_data, report_url, date_str):
    """Post a rich summary to Slack with link to full report."""
    if not SLACK_WEBHOOK_URL:
        print("  ⚠️  No Slack webhook — skipping")
        return

    weeks       = looker_data["weeks"]
    curr_week   = weeks[0] if weeks else "N/A"
    prev_week   = weeks[1] if len(weeks) > 1 else None
    curr_total  = looker_data["by_week"].get(curr_week, 0)
    prev_total  = looker_data["by_week"].get(prev_week, 0) if prev_week else 0
    wow_pct     = ((curr_total - prev_total) / prev_total * 100) if prev_total else 0
    wow_arrow   = "↑" if wow_pct >= 0 else "↓"
    wow_color   = "good" if wow_pct >= 0 else "danger"

    flagged_count = sum(1 for v in churn_data.values() if v["status"] in ("at_risk", "watch"))
    active_count  = sum(1 for v in churn_data.values() if v["status"] == "active")

    # CSM lines
    csm_lines = []
    for csm in CSM_TEAM:
        csm_weeks = looker_data["by_csm_week"].get(csm, {})
        curr = csm_weeks.get(curr_week, 0)
        prev = csm_weeks.get(prev_week, 0) if prev_week else 0
        pct  = ((curr - prev) / prev * 100) if prev else 0
        arr  = "↑" if pct >= 0 else "↓"
        csm_lines.append(f"• {csm.split()[0]}: ${curr:,.0f} {arr} {abs(pct):.1f}% WoW")

    # At-risk accounts
    risk_lines = []
    for company, data in churn_data.items():
        if data["status"] == "at_risk":
            risk_lines.append(f"🔴 {company} ({data['csm'].split()[0]}) — {data['dark_streak']} wks dark")
        elif data["status"] == "watch":
            risk_lines.append(f"⚠️ {company} ({data['csm'].split()[0]}) — {data['dark_streak']} wks dark")

    report_link = f"\n\n<{report_url}|📄 View Full Report>" if report_url else ""

    payload = {
        "attachments": [{
            "color": "#1a2332",
            "blocks": [
                {
                    "type": "header",
                    "text": {"type": "plain_text", "text": f"📊 CSM Weekly Intelligence Report — {date_str}"}
                },
                {
                    "type": "section",
                    "fields": [
                        {"type": "mrkdwn", "text": f"*Total Revenue*\n${curr_total:,.0f} {wow_arrow} {abs(wow_pct):.1f}%"},
                        {"type": "mrkdwn", "text": f"*Active Clients*\n{active_count} accounts"},
                        {"type": "mrkdwn", "text": f"*Flagged Accounts*\n{flagged_count} need attention"},
                        {"type": "mrkdwn", "text": f"*Week Of*\n{curr_week}"},
                    ]
                },
                {"type": "divider"},
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": "*CSM Performance*\n" + "\n".join(csm_lines)}
                },
            ] + ([
                {"type": "divider"},
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": "*🚨 Risk Register*\n" + "\n".join(risk_lines[:5])}
                }
            ] if risk_lines else []) + ([
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": report_link}
                }
            ] if report_url else [])
        }]
    }

    try:
        resp = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=15)
        resp.raise_for_status()
        print("  ✓ Posted to Slack")
    except Exception as e:
        print(f"  ⚠️  Slack error: {e}")


# ── Main ──────────────────────────────────────────────────────────────────────

def run_report():
    date_str = datetime.now().strftime("%Y-%m-%d")
    print("\n" + "="*60)
    print(f"  CSM Report — {datetime.now().strftime('%A %B %d, %Y %H:%M')}")
    print("="*60)

    try:
        # 1. Pull data
        looker_data  = pull_looker_data()
        churn_data   = analyze_churn(looker_data)
        hubspot_data = pull_hubspot_data(list(looker_data["by_company"].keys()))

        # 2. Generate report
        html = generate_report(looker_data, hubspot_data, churn_data)

        # 3. Save locally
        local_path = save_report_locally(html, date_str)

        # 4. Upload to GitHub Gist
        report_url = upload_to_github_gist(html, date_str)

        # 5. Post to Slack
        post_to_slack(looker_data, churn_data, report_url, date_str)

        print(f"\n✅ Done — {date_str}")
        if local_path:
            print(f"   Local: {local_path}")
        if report_url:
            print(f"   Online: {report_url}")

    except Exception as e:
        print(f"\n❌ Report failed: {e}")
        raise


def run_scheduler():
    """Run every Monday at 9:00 AM."""
    print("📅 Scheduler started — will run every Monday at 9:00 AM")
    print("   (Leave this terminal window open)")
    print("   Press Ctrl+C to stop\n")
    schedule.every().monday.at("09:00").do(run_report)
    while True:
        schedule.run_pending()
        time.sleep(30)


# ── Entry Point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if "--now" in sys.argv:
        run_report()
    else:
        run_scheduler()
