"""
Customer Growth — Weekly Intelligence Report
=============================================
Pulls live data from Looker + HubSpot, generates an HTML report via
Claude API, uploads it to GitHub Gist for a shareable link, posts a
rich summary to Slack, and updates the team's Slack Canvas.

Usage:
    python scripts/generate_report.py --now    # runs immediately
"""

import os, re, json, sys, time
import requests
from datetime import datetime, timedelta
from collections import defaultdict

# ── Config ─────────────────────────────────────────────────────────────────
LOOKER_BASE_URL      = os.getenv("LOOKER_BASE_URL", "https://monsterlg.looker.com")
LOOKER_CLIENT_ID     = os.getenv("LOOKER_CLIENT_ID")
LOOKER_CLIENT_SECRET = os.getenv("LOOKER_CLIENT_SECRET")
ANTHROPIC_API_KEY    = os.getenv("ANTHROPIC_API_KEY")
HUBSPOT_TOKEN        = os.getenv("HUBSPOT_ACCESS_TOKEN")
SLACK_BOT_TOKEN      = os.getenv("SLACK_BOT_TOKEN")
SLACK_CHANNEL_ID     = os.getenv("SLACK_CHANNEL_ID", "C0AUSR83YMP")
SLACK_CANVAS_ID      = os.getenv("SLACK_CANVAS_ID", "F0AUBSHSLK0")
GITHUB_TOKEN         = os.getenv("GITHUB_TOKEN")

TEAM = {
    "Molly Thomas":    "U080T9KQY69",
    "Ashley Weik":     "U01FXGDKZD5",
    "Meghan Clothier": "U08S86GT0SW",
    "Ellie Alvarez":   "U03RVMW6QD8",
    "Brandon":         "U0A0NJBAPSL",
    "Taryn":           "UPVN9MB24",
}

PIPELINE_MARKETING = "685533872"
PIPELINE_PLATFORM  = "868174516"
AT_RISK_WEEKS = 4
CHURN_WEEKS   = 8

MARKETING_STAGES = {
    "1188182697": "Welcome Docs",
    "1003781301": "Kick Off Call",
    "1033521217": "Campaign Setup",
    "1007507123": "Insights Orientation",
    "1188182702": "First 4 Weeks",
    "1188076969": "5-8 Weeks",
    "1188294551": "9-12 Weeks",
    "1188182703": "Steady State",
    "1038961053": "Sent Back to Sales",
}
PLATFORM_STAGES = {
    "1300153190": "Prospect",
    "1300009485": "Setup/Config",
    "1300153192": "Setup Complete",
    "1300153609": "Platform Training",
    "1300153193": "Complete",
    "1316887615": "Closed/Lost",
}
MARKETING_TERMINAL = {"1038961053", "1188182703"}
PLATFORM_TERMINAL  = {"1316887615", "1300153193"}


# ── Looker ──────────────────────────────────────────────────────────────────

def looker_login():
    r = requests.post(f"{LOOKER_BASE_URL}/api/4.0/login",
        data={"client_id": LOOKER_CLIENT_ID, "client_secret": LOOKER_CLIENT_SECRET},
        headers={"Content-Type": "application/x-www-form-urlencoded"}, timeout=30)
    r.raise_for_status()
    return r.json()["access_token"]


def looker_query(token, fields, filters, sorts=None, limit=5000):
    r = requests.post(f"{LOOKER_BASE_URL}/api/4.0/queries/run/json",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={"model": "General_Reporting", "view": "order",
              "fields": fields, "filters": filters,
              "sorts": sorts or [], "limit": str(limit)}, timeout=90)
    r.raise_for_status()
    return r.json()


def pull_looker_data():
    print("  [Looker] Authenticating...")
    token = looker_login()
    base = {
        "office_company.Company_Name": "-Sales Boomerang",
        "person.Account_Managers":     "-Others,-NULL",
        "order.dropdate_week":         "8 weeks",
    }
    by_co = looker_query(token, [
        "person.Account_Managers", "office_company.Company_Name",
        "order.dropdate_week", "order.item_count_sum",
        "ordercostandrevenue.gross_invoice_revenue_sum"
    ], base, ["order.dropdate_week desc"])

    by_csm = looker_query(token, [
        "person.Account_Managers", "order.dropdate_week",
        "order.item_count_sum", "ordercostandrevenue.gross_invoice_revenue_sum"
    ], base, ["order.dropdate_week desc"])

    print(f"  [Looker] {len(by_co)} rows")
    return by_co, by_csm


def analyze_looker(by_co, by_csm):
    all_weeks = sorted(set(r["order.dropdate_week"] for r in by_co), reverse=True)
    cutoff = (datetime.now() - timedelta(days=datetime.now().weekday())).strftime("%Y-%m-%d")
    full_weeks = [w for w in all_weeks if w < cutoff]

    co_rev = defaultdict(dict)
    co_csm = {}
    for r in by_co:
        co, wk = r["office_company.Company_Name"], r["order.dropdate_week"]
        rev = r["ordercostandrevenue.gross_invoice_revenue_sum"] or 0
        co_rev[co][wk] = co_rev[co].get(wk, 0) + rev
        if r["person.Account_Managers"]:
            co_csm[co] = r["person.Account_Managers"]

    co_status = {}
    for co in co_rev:
        revs = [co_rev[co].get(w, 0) for w in full_weeks]
        zeros = next((i for i, v in enumerate(revs) if v > 0), len(revs))
        last_active = next((v for v in revs if v > 0), 0)
        latest = revs[0] if revs else 0
        prev   = revs[1] if len(revs) > 1 else 0
        wow    = round((latest - prev) / prev * 100, 1) if prev else 0

        if zeros >= CHURN_WEEKS:     status = "CHURNED"
        elif zeros >= AT_RISK_WEEKS: status = "AT-RISK"
        elif zeros >= 2:             status = "WATCH-INACTIVE"
        else:
            active = [v for v in revs if v > 0]
            status = "DECLINING" if len(active) >= 3 and active[0] < active[1] else "HEALTHY"

        co_status[co] = {
            "status": status, "csm": co_csm.get(co, "?"),
            "zeros": zeros, "last_active": last_active,
            "latest": latest, "prev": prev, "wow": wow,
            "weekly": {w: co_rev[co].get(w, 0) for w in full_weeks},
        }

    csm_raw = defaultdict(dict)
    for r in by_csm:
        csm, wk = r["person.Account_Managers"], r["order.dropdate_week"]
        if wk in full_weeks:
            csm_raw[csm][wk] = r["ordercostandrevenue.gross_invoice_revenue_sum"] or 0

    csm_sum = {}
    for csm, wrev in csm_raw.items():
        sw   = sorted(wrev.keys(), reverse=True)
        revs = [wrev[w] for w in sw]
        l, p, o = (revs[0] if revs else 0), (revs[1] if len(revs)>1 else 0), (revs[-1] if revs else 0)
        csm_sum[csm] = {
            "latest": l, "prev": p,
            "wow":      round((l-p)/p*100,1) if p else 0,
            "eight_wk": round((l-o)/o*100,1) if o else 0,
            "weekly": wrev, "weeks": sw,
        }

    return {
        "weeks": full_weeks, "co_status": co_status, "csm": csm_sum,
        "as_of": full_weeks[0] if full_weeks else datetime.now().strftime("%Y-%m-%d"),
    }


# ── HubSpot ─────────────────────────────────────────────────────────────────

def hs_search(obj, filter_groups=None, props=None, sorts=None, limit=50):
    r = requests.post(f"https://api.hubspot.com/crm/v3/objects/{obj}/search",
        headers={"Authorization": f"Bearer {HUBSPOT_TOKEN}", "Content-Type": "application/json"},
        json={"filterGroups": filter_groups or [], "properties": props or [],
              "sorts": sorts or [{"propertyName": "hs_lastmodifieddate", "direction": "DESCENDING"}],
              "limit": limit}, timeout=30)
    r.raise_for_status()
    return r.json().get("results", [])


def strip_html(t):
    return re.sub(r'\s+', ' ', re.sub(r'<[^>]+>', ' ', t or '')).strip()


def pull_hubspot_data(co_status):
    print("  [HubSpot] Pulling pipelines + intelligence...")

    marketing = hs_search("tickets", [{"filters": [
        {"propertyName": "hs_pipeline", "operator": "EQ", "value": PIPELINE_MARKETING},
        {"propertyName": "hs_pipeline_stage", "operator": "NOT_IN", "values": list(MARKETING_TERMINAL)}
    ]}], ["subject","hs_pipeline_stage","hubspot_owner_id","createdate","hs_lastmodifieddate","content"], limit=50)

    platform = hs_search("tickets", [{"filters": [
        {"propertyName": "hs_pipeline", "operator": "EQ", "value": PIPELINE_PLATFORM},
        {"propertyName": "hs_pipeline_stage", "operator": "NOT_IN", "values": list(PLATFORM_TERMINAL)}
    ]}], ["subject","hs_pipeline_stage","hubspot_owner_id","createdate","hs_lastmodifieddate","content"], limit=50)

    flagged = [co for co, d in co_status.items()
               if d["status"] in ("AT-RISK","WATCH-INACTIVE","DECLINING")][:10]
    co_ids = []
    for name in flagged:
        try:
            res = hs_search("companies",
                [{"filters": [{"propertyName": "name", "operator": "EQ", "value": name}]}],
                ["name"], limit=1)
            if res: co_ids.append(int(res[0]["id"]))
        except Exception: pass

    notes, calls, contacts = [], [], []
    if co_ids:
        assoc = [{"associatedWith": [{"objectType": "companies", "operator": "IN", "objectIdValues": co_ids}]}]
        desc  = [{"propertyName": "hs_timestamp", "direction": "DESCENDING"}]
        try:
            for n in hs_search("notes", assoc, ["hs_note_body","hs_timestamp"], desc, 15):
                b = strip_html(n["properties"].get("hs_note_body",""))[:400]
                if b: notes.append({"date": (n["properties"].get("hs_timestamp") or "")[:10], "body": b})
        except Exception as e: print(f"  Notes: {e}")
        try:
            for c in hs_search("calls", assoc, ["hs_call_title","hs_call_body","hs_timestamp"], desc, 8):
                calls.append({"date": (c["properties"].get("hs_timestamp") or "")[:10],
                              "title": c["properties"].get("hs_call_title",""),
                              "body": strip_html(c["properties"].get("hs_call_body",""))[:500]})
        except Exception as e: print(f"  Calls: {e}")
        try:
            for c in hs_search("contacts", assoc,
                    ["firstname","lastname","jobtitle","notes_last_updated","hs_sales_email_last_replied","num_contacted_notes"],
                    [{"propertyName":"notes_last_updated","direction":"DESCENDING"}], 15):
                p = c["properties"]
                contacts.append({"name": f"{p.get('firstname','')} {p.get('lastname','')}".strip(),
                                 "title": p.get("jobtitle",""),
                                 "last_note": (p.get("notes_last_updated") or "")[:10],
                                 "last_reply": (p.get("hs_sales_email_last_replied") or "")[:10],
                                 "touches": p.get("num_contacted_notes","0")})
        except Exception as e: print(f"  Contacts: {e}")

    print(f"  [HubSpot] {len(marketing)} mktg · {len(platform)} platform · {len(notes)} notes · {len(calls)} calls")
    return {"marketing": marketing, "platform": platform,
            "notes": notes, "calls": calls, "contacts": contacts}


# ── HTML via Claude API ──────────────────────────────────────────────────────

def generate_html_report(looker, hubspot):
    print("  [Claude] Generating HTML report...")
    import anthropic
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    dept_total = sum(d["latest"] for d in looker["csm"].values())
    dept_prev  = sum(d["prev"]   for d in looker["csm"].values())
    dept_wow   = round((dept_total - dept_prev) / dept_prev * 100, 1) if dept_prev else 0
    today = datetime.now()

    mktg_enriched = []
    for t in hubspot["marketing"]:
        p = t["properties"]
        created = datetime.fromisoformat(p.get("createdate","").replace("Z","+00:00")) if p.get("createdate") else today
        weeks_in = max(0, (today - created.replace(tzinfo=None)).days // 7)
        mktg_enriched.append({
            "name": p.get("subject","").replace("Onboarding | ","").strip(),
            "stage": MARKETING_STAGES.get(p.get("hs_pipeline_stage",""), "Unknown"),
            "weeks_in": weeks_in,
            "last_updated": (p.get("hs_lastmodifieddate") or "")[:10],
            "notes": p.get("content",""),
        })

    plat_enriched = []
    for t in hubspot["platform"]:
        p = t["properties"]
        plat_enriched.append({
            "name": p.get("subject","").replace("| Loansure Platform Onboarding","").strip(),
            "stage": PLATFORM_STAGES.get(p.get("hs_pipeline_stage",""), "Unknown"),
            "last_updated": (p.get("hs_lastmodifieddate") or "")[:10],
            "notes": p.get("content",""),
        })

    payload = {
        "generated":   datetime.now().strftime("%B %d, %Y at %I:%M %p"),
        "week_of":     looker["as_of"],
        "dept":        {"total": round(dept_total), "wow": dept_wow},
        "csm_summaries": {
            csm: {"latest": round(d["latest"]), "wow": d["wow"], "eight_wk": d["eight_wk"],
                  "weekly": {k: round(v) for k, v in list(d["weekly"].items())[:7]}}
            for csm, d in looker["csm"].items()
        },
        "risks": {
            co: {"status": d["status"], "csm": d["csm"], "latest": round(d["latest"]),
                 "wow": d["wow"], "zeros": d["zeros"], "last_active": round(d["last_active"])}
            for co, d in looker["co_status"].items()
            if d["status"] in ("AT-RISK","CHURNED","WATCH-INACTIVE","DECLINING")
        },
        "healthy_top5": sorted(
            [{"co": co, "csm": d["csm"], "rev": round(d["latest"]), "wow": d["wow"]}
             for co, d in looker["co_status"].items() if d["status"] == "HEALTHY" and d["latest"] > 0],
            key=lambda x: x["rev"], reverse=True)[:5],
        "marketing_onboarding": mktg_enriched,
        "platform_onboarding":  plat_enriched,
        "hs_notes":    hubspot["notes"][:8],
        "hs_calls":    hubspot["calls"][:5],
        "hs_contacts": hubspot["contacts"][:10],
    }

    msg = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=8000,
        messages=[{"role": "user", "content": f"""Generate a complete polished HTML weekly intelligence report for the Customer Growth team at Monster Lead Group.

Data (live from Looker + HubSpot):
{json.dumps(payload, indent=2)}

Build a self-contained single-file HTML page with:
1. Header with report title, week of, generated timestamp
2. Department snapshot — 4 metric cards (total revenue WoW, active clients, onboarding count, at-risk count)
3. CSM scorecards (one card each for Molly, Ashley, Meghan, Ellie) with 7-week revenue table, WoW%, 8-wk%, and account-level flags
4. Chart.js line chart — 8-week revenue trend by CSM
5. Marketing Onboarding Pipeline table — all clients, stage, weeks in program, Looker revenue, signal
6. Platform (Loansure) Onboarding Pipeline table — all tickets, stage, notes, signal
7. Steady-state risk register — expandable cards per flagged account with Looker signal + HubSpot context + action
8. VP executive intelligence — 4-6 leadership bullets
9. Priority action list — numbered, color-coded by urgency, tagged by owner name

Design: clean white background, professional, mobile-responsive.
Colors: red=#E24B4A, amber=#EF9F27, green=#1D9E75, blue=#378ADD
Chart.js from cdnjs.cloudflare.com. Expandable risk cards via onclick toggle.

Return ONLY raw HTML starting with <!DOCTYPE html>. No markdown, no explanation."""}]
    )
    html = msg.content[0].text
    print(f"  [Claude] {len(html):,} chars generated")
    return html


# ── GitHub Gist ──────────────────────────────────────────────────────────────

def upload_to_gist(html, week_of):
    if not GITHUB_TOKEN:
        return None
    filename = f"customer_growth_{week_of}.html"
    r = requests.post("https://api.github.com/gists",
        headers={"Authorization": f"token {GITHUB_TOKEN}",
                 "Accept": "application/vnd.github.v3+json"},
        json={"description": f"Customer Growth Weekly Report — {week_of}",
              "public": False,
              "files": {filename: {"content": html}}}, timeout=30)
    r.raise_for_status()
    gist = r.json()
    raw_url = gist["files"][filename]["raw_url"]
    preview_url = f"https://htmlpreview.github.io/?{raw_url}"
    print(f"  [Gist] {preview_url}")
    return preview_url


# ── Slack ────────────────────────────────────────────────────────────────────

def slack_post(method, payload):
    r = requests.post(f"https://slack.com/api/{method}",
        headers={"Authorization": f"Bearer {SLACK_BOT_TOKEN}",
                 "Content-Type": "application/json"},
        json=payload, timeout=20)
    res = r.json()
    if not res.get("ok"):
        print(f"  [Slack] {method}: {res.get('error','unknown')}")
    return res


def post_slack_notification(looker, week_of, report_url):
    dept_total = sum(d["latest"] for d in looker["csm"].values())
    dept_prev  = sum(d["prev"]   for d in looker["csm"].values())
    dept_wow   = round((dept_total - dept_prev) / dept_prev * 100, 1) if dept_prev else 0

    csm_lines = "\n".join(
        f"• *{csm.split()[0]}*: ${d['latest']:,.0f} "
        f"{'↑' if d['wow']>0 else '↓'}{abs(d['wow'])}% WoW · "
        f"{'+' if d['eight_wk']>0 else ''}{d['eight_wk']}% 8-wk"
        for csm, d in sorted(looker["csm"].items(), key=lambda x: x[1]["latest"], reverse=True)
    )

    urgent = [
        f":red_circle: *{co}* ({d['csm'].split()[0]}) — {d['zeros']} wks dark"
        for co, d in looker["co_status"].items()
        if d["status"] in ("AT-RISK","CHURNED")
    ]
    watch = [
        f":large_orange_circle: *{co}* ({d['csm'].split()[0]}) — "
        f"{d['wow']:+.1f}% WoW · ${d['latest']:,.0f}/wk"
        for co, d in looker["co_status"].items()
        if d["status"] == "DECLINING"
    ]

    canvas_url = f"https://monsterleadgroup.slack.com/docs/T0A7VCVLH/{SLACK_CANVAS_ID}"

    blocks = [
        {"type": "header", "text": {"type": "plain_text",
            "text": f"📊 Customer Growth — Weekly Intelligence Report · {week_of}"}},
        {"type": "section", "fields": [
            {"type": "mrkdwn", "text": f"*Total Revenue*\n${dept_total:,.0f} "
                f"({'↑' if dept_wow>0 else '↓'}{abs(dept_wow)}% WoW)"},
            {"type": "mrkdwn", "text": f"*Accounts Flagged*\n{len(urgent)} at-risk · {len(watch)} declining"},
        ]},
        {"type": "divider"},
        {"type": "section", "text": {"type": "mrkdwn", "text": f"*CSM Performance*\n{csm_lines}"}},
    ]
    if urgent:
        blocks += [{"type": "divider"},
                   {"type": "section", "text": {"type": "mrkdwn",
                       "text": ":rotating_light: *Urgent*\n" + "\n".join(urgent[:5])}}]
    if watch:
        blocks.append({"type": "section", "text": {"type": "mrkdwn",
            "text": ":eyes: *Watch*\n" + "\n".join(watch[:5])}})

    links = f":slack: *<{canvas_url}|Open Live Canvas>*"
    if report_url:
        links += f"\n:page_facing_up: *<{report_url}|Open Full HTML Report>*"
    blocks += [{"type": "divider"},
               {"type": "section", "text": {"type": "mrkdwn", "text": links}},
               {"type": "context", "elements": [{"type": "mrkdwn",
                   "text": f"Generated {datetime.now():%b %d %Y %I:%M %p} · Looker + HubSpot · "
                           f"4 wks at-risk · 8 wks churned"}]}]

    slack_post("chat.postMessage", {"channel": SLACK_CHANNEL_ID, "blocks": blocks})


def update_canvas(looker, hubspot, week_of):
    dept_total = sum(d["latest"] for d in looker["csm"].values())
    dept_prev  = sum(d["prev"]   for d in looker["csm"].values())
    dept_wow   = round((dept_total - dept_prev) / dept_prev * 100, 1) if dept_prev else 0

    csm_rows = "\n".join(
        f"|{csm}|${d['latest']:,.0f}|{d['wow']:+.1f}% WoW|{d['eight_wk']:+.1f}% 8-wk|"
        for csm, d in sorted(looker["csm"].items(), key=lambda x: x[1]["latest"], reverse=True)
    )
    risks = "\n".join(
        f"- {'🔴' if d['status']=='AT-RISK' else '🟠'} **{co}** "
        f"({d['csm'].split()[0]}) — {d['zeros']} wks dark · last ${d['last_active']:,.0f}"
        for co, d in looker["co_status"].items()
        if d["status"] in ("AT-RISK","CHURNED","DECLINING","WATCH-INACTIVE")
    ) or "- No at-risk accounts this week 🟢"

    content = f""":bar_chart: **Week of {week_of}** · Updated {datetime.now().strftime('%B %d, %Y')}

---

# :dart: Department Snapshot

|Metric|Value|Signal|
|---|---|---|
|Total Weekly Revenue|**${dept_total:,.0f}**|{'↑' if dept_wow>0 else '↓'} {dept_wow:+.1f}% WoW|
|Marketing Onboarding|**{len(hubspot['marketing'])} clients**|Active|
|Platform Onboarding|**{len(hubspot['platform'])} clients**|Active|

---

# :busts_in_silhouette: CSM Performance

|CSM|Revenue|WoW|8-Week|
|---|---|---|---|
{csm_rows}

---

# :rotating_light: Risk Register

{risks}

---

# :white_check_mark: Actions

See full HTML report for detailed actions tagged by owner.

---

:robot_face: Auto-generated every Monday 7 AM via [GitHub Actions](https://github.com/bduensing/customergrowthteam/actions)
"""
    requests.post("https://slack.com/api/canvases.edit",
        headers={"Authorization": f"Bearer {SLACK_BOT_TOKEN}",
                 "Content-Type": "application/json"},
        json={"canvas_id": SLACK_CANVAS_ID,
              "changes": [{"operation": "replace",
                           "document_content": {"type": "markdown", "markdown": content}}]},
        timeout=20)
    print(f"  [Slack] Canvas updated")


# ── Save locally ─────────────────────────────────────────────────────────────

def save_report(html, week_of):
    os.makedirs("reports", exist_ok=True)
    path = f"reports/customer_growth_{week_of}.html"
    with open(path, "w") as f:
        f.write(html)
    print(f"  [Local] Saved: {path}")
    return path


# ── Main ─────────────────────────────────────────────────────────────────────

def run():
    print(f"\n{'='*55}")
    print(f"Customer Growth Report — {datetime.now():%A %B %d, %Y %H:%M}")
    print(f"{'='*55}")
    try:
        by_co, by_csm = pull_looker_data()
        looker  = analyze_looker(by_co, by_csm)
        week_of = looker["as_of"]
        print(f"  Week of: {week_of}")

        hubspot = pull_hubspot_data(looker["co_status"])
        html    = generate_html_report(looker, hubspot)
        save_report(html, week_of)

        report_url = None
        if GITHUB_TOKEN:
            try: report_url = upload_to_gist(html, week_of)
            except Exception as e: print(f"  [Gist] {e}")

        post_slack_notification(looker, week_of, report_url)
        update_canvas(looker, hubspot, week_of)

        print(f"\n✅ Done — week of {week_of}")
        if report_url:
            print(f"   Report: {report_url}")

    except Exception as e:
        import traceback; traceback.print_exc()
        if SLACK_BOT_TOKEN and SLACK_CHANNEL_ID:
            requests.post("https://slack.com/api/chat.postMessage",
                headers={"Authorization": f"Bearer {SLACK_BOT_TOKEN}",
                         "Content-Type": "application/json"},
                json={"channel": SLACK_CHANNEL_ID,
                      "text": f":x: *Report failed* — {datetime.now():%B %d}\n"
                              f"Error: `{str(e)[:200]}`\n"
                              f"https://github.com/bduensing/customergrowthteam/actions"},
                timeout=10)


if __name__ == "__main__":
    run()
