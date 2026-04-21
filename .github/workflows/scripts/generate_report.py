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
    "Ellie Alvarez":   "U03RVMW6Q
