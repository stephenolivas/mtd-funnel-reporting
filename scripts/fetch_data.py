#!/usr/bin/env python3
"""
MTD Funnel Reporting Dashboard — Data Fetcher
Fetches meetings from Close CRM, classifies by funnel, writes data.json + archives.

PIPELINE ORDER (critical — do not reorder):
  1. Paginate ALL meetings (~107 API calls)
  2. Filter by MTD date range in Python (UTC → Pacific)
  3. Classify meeting titles in Python (zero API calls)
  4. Collect unique lead_ids from surviving meetings
  5. Fetch ONLY those leads individually (~80-200 API calls)
  6. Apply lead-level exclusions at fetch time

NOTE on setter calls:
  Setter meetings (Kristin Nelson, Spencer Reynolds, "Vending Quick Discovery") ARE
  counted in the funnel totals — they are NOT separated out. Each funnel card shows
  "Sales X / Setter Y" as a breakdown sub-line, but MTD Booked = sales + setter.
"""

import json
import os
import re
import time
import calendar
from datetime import datetime, date, timezone, timedelta
from zoneinfo import ZoneInfo
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ─── Config ───────────────────────────────────────────────────────────────────

CLOSE_API_BASE = "https://api.close.com/api/v1"
CLOSE_API_KEY = os.environ["CLOSE_API_KEY"]
PACIFIC = ZoneInfo("America/Los_Angeles")

# Custom field IDs
CF_FUNNEL_NAME = "cf_xqDQE8fkPsWa0RNEve7hcaxKblCe6489XeZGRDzyPdX"  # Funnel Name DEAL
CF_LEAD_OWNER  = "cf_gOfS9pFwext58oberEegLyix8hZzeHrxhCZOVh3P3rd"   # Lead Owner (user_id)

# Lead fields to fetch (minimized — 50-100x smaller payload vs full lead object)
LEAD_FIELDS = (
    f"id,display_name,status_id,"
    f"custom.{CF_FUNNEL_NAME},"
    f"custom.{CF_LEAD_OWNER}"
)

# ─── Hard User Exclusions (meeting-level) ─────────────────────────────────────

EXCLUDED_USER_IDS = {
    "user_5cZRqXu8kb4O1IeBVA98UMcMEhYZUhx1fnCHfSL0YMV",  # Stephen Olivas
    "user_yRF070m26JE67J6CJqzkAB3IqY7btNm1K5RisCglKa6",  # Ahmad Bukhari
}

# Lead-level status exclusions
EXCLUDED_STATUS_IDS = {
    "stat_hWIGHjzyNpl4YjIFSFz3VK4fp2ny10SFJLKAihmo4KT",  # Canceled (by Lead)
    "stat_YV4ZngDB4IGjLjlOf0YTFEWuKZJ6fhNxVkzQkvKYfdB",  # Outside the US
}

# Setter owners — counted in funnel totals, flagged for breakdown sub-line
SETTER_OWNER_NAMES = {"Kristin Nelson", "Spencer Reynolds"}

# ─── Session Setup ─────────────────────────────────────────────────────────────

session = requests.Session()
session.auth = (CLOSE_API_KEY, "")
session.mount("https://", HTTPAdapter(max_retries=Retry(total=0)))


def close_get(endpoint, params=None):
    """Single Close API GET with 0.5s throttle + retry on 429."""
    time.sleep(0.5)  # Global throttle — DO NOT REMOVE
    url = f"{CLOSE_API_BASE}/{endpoint}"
    for attempt in range(5):
        try:
            resp = session.get(url, params=params or {}, timeout=60)
        except requests.RequestException as e:
            print(f"  Network error (attempt {attempt+1}): {e}", flush=True)
            time.sleep(5)
            continue
        if resp.status_code == 429:
            wait = float(resp.headers.get("Retry-After", 5))
            print(f"  Rate limited — sleeping {wait}s", flush=True)
            time.sleep(wait)
            continue
        resp.raise_for_status()
        return resp.json()
    resp.raise_for_status()


# ─── Custom Field Helper ───────────────────────────────────────────────────────

def get_custom_field(lead, field_id):
    """
    Close API returns _fields-filtered custom fields in TWO possible shapes:
      1. Nested:  lead["custom"][field_id]         (standard full-object response)
      2. Flat:    lead[f"custom.{field_id}"]       (when using _fields param)

    We check both so the code is robust regardless of API behavior.
    """
    # Try nested first (most common for full objects)
    nested = (lead.get("custom") or {}).get(field_id)
    if nested not in (None, ""):
        return nested

    # Try flat key (Close flattens _fields-selected custom attrs in some responses)
    flat = lead.get(f"custom.{field_id}")
    if flat not in (None, ""):
        return flat

    return None


# ─── User Name Resolution ──────────────────────────────────────────────────────

def fetch_users():
    """Returns {user_id: display_name} for the org."""
    print("Fetching org users...", flush=True)
    data = close_get("user/", {"_limit": 100})
    users = {}
    for u in data.get("data", []):
        users[u["id"]] = u.get("display_name", "Unknown")
    print(f"  Loaded {len(users)} users", flush=True)
    return users


# ─── Meeting Pagination ────────────────────────────────────────────────────────

def fetch_all_meetings():
    """
    Paginate ALL meetings from Close API.
    IMPORTANT: Date filter params are silently ignored by Close — we MUST paginate
    everything and filter in Python after UTC → Pacific conversion.
    """
    print("Fetching all meetings (paginating)...", flush=True)
    all_meetings = []
    skip = 0
    limit = 100
    page = 0

    while True:
        page += 1
        data = close_get("activity/meeting/", {"_skip": skip, "_limit": limit})
        batch = data.get("data", [])
        all_meetings.extend(batch)
        print(f"  Page {page}: {len(batch)} meetings (total: {len(all_meetings)})", flush=True)
        if not data.get("has_more", False):
            break
        skip += limit

    print(f"Total meetings fetched: {len(all_meetings)}", flush=True)
    return all_meetings


# ─── Date Filtering ────────────────────────────────────────────────────────────

def parse_meeting_date_pacific(meeting):
    """
    Returns the Pacific-local date of a meeting, or None if unparseable.
    CRITICAL: A 4 PM PST meeting is stored as midnight UTC the NEXT day.
    Without UTC→Pacific conversion, late-afternoon meetings get the wrong date.
    """
    for field in ("starts_at", "activity_at", "date_start"):
        raw = meeting.get(field)
        if raw:
            try:
                ts = raw.replace("Z", "+00:00")
                dt_utc = datetime.fromisoformat(ts)
                if dt_utc.tzinfo is None:
                    dt_utc = dt_utc.replace(tzinfo=timezone.utc)
                return dt_utc.astimezone(PACIFIC).date()
            except (ValueError, TypeError):
                continue
    return None


def filter_mtd_meetings(meetings, today_pac):
    """Keep only meetings from the 1st of the current month through today (Pacific)."""
    month_start = date(today_pac.year, today_pac.month, 1)
    result = []
    for m in meetings:
        d = parse_meeting_date_pacific(m)
        if d and month_start <= d <= today_pac:
            m["_pac_date"] = d  # stash for day-of-month breakdown
            result.append(m)
    print(f"MTD meetings ({month_start} to {today_pac}): {len(result)}", flush=True)
    return result


# ─── Title Classification ──────────────────────────────────────────────────────

RE_EXCLUDE_FOLLOWUP = re.compile(
    r"follow[\s-]?up|fallow\s+up|f/u|next\s+steps|rescheduled?|reschedule",
    re.IGNORECASE
)
RE_EXCLUDE_ENROLLMENT = re.compile(
    r"enrollment|silver\s+start\s*up|bronze\s+enrollment|questions\s+on\s+enrollment",
    re.IGNORECASE
)

FIRST_CALL_PATTERNS = [
    re.compile(r"vending\s+strategy\s+call", re.IGNORECASE),
    re.compile(r"vendingpren[eu]+rs?\s+consultation", re.IGNORECASE),
    re.compile(r"vendingpren[eu]+rs?\s+strategy\s+call", re.IGNORECASE),
    re.compile(r"new\s+vendingpren[eu]+r\s+strategy\s+call", re.IGNORECASE),
    re.compile(r"vending\s+consult\b", re.IGNORECASE),
]


def classify_meeting(meeting, user_id_to_name):
    """
    Returns: (status, is_setter)
      status:    'qualifying' or 'excluded'
      is_setter: True if owned by Kristin/Spencer or Quick Discovery title

    Setter meetings ARE qualifying — they count toward funnel totals.
    The is_setter flag only drives the Sales/Setter sub-line breakdown.
    """
    title      = (meeting.get("title") or "").strip()
    user_id    = meeting.get("user_id", "")
    owner_name = user_id_to_name.get(user_id, "Unknown")

    # ── Hard user exclusion (meeting owner) ──────────────────────────────────
    if user_id in EXCLUDED_USER_IDS:
        return "excluded", False
    if owner_name == "Unknown" and user_id and user_id not in user_id_to_name:
        return "excluded", False

    # ── Step 1: Hard title excludes ──────────────────────────────────────────
    if title.startswith("Canceled:"):
        return "excluded", False
    if RE_EXCLUDE_FOLLOWUP.search(title):
        return "excluded", False
    if "Anthony" in title and "Q&A" in title:
        return "excluded", False
    if RE_EXCLUDE_ENROLLMENT.search(title):
        return "excluded", False

    # ── Step 2: Setter / Discovery — qualifying, flagged ─────────────────────
    if re.search(r"vending\s+quick\s+discovery", title, re.IGNORECASE):
        return "qualifying", True
    if owner_name in SETTER_OWNER_NAMES:
        return "qualifying", True

    # ── Step 3: First call qualifying titles ─────────────────────────────────
    if not title:  # Blank title = qualifying (GCal sync safety net)
        return "qualifying", False
    for pattern in FIRST_CALL_PATTERNS:
        if pattern.search(title):
            return "qualifying", False

    return "excluded", False


# ─── Lead Fetching & Exclusion ─────────────────────────────────────────────────

def fetch_lead(lead_id, lead_cache):
    """
    Fetch a single lead with minimal fields. Returns lead dict or None if excluded.
    Exclusion applied immediately — don't fetch-all-then-filter.
    """
    if lead_id in lead_cache:
        return lead_cache[lead_id]

    try:
        lead = close_get(f"lead/{lead_id}", {"_fields": LEAD_FIELDS})
    except Exception as e:
        print(f"  Warning: could not fetch lead {lead_id}: {e}", flush=True)
        lead_cache[lead_id] = None
        return None

    status_id = lead.get("status_id", "")
    owner_id  = get_custom_field(lead, CF_LEAD_OWNER) or ""

    if status_id in EXCLUDED_STATUS_IDS:
        lead_cache[lead_id] = None
        return None
    if owner_id in EXCLUDED_USER_IDS:
        lead_cache[lead_id] = None
        return None

    lead_cache[lead_id] = lead
    return lead


def get_funnel_name(lead):
    """Extract funnel name from lead custom field."""
    if lead is None:
        return "Unknown (Needs Review)"
    funnel = get_custom_field(lead, CF_FUNNEL_NAME)
    return funnel.strip() if funnel and funnel.strip() else "Unknown (Needs Review)"


# ─── Goals ────────────────────────────────────────────────────────────────────

def load_goals():
    goals_path = os.path.join(os.path.dirname(__file__), "..", "goals.json")
    try:
        with open(goals_path) as f:
            data = json.load(f)
        return data["goals"] if "goals" in data else data
    except FileNotFoundError:
        print("Warning: goals.json not found — using empty goals", flush=True)
        return {}
    except Exception as e:
        print(f"Warning: could not load goals.json: {e}", flush=True)
        return {}


# ─── Archive Helpers ───────────────────────────────────────────────────────────

def load_json_file(path):
    try:
        with open(path) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def save_json_file(path, data):
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, indent=2, default=str)


def update_archives(data_dict, today_pac, repo_root):
    archives_dir = os.path.join(repo_root, "archives")
    os.makedirs(archives_dir, exist_ok=True)

    week_key  = (today_pac - timedelta(days=today_pac.weekday())).strftime("%Y-%m-%d")
    month_key = today_pac.strftime("%Y-%m")

    save_json_file(os.path.join(archives_dir, f"data_week_{week_key}.json"), data_dict)
    save_json_file(os.path.join(archives_dir, f"data_month_{month_key}.json"), data_dict)
    print(f"  Saved weekly ({week_key}) and monthly ({month_key}) archives", flush=True)

    index_path = os.path.join(archives_dir, "index.json")
    index = load_json_file(index_path) or {"weeks": [], "months": []}

    if week_key  not in index["weeks"]:  index["weeks"].append(week_key);   index["weeks"].sort(reverse=True)
    if month_key not in index["months"]: index["months"].append(month_key); index["months"].sort(reverse=True)

    save_json_file(index_path, index)
    print("  Updated archive index", flush=True)


# ─── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60, flush=True)
    print("MTD Funnel Dashboard — fetch_data.py", flush=True)

    now_pac      = datetime.now(tz=PACIFIC)
    today_pac    = now_pac.date()
    tz_label     = now_pac.strftime("%Z")

    print(f"Run time: {now_pac.strftime('%Y-%m-%d %H:%M %Z')}", flush=True)
    print(f"MTD window: {today_pac.year}-{today_pac.month:02d}-01 to {today_pac}", flush=True)
    print("=" * 60, flush=True)

    # Step 1: Users
    user_id_to_name = fetch_users()

    # Step 2: All meetings
    all_meetings = fetch_all_meetings()

    # Step 3: MTD filter
    mtd_meetings = filter_mtd_meetings(all_meetings, today_pac)

    # Step 4: Classify titles
    print("Classifying meeting titles...", flush=True)
    qualifying_meetings = []
    excluded_meetings   = []

    for m in mtd_meetings:
        status, is_setter = classify_meeting(m, user_id_to_name)
        if status == "qualifying":
            m["_is_setter"] = is_setter
            qualifying_meetings.append(m)
        else:
            excluded_meetings.append(m)

    sales_count  = sum(1 for m in qualifying_meetings if not m["_is_setter"])
    setter_count = sum(1 for m in qualifying_meetings if m["_is_setter"])
    print(f"  Qualifying: {len(qualifying_meetings)} ({sales_count} sales / {setter_count} setter)", flush=True)
    print(f"  Excluded:   {len(excluded_meetings)}", flush=True)

    # Audit log: sample titles
    def sample_titles(meetings, label, n=5):
        titles = [m.get("title") or "(blank)" for m in meetings[:n]]
        if titles:
            print(f"\nSample {label} titles:", flush=True)
            for t in titles:
                print(f"  - {t}", flush=True)

    sample_titles([m for m in qualifying_meetings if not m["_is_setter"]], "SALES (first call)")
    sample_titles([m for m in qualifying_meetings if m["_is_setter"]], "SETTER")
    sample_titles(excluded_meetings, "EXCLUDED")
    print("", flush=True)

    # Step 5: Unique lead_ids
    unique_lead_ids = list({m.get("lead_id") for m in qualifying_meetings if m.get("lead_id")})
    print(f"Unique leads to fetch: {len(unique_lead_ids)}", flush=True)

    # Step 6: Fetch only those leads
    lead_cache = {}
    for i, lead_id in enumerate(unique_lead_ids, 1):
        if i % 25 == 0:
            print(f"  Fetched {i}/{len(unique_lead_ids)} leads...", flush=True)
        fetch_lead(lead_id, lead_cache)

    excluded_leads = sum(1 for v in lead_cache.values() if v is None)
    print(f"Lead fetch complete. Excluded leads: {excluded_leads}", flush=True)

    # ── Build funnel breakdown ─────────────────────────────────────────────────

    by_funnel     = {}
    by_funnel_day = {}

    for m in qualifying_meetings:
        lead_id   = m.get("lead_id")
        lead      = lead_cache.get(lead_id) if lead_id else None

        # Skip meetings whose lead was excluded at lead-level
        if lead_id and lead is None:
            continue

        funnel    = get_funnel_name(lead)
        is_setter = m["_is_setter"]

        if funnel not in by_funnel:
            by_funnel[funnel] = {"total": 0, "sales": 0, "setter": 0}
        by_funnel[funnel]["total"] += 1
        if is_setter:
            by_funnel[funnel]["setter"] += 1
        else:
            by_funnel[funnel]["sales"] += 1

        pac_date = m.get("_pac_date")
        if pac_date:
            day_str = str(pac_date.day)
            if funnel not in by_funnel_day:
                by_funnel_day[funnel] = {}
            by_funnel_day[funnel][day_str] = by_funnel_day[funnel].get(day_str, 0) + 1

    # Log funnel attribution
    print("\nFunnel attribution results:", flush=True)
    for f, v in sorted(by_funnel.items(), key=lambda x: -x[1]["total"]):
        print(f"  {f}: {v['total']} ({v['sales']} sales / {v['setter']} setter)", flush=True)
    print("", flush=True)

    # ── Summary stats ──────────────────────────────────────────────────────────

    goals        = load_goals()
    days_in_mo   = calendar.monthrange(today_pac.year, today_pac.month)[1]
    days_elapsed = today_pac.day

    total_goal     = sum(goals.values())
    mtd_booked     = sum(v["total"] for v in by_funnel.values())
    on_pace        = round(total_goal * days_elapsed / days_in_mo) if days_in_mo else 0
    remaining      = max(0, total_goal - mtd_booked)
    eom_projection = round(mtd_booked * days_in_mo / days_elapsed) if days_elapsed else 0

    print(f"Summary — MTD Booked: {mtd_booked} | On-Pace: {on_pace} | "
          f"Remaining: {remaining} | EOM Projection: {eom_projection}", flush=True)

    # ── Assemble data.json ─────────────────────────────────────────────────────

    data_dict = {
        "generated_at":  now_pac.isoformat(),
        "generated_tz":  tz_label,
        "month":         today_pac.strftime("%Y-%m"),
        "month_label":   now_pac.strftime("%B %Y"),
        "day":           today_pac.day,
        "month_days":    days_in_mo,
        "goals":         goals,
        "summary": {
            "mtd_booked":     mtd_booked,
            "on_pace":        on_pace,
            "remaining":      remaining,
            "eom_projection": eom_projection,
            "total_goal":     total_goal,
        },
        "by_funnel":     by_funnel,
        "by_funnel_day": by_funnel_day,
    }

    repo_root = os.path.join(os.path.dirname(__file__), "..")
    data_path = os.path.join(repo_root, "data.json")
    save_json_file(data_path, data_dict)
    print(f"Wrote {data_path}", flush=True)

    print("Updating archives...", flush=True)
    update_archives(data_dict, today_pac, repo_root)

    print("=" * 60, flush=True)
    print("Done.", flush=True)


if __name__ == "__main__":
    main()
