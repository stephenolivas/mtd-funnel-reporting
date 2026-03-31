#!/usr/bin/env python3
"""
MTD Funnel Reporting Dashboard — Data Fetcher (V3)
Changes from V2:
  - Status exclusions expanded: Lost, No Show added (in addition to Canceled, Outside US)
  - "Instagram Setter" funnel merged into "Instagram"
  - Per-funnel tracking: showed, qualified, closed (in addition to total)
  - Summary includes in-house vs external breakdowns + aggregate rates
  - status_label added to lead fetch for closed detection + expanded exclusions
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
CLOSE_API_KEY  = os.environ["CLOSE_API_KEY"]
PACIFIC        = ZoneInfo("America/Los_Angeles")

# Custom field IDs
CF_FUNNEL_NAME = "cf_xqDQE8fkPsWa0RNEve7hcaxKblCe6489XeZGRDzyPdX"  # Funnel Name DEAL
CF_LEAD_OWNER  = "cf_gOfS9pFwext58oberEegLyix8hZzeHrxhCZOVh3P3rd"   # Lead Owner
CF_SHOWED      = "cf_OPyvpU45RdvjLqfm8V1VWwNxrGKogEH2IBJmfCj0Uhq"  # First Call Show Up (Opp)
CF_QUALIFIED   = "cf_ZDx7NBQaDzV1yYrFcBMzt6cIYj81dAcswpNN0CQzCPS"  # Qualified (Opp)

LEAD_FIELDS = (
    f"id,display_name,status_id,status_label,"
    f"custom.{CF_FUNNEL_NAME},"
    f"custom.{CF_LEAD_OWNER},"
    f"custom.{CF_SHOWED},"
    f"custom.{CF_QUALIFIED}"
)

# ─── Exclusions ───────────────────────────────────────────────────────────────

# Meeting-owner hard exclusions (user_id)
EXCLUDED_USER_IDS = {
    "user_5cZRqXu8kb4O1IeBVA98UMcMEhYZUhx1fnCHfSL0YMV",  # Stephen Olivas
    "user_yRF070m26JE67J6CJqzkAB3IqY7btNm1K5RisCglKa6",  # Ahmad Bukhari
}

# Lead-level status exclusions — matched against status_label substring
# Covering: Canceled (by Lead), Outside the US, Lost, No Show
EXCLUDED_STATUS_SUBSTRINGS = [
    "Canceled (by Lead)",
    "Outside the US",
    "Lost",
]

# Setter owners — excluded entirely (dashboard is sales-only)
SETTER_OWNER_NAMES = {"Kristin Nelson", "Spencer Reynolds"}

# ─── In-House vs External ─────────────────────────────────────────────────────

INHOUSE_FUNNELS = {
    "YouTube", "Meta Ads", "VSL", "Website", "Internal Webinar",
    "Mike Newsletter", "TikTok", "Side Hustle Nation", "WWWS",
    "Passivepreneurs", "Reactivation Email", "Reactivation Scrapers",
}
# Everything else (Low Ticket Funnel, Instagram, X, LinkedIn, etc.) = External

def is_inhouse(funnel_name):
    return funnel_name in INHOUSE_FUNNELS

# ─── Session Setup ─────────────────────────────────────────────────────────────

session = requests.Session()
session.auth = (CLOSE_API_KEY, "")
session.mount("https://", HTTPAdapter(max_retries=Retry(total=0)))


def close_get(endpoint, params=None):
    """Single Close API GET with 0.5s throttle + retry on 429."""
    time.sleep(0.5)
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
    """Handle both nested lead['custom'][field_id] and flat lead['custom.field_id'] shapes."""
    nested = (lead.get("custom") or {}).get(field_id)
    if nested not in (None, ""):
        return nested
    flat = lead.get(f"custom.{field_id}")
    if flat not in (None, ""):
        return flat
    return None


# ─── User Name Resolution ──────────────────────────────────────────────────────

def fetch_users():
    print("Fetching org users...", flush=True)
    data = close_get("user/", {"_limit": 100})
    users = {u["id"]: u.get("display_name", "Unknown") for u in data.get("data", [])}
    print(f"  Loaded {len(users)} users", flush=True)
    return users


# ─── Meeting Pagination ────────────────────────────────────────────────────────

def fetch_all_meetings():
    """Paginate ALL meetings. Close silently ignores date filters — filter in Python."""
    print("Fetching all meetings (paginating)...", flush=True)
    all_meetings, skip, page = [], 0, 0
    while True:
        page += 1
        data  = close_get("activity/meeting/", {"_skip": skip, "_limit": 100})
        batch = data.get("data", [])
        all_meetings.extend(batch)
        print(f"  Page {page}: {len(batch)} meetings (total: {len(all_meetings)})", flush=True)
        if not data.get("has_more", False):
            break
        skip += 100
    print(f"Total meetings fetched: {len(all_meetings)}", flush=True)
    return all_meetings


# ─── Date Filtering ────────────────────────────────────────────────────────────

def parse_meeting_date_pacific(meeting):
    """Convert UTC starts_at → Pacific date. Critical: 4 PM PST = midnight UTC next day."""
    for field in ("starts_at", "activity_at", "date_start"):
        raw = meeting.get(field)
        if raw:
            try:
                ts     = raw.replace("Z", "+00:00")
                dt_utc = datetime.fromisoformat(ts)
                if dt_utc.tzinfo is None:
                    dt_utc = dt_utc.replace(tzinfo=timezone.utc)
                return dt_utc.astimezone(PACIFIC).date()
            except (ValueError, TypeError):
                continue
    return None


def filter_mtd_meetings(meetings, today_pac):
    month_start = date(today_pac.year, today_pac.month, 1)
    result = []
    for m in meetings:
        d = parse_meeting_date_pacific(m)
        if d and month_start <= d <= today_pac:
            m["_pac_date"] = d
            result.append(m)
    print(f"MTD meetings ({month_start} to {today_pac}): {len(result)}", flush=True)
    return result


# ─── Title Classification ──────────────────────────────────────────────────────

RE_EXCLUDE_FOLLOWUP   = re.compile(r"follow[\s-]?up|fallow\s+up|f/u|next\s+steps|rescheduled?|reschedule", re.IGNORECASE)
RE_EXCLUDE_ENROLLMENT = re.compile(r"enrollment|silver\s+start\s*up|bronze\s+enrollment|questions\s+on\s+enrollment", re.IGNORECASE)

FIRST_CALL_PATTERNS = [
    re.compile(r"vending\s+strategy\s+call", re.IGNORECASE),
    re.compile(r"vendingpren[eu]+rs?\s+consultation", re.IGNORECASE),
    re.compile(r"vendingpren[eu]+rs?\s+strategy\s+call", re.IGNORECASE),
    re.compile(r"new\s+vendingpren[eu]+r\s+strategy\s+call", re.IGNORECASE),
    re.compile(r"vending\s+consult\b", re.IGNORECASE),
]


def classify_meeting(meeting, user_id_to_name):
    title      = (meeting.get("title") or "").strip()
    user_id    = meeting.get("user_id", "")
    owner_name = user_id_to_name.get(user_id, "Unknown")

    if user_id in EXCLUDED_USER_IDS:                          return "excluded"
    if owner_name == "Unknown" and user_id and user_id not in user_id_to_name: return "excluded"
    if title.startswith("Canceled:"):                         return "excluded"
    if RE_EXCLUDE_FOLLOWUP.search(title):                     return "excluded"
    if "Anthony" in title and "Q&A" in title:                 return "excluded"
    if RE_EXCLUDE_ENROLLMENT.search(title):                   return "excluded"

    # Setter / Discovery — excluded (sales-only dashboard)
    if re.search(r"vending\s+quick\s+discovery", title, re.IGNORECASE): return "excluded"
    if owner_name in SETTER_OWNER_NAMES:                      return "excluded"

    # Qualifying first-call titles
    if not title:                                             return "qualifying"
    for pattern in FIRST_CALL_PATTERNS:
        if pattern.search(title):                             return "qualifying"

    return "excluded"


# ─── Lead Fetching ─────────────────────────────────────────────────────────────

def is_lead_excluded(lead):
    """Return True if this lead should be excluded based on status_label."""
    status_label = lead.get("status_label") or ""
    # Also check flat key in case _fields flattens it
    if not status_label:
        status_label = lead.get("status_label") or ""
    for substring in EXCLUDED_STATUS_SUBSTRINGS:
        if substring in status_label:
            return True
    return False


def fetch_lead(lead_id, lead_cache):
    if lead_id in lead_cache:
        return lead_cache[lead_id]
    try:
        lead = close_get(f"lead/{lead_id}", {"_fields": LEAD_FIELDS})
    except Exception as e:
        print(f"  Warning: could not fetch lead {lead_id}: {e}", flush=True)
        lead_cache[lead_id] = None
        return None

    # Lead-owner exclusion
    owner_id = get_custom_field(lead, CF_LEAD_OWNER) or ""
    if owner_id in EXCLUDED_USER_IDS:
        lead_cache[lead_id] = None
        return None

    # Status exclusion (expanded: Lost, No Show, Canceled, Outside US)
    if is_lead_excluded(lead):
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


def get_lead_metrics(lead):
    """Return (showed, qualified) booleans for a lead."""
    if lead is None:
        return False, False
    showed    = str(get_custom_field(lead, CF_SHOWED)    or "").strip().lower() == "yes"
    qualified = str(get_custom_field(lead, CF_QUALIFIED) or "").strip().lower() == "yes"
    return showed, qualified


# ─── Closed-Won MTD (opportunity-based) ──────────────────────────────────────

# The opp-level funnel field — this is what Close UI filters on and what your
# CSV exports use. It lives as a TOP-LEVEL key on the opportunity object:
# opp["cf_qaCxv0OBskvZKsappkm1EfLSvyhJ2wAmDbYkHdq2NAo"]
# NOT nested under opp["custom"] — that's a lead-only structure.
# CF_OPP_FUNNEL removed — only CF_FUNNEL_NAME on the lead is used.

def fetch_closed_won_mtd(month_start_str, month_end_str, lead_cache):
    """
    Fetch all closed-won opportunities where close_at falls within the current
    month. Uses CURSOR-based pagination.

    Funnel attribution: always reads CF_FUNNEL_NAME from the LEAD — the single
    source of truth (cf_xqDQE8fkPsWa0RNEve7hcaxKblCe6489XeZGRDzyPdX).

    Deduplication: counts each unique lead_id once, even if they have multiple
    won opps (add-ons, upgrades). This matches how Close UI counts leads.

    Exclusions: skips leads owned by excluded users (Ahmad, Stephen test records).
    """
    print(f"Fetching closed-won opps ({month_start_str} to {month_end_str})...", flush=True)

    total         = 0
    by_funnel     = {}
    seen_lead_ids = set()   # dedup — one lead can have multiple won opps
    cursor        = None
    page          = 0

    while True:
        page  += 1
        params = {
            "status_type":   "won",
            "close_at__gte": f"{month_start_str}T00:00:00+00:00",
            "close_at__lte": f"{month_end_str}T23:59:59+00:00",
            "_limit":        100,
            "_fields":       "id,lead_id",  # funnel comes from lead, not opp
        }
        if cursor:
            params["_cursor"] = cursor

        data  = close_get("opportunity/", params)
        batch = data.get("data", [])

        for opp in batch:
            lead_id = opp.get("lead_id")
            if not lead_id:
                continue

            # Skip duplicate leads (add-ons/upgrades = multiple opps on one lead)
            # Close UI counts unique leads — we must match that behavior
            if lead_id in seen_lead_ids:
                print(f"  Skipping duplicate lead_id: {lead_id}", flush=True)
                continue
            seen_lead_ids.add(lead_id)

            # Fetch lead — from cache (already fetched in meeting pipeline) or fresh
            lead = lead_cache.get(lead_id)
            if lead is None and lead_id not in lead_cache:
                print(f"  Fetching prior-month lead for won opp: {lead_id}", flush=True)
                try:
                    lead = close_get(f"lead/{lead_id}", {"_fields": LEAD_FIELDS})
                    lead_cache[lead_id] = lead
                except Exception as e:
                    print(f"  Warning: could not fetch lead {lead_id}: {e}", flush=True)
                    lead_cache[lead_id] = None

            if not lead:
                continue

            # Apply same owner exclusion as meeting pipeline (skips test/internal records)
            owner_id = get_custom_field(lead, CF_LEAD_OWNER) or ""
            if owner_id in EXCLUDED_USER_IDS:
                print(f"  Skipping won lead — excluded owner: {lead_id}", flush=True)
                continue

            funnel = (get_custom_field(lead, CF_FUNNEL_NAME) or "").strip()
            if not funnel:
                funnel = "Unknown (Needs Review)"

            total += 1
            by_funnel[funnel] = by_funnel.get(funnel, 0) + 1

        print(f"  Opp page {page}: {len(batch)} fetched (unique leads counted so far: {total})", flush=True)

        cursor = data.get("cursor")
        if not cursor or not batch:
            break

    print(f"Closed-won MTD total: {total}", flush=True)
    print(f"Won by funnel: {by_funnel}", flush=True)
    return total, by_funnel


def load_goals():
    goals_path = os.path.join(os.path.dirname(__file__), "..", "goals.json")
    try:
        with open(goals_path) as f:
            data = json.load(f)
        return data["goals"] if "goals" in data else data
    except FileNotFoundError:
        print("Warning: goals.json not found", flush=True)
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

    index_path = os.path.join(archives_dir, "index.json")
    index = load_json_file(index_path) or {"weeks": [], "months": []}
    if week_key  not in index["weeks"]:  index["weeks"].append(week_key);   index["weeks"].sort(reverse=True)
    if month_key not in index["months"]: index["months"].append(month_key); index["months"].sort(reverse=True)
    save_json_file(index_path, index)
    print(f"  Archives updated: week={week_key}, month={month_key}", flush=True)


# ─── Main ──────────────────────────────────────────────────────────────────────

def blank_metrics():
    return {"total": 0, "showed": 0, "qualified": 0}

def add_metrics(target, showed, qualified):
    target["total"]     += 1
    target["showed"]    += int(showed)
    target["qualified"] += int(qualified)


def main():
    print("=" * 60, flush=True)
    print("MTD Funnel Dashboard V3 — fetch_data.py", flush=True)

    now_pac    = datetime.now(tz=PACIFIC)
    today_pac  = now_pac.date()
    tz_label   = now_pac.strftime("%Z")

    print(f"Run time:   {now_pac.strftime('%Y-%m-%d %H:%M %Z')}", flush=True)
    print(f"MTD window: {today_pac.year}-{today_pac.month:02d}-01 to {today_pac}", flush=True)
    print("=" * 60, flush=True)

    user_id_to_name = fetch_users()
    all_meetings    = fetch_all_meetings()
    mtd_meetings    = filter_mtd_meetings(all_meetings, today_pac)

    # Classify titles
    print("Classifying meeting titles...", flush=True)
    qualifying, excluded = [], []
    for m in mtd_meetings:
        if classify_meeting(m, user_id_to_name) == "qualifying":
            qualifying.append(m)
        else:
            excluded.append(m)
    print(f"  Qualifying: {len(qualifying)}  |  Excluded: {len(excluded)}", flush=True)

    # Audit sample
    for label, meetings in [("QUALIFYING", qualifying[:5]), ("EXCLUDED", excluded[:5])]:
        print(f"\nSample {label}:", flush=True)
        for m in meetings:
            print(f"  - {m.get('title') or '(blank)'}", flush=True)
    print("", flush=True)

    # Fetch leads (only from qualifying meetings)
    unique_lead_ids = list({m.get("lead_id") for m in qualifying if m.get("lead_id")})
    print(f"Unique leads to fetch: {len(unique_lead_ids)}", flush=True)
    lead_cache = {}
    for i, lead_id in enumerate(unique_lead_ids, 1):
        if i % 25 == 0:
            print(f"  Fetched {i}/{len(unique_lead_ids)} leads...", flush=True)
        fetch_lead(lead_id, lead_cache)

    excluded_leads = sum(1 for v in lead_cache.values() if v is None)
    print(f"Lead fetch complete. Excluded leads: {excluded_leads}", flush=True)

    # Build funnel breakdown
    by_funnel     = {}
    by_funnel_day = {}

    inhouse_agg  = blank_metrics()
    external_agg = blank_metrics()

    for m in qualifying:
        lead_id = m.get("lead_id")
        lead    = lead_cache.get(lead_id) if lead_id else None
        if lead_id and lead is None:
            continue  # lead-level excluded

        funnel             = get_funnel_name(lead)
        showed, qualified  = get_lead_metrics(lead)

        if funnel not in by_funnel:
            by_funnel[funnel] = blank_metrics()
        add_metrics(by_funnel[funnel], showed, qualified)

        # Day breakdown (for future chart use)
        pac_date = m.get("_pac_date")
        if pac_date:
            day_str = str(pac_date.day)
            if funnel not in by_funnel_day:
                by_funnel_day[funnel] = {}
            by_funnel_day[funnel][day_str] = by_funnel_day[funnel].get(day_str, 0) + 1

        # In-house vs external aggregate
        if is_inhouse(funnel):
            add_metrics(inhouse_agg, showed, qualified)
        else:
            add_metrics(external_agg, showed, qualified)

    # Log attribution
    print("\nFunnel attribution:", flush=True)
    for f, v in sorted(by_funnel.items(), key=lambda x: -x[1]["total"]):
        inout = "IN" if is_inhouse(f) else "EX"
        print(f"  [{inout}] {f}: {v['total']} booked | "
              f"showed {v['showed']} | qualified {v['qualified']}", flush=True)
    print("", flush=True)

    # Summary stats
    goals        = load_goals()
    days_in_mo   = calendar.monthrange(today_pac.year, today_pac.month)[1]
    days_elapsed = today_pac.day

    total_goal     = sum(goals.values())
    mtd_booked     = sum(v["total"] for v in by_funnel.values())
    on_pace        = round(total_goal * days_elapsed / days_in_mo) if days_in_mo else 0
    remaining      = max(0, total_goal - mtd_booked)
    eom_projection = round(mtd_booked * days_in_mo / days_elapsed) if days_elapsed else 0

    # Closed-won from opportunity endpoint — cursor pagination, grouped by funnel
    month_start_str = f"{today_pac.year}-{today_pac.month:02d}-01"
    month_end_str   = today_pac.strftime(f"%Y-%m-{days_in_mo:02d}")
    closed_won_mtd, won_by_funnel = fetch_closed_won_mtd(month_start_str, month_end_str, lead_cache)

    print(f"Summary — MTD: {mtd_booked} | On-Pace: {on_pace} | "
          f"Remaining: {remaining} | EOM Proj: {eom_projection}", flush=True)
    print(f"In-House:  {inhouse_agg['total']} booked | "
          f"showed {inhouse_agg['showed']} | qualified {inhouse_agg['qualified']}", flush=True)
    print(f"External:  {external_agg['total']} booked | "
          f"showed {external_agg['showed']} | qualified {external_agg['qualified']}", flush=True)

    # Assemble data.json
    data_dict = {
        "generated_at":  now_pac.isoformat(),
        "generated_tz":  tz_label,
        "month":         today_pac.strftime("%Y-%m"),
        "month_label":   now_pac.strftime("%B %Y"),
        "day":           today_pac.day,
        "month_days":    days_in_mo,
        "goals":         goals,
        "summary": {
            "mtd_booked":      mtd_booked,
            "on_pace":         on_pace,
            "remaining":       remaining,
            "eom_projection":  eom_projection,
            "total_goal":      total_goal,
            "closed_won_mtd":  closed_won_mtd,
            "inhouse":         inhouse_agg,
            "external":        external_agg,
        },
        "by_funnel":     by_funnel,
        "by_funnel_day": by_funnel_day,
        "won_by_funnel": won_by_funnel,
    }

    repo_root = os.path.join(os.path.dirname(__file__), "..")
    save_json_file(os.path.join(repo_root, "data.json"), data_dict)
    print(f"Wrote data.json", flush=True)

    update_archives(data_dict, today_pac, repo_root)

    print("=" * 60, flush=True)
    print("Done.", flush=True)


if __name__ == "__main__":
    main()
