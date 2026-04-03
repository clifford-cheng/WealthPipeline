"""
Lead scoring, tagging, and territory-based allocation (curated deal flow — not a marketplace).
"""
from __future__ import annotations

import json
import math
import re
import sqlite3
from collections import defaultdict
from datetime import date, datetime
from typing import Any

from wealth_leads.config import (
    assign_exclude_issuer_risk_elevated,
    assign_exclude_issuer_risk_high,
    assign_exclude_visibility_tier,
    assign_max_filing_stale_days,
)
from wealth_leads.db import (
    connect,
    delete_assignments_for_cycle,
    get_allocation_settings,
    insert_lead_assignment,
    is_lead_suppressed,
    list_allocation_clients,
    list_assignments_for_cycle,
)
from wealth_leads.serve import _is_s1_form_type, _norm_person_name
from wealth_leads.territory import (
    exclusivity_key_for_lead,
    extract_territory_keys_from_hq,
    lead_matches_territory,
    parse_location_parts,
    territory_spec_to_match_rules,
)

_TAG_S1 = "S-1 Filed"
_TAG_PRE_IPO = "Pre-IPO / RSU Vesting"


def _filing_date_age_days(filing_date: object) -> int | None:
    s = str(filing_date or "").strip()[:10]
    if len(s) < 10:
        return None
    try:
        fd = datetime.strptime(s, "%Y-%m-%d").date()
        return (date.today() - fd).days
    except ValueError:
        return None


def _assignment_gate_skip_reason(conn: sqlite3.Connection, p: dict) -> str | None:
    """
    If this profile should not receive automated cycle assignment, return a short reason key;
    otherwise None.
    """
    ck = str(p.get("cik") or "").strip()
    pn = (p.get("norm_name") or "").strip()
    if not ck or not pn:
        return "incomplete"
    if is_lead_suppressed(conn, ck, pn):
        return "suppress"
    if assign_exclude_visibility_tier() and (p.get("lead_tier") or "") == "visibility":
        return "visibility"
    lvl = (p.get("issuer_risk_level") or "none").strip().lower()
    if assign_exclude_issuer_risk_high() and lvl == "high":
        return "issuer_risk_high"
    if assign_exclude_issuer_risk_elevated() and lvl == "elevated":
        return "issuer_risk_elevated"
    max_days = assign_max_filing_stale_days()
    if max_days > 0:
        age = _filing_date_age_days(p.get("filing_date"))
        if age is None or age > max_days:
            return "stale_filing"
    return None


def _safe_float(x: Any) -> float:
    try:
        return float(x) if x is not None else 0.0
    except (TypeError, ValueError):
        return 0.0


def seniority_tier(title: str) -> tuple[int, str]:
    t = (title or "").lower()
    if any(
        x in t
        for x in (
            "vice president",
            "vice-president",
            "assistant vice",
            "associate vice",
        )
    ) or re.search(r"\b(svp|evp|avp)\b", t):
        return 2, "senior"
    if any(
        x in t
        for x in (
            "chief executive",
            "chief financial",
            "chief operating",
            "chief technology",
            "chief legal",
            "ceo",
            "cfo",
            "coo",
            "cto",
            "president",
            "chair",
        )
    ):
        return 3, "executive"
    if "chief" in t:
        return 3, "executive"
    if any(x in t for x in ("general counsel", "treasurer", "secretary")):
        return 2, "senior"
    if "director" in t:
        return 1, "director"
    return 0, "other"


def liquidity_stage_for_profile(p: dict) -> str:
    ft = (p.get("filing_form_type") or "").upper()
    if _is_s1_form_type(ft) or p.get("has_s1_comp") or p.get("has_s1_officer"):
        return "S-1 filed (registration / pre-IPO disclosure)"
    if "10-K" in ft:
        return "Public company (10-K / ongoing disclosure)"
    return "Equity disclosure in registration filings"


def build_tags(p: dict) -> list[str]:
    tags: list[str] = []
    ft = (p.get("filing_form_type") or "").upper()
    if _is_s1_form_type(ft) or p.get("has_s1_comp") or p.get("has_s1_officer"):
        tags.append(_TAG_S1)
    eq = _safe_float(p.get("equity_hwm"))
    tot = _safe_float(p.get("total"))
    if eq >= 250_000 or tot >= 400_000:
        if _TAG_S1 in tags or _is_s1_form_type(ft):
            tags.append(_TAG_PRE_IPO)
        elif "10-K" not in ft:
            tags.append(_TAG_PRE_IPO)
    return list(dict.fromkeys(tags))


def score_profile(p: dict, tags: list[str]) -> float:
    eq = _safe_float(p.get("equity_hwm"))
    tot = _safe_float(p.get("total"))
    base = math.log10(eq + 1) * 22 + math.log10(tot + 1) * 6
    tier, _ = seniority_tier(p.get("title") or "")
    base += {0: 0, 1: 10, 2: 18, 3: 28}[tier]
    if _TAG_S1 in tags:
        base += 35
    if _TAG_PRE_IPO in tags:
        base += 18
    return round(base, 4)


def why_this_lead_matters(p: dict, tags: list[str], liq: str) -> str:
    parts = []
    nm = (p.get("display_name") or "").strip()
    co = (p.get("company_name") or "").strip()
    tier_name = seniority_tier(p.get("title") or "")[1]
    eq = _safe_float(p.get("equity_hwm"))
    if _TAG_S1 in tags:
        parts.append(
            f"{nm or 'Executive'} appears in {co or 'issuer'} S-1-related compensation disclosure — "
            "registration activity often coincides with liquidity planning windows."
        )
    elif eq > 0:
        parts.append(
            f"Significant disclosed equity grant value (~${eq:,.0f} stock/options in a single FY) "
            f"for a {tier_name}-level role at {co or 'the issuer'}."
        )
    else:
        parts.append(
            f"Named executive / officer disclosure for {co or 'issuer'} — review filing for role and context."
        )
    parts.append(f"Liquidity context: {liq}")
    return " ".join(parts)


def suggested_outreach_angle(tags: list[str], p: dict) -> str:
    if _TAG_PRE_IPO in tags and _TAG_S1 in tags:
        return "Pre-IPO liquidity planning, 83(b) / RSU timing, concentrated single-stock risk."
    if _TAG_S1 in tags:
        return "Registration-track timing, executive compensation visibility, diversification ahead of float."
    if _safe_float(p.get("equity_hwm")) > 500_000:
        return "High disclosed equity grants — tax-aware diversification and exercise timing."
    return "Executive wealth transition — discovery call framed around public filing milestones and goals."


def enrich_profile(p: dict) -> dict[str, Any]:
    hq = (p.get("issuer_headquarters") or "").strip()
    lead_keys = extract_territory_keys_from_hq(hq)
    loc = parse_location_parts(hq)
    tags = build_tags(p)
    liq = liquidity_stage_for_profile(p)
    score = score_profile(p, tags)
    why = why_this_lead_matters(p, tags, liq)
    outreach = suggested_outreach_angle(tags, p)
    excl = exclusivity_key_for_lead(lead_keys) or f"US-NA-{p.get('cik') or '0'}"
    return {
        "profile": p,
        "cik": str(p.get("cik") or "").strip(),
        "person_norm": p.get("norm_name") or _norm_person_name(p.get("display_name") or ""),
        "hq": hq,
        "lead_keys": lead_keys,
        "exclusivity_key": excl,
        "score": score,
        "tags": tags,
        "liquidity_stage": liq,
        "why_summary": why,
        "outreach_angle": outreach,
        "location": loc,
        "email_guess": "",
        "email_confidence": 0.0,
        "seniority_label": seniority_tier(p.get("title") or "")[1],
    }


def assign_for_cycle(
    conn: sqlite3.Connection,
    *,
    cycle_yyyymm: str,
    profiles_all: list[dict],
    replace: bool = True,
) -> dict[str, Any]:
    """
    Greedy fair allocation: highest scores first. Each (CIK, person) at most once per cycle
    globally unless allow_shared_leads_default is enabled on settings.
    """
    settings = get_allocation_settings(conn)
    allow_shared = bool(int(settings["allow_shared_leads_default"] or 0))
    clients = list_allocation_clients(conn)
    active = [
        c
        for c in clients
        if int(c["monthly_lead_quota"] or 0) > 0
        and (c.get("territory_spec") or "").strip()
    ]
    rules_by_uid: dict[int, dict] = {}
    quota_by_uid: dict[int, int] = {}
    for c in active:
        uid = int(c["id"])
        rules_by_uid[uid] = territory_spec_to_match_rules(
            str(c.get("territory_type") or "state"),
            str(c.get("territory_spec") or ""),
        )
        quota_by_uid[uid] = int(c["monthly_lead_quota"] or 0)

    globally_assigned: set[tuple[str, str]] = set()
    if replace:
        delete_assignments_for_cycle(conn, cycle_yyyymm)
    else:
        for row in list_assignments_for_cycle(conn, cycle_yyyymm):
            globally_assigned.add(
                (str(row["cik"]), str(row["person_norm"] or ""))
            )

    assigned_count: dict[int, int] = defaultdict(int)
    gate_counts: dict[str, int] = defaultdict(int)
    eligible_profiles: list[dict] = []
    for p in profiles_all:
        gr = _assignment_gate_skip_reason(conn, p)
        if gr:
            gate_counts[gr] += 1
            continue
        eligible_profiles.append(p)

    enriched: list[dict[str, Any]] = []
    for p in eligible_profiles:
        e = enrich_profile(p)
        if not e["cik"] or not e["person_norm"]:
            continue
        enriched.append(e)
    enriched.sort(key=lambda x: x["score"], reverse=True)

    stats: dict[str, Any] = {
        "cycle": cycle_yyyymm,
        "candidates": len(enriched),
        "assigned": 0,
        "skipped_no_eligible": 0,
        "skipped_exclusive": 0,
        "skipped_assignment_gate": int(sum(gate_counts.values())),
        "assignment_gate_breakdown": dict(gate_counts),
    }

    for e in enriched:
        cik, pn = e["cik"], e["person_norm"]
        pair = (cik, pn)
        if not allow_shared and pair in globally_assigned:
            stats["skipped_exclusive"] += 1
            continue

        hq = e["hq"]
        lead_keys = e["lead_keys"]
        eligible_uids: list[int] = []
        for c in active:
            uid = int(c["id"])
            if assigned_count[uid] >= quota_by_uid[uid]:
                continue
            if int(c.get("premium_s1_only") or 0) and _TAG_S1 not in e["tags"]:
                continue
            rules = rules_by_uid[uid]
            if rules["type"] != "metro" and not lead_keys:
                continue
            if not lead_matches_territory(hq, lead_keys, rules):
                continue
            eligible_uids.append(uid)

        if not eligible_uids:
            stats["skipped_no_eligible"] += 1
            continue

        eligible_uids.sort(
            key=lambda uid: (
                assigned_count[uid] / max(quota_by_uid[uid], 1),
                assigned_count[uid],
                uid,
            )
        )
        targets = eligible_uids if allow_shared else eligible_uids[:1]
        for picked in targets:
            if assigned_count[picked] >= quota_by_uid[picked]:
                continue
            insert_lead_assignment(
                conn,
                user_id=picked,
                cik=cik,
                person_norm=pn,
                cycle_yyyymm=cycle_yyyymm,
                territory_key=e["exclusivity_key"],
                score=e["score"],
                tags=e["tags"],
                liquidity_stage=e["liquidity_stage"],
                why_summary=e["why_summary"],
                outreach_angle=e["outreach_angle"],
                email_guess=e["email_guess"],
                email_confidence=e["email_confidence"],
                snapshot=e,
            )
            assigned_count[picked] += 1
            stats["assigned"] += 1
        globally_assigned.add(pair)

    return stats


def run_allocation_from_db(
    *,
    cycle_yyyymm: str | None = None,
    replace: bool = True,
) -> dict[str, Any]:
    from wealth_leads.serve import _build_profiles, _lead_desk_filter_profiles

    if cycle_yyyymm is None:
        cycle_yyyymm = __import__("datetime").datetime.now().strftime("%Y%m")
    with connect() as conn:
        profiles_all = _build_profiles(conn)
        candidates = _lead_desk_filter_profiles(profiles_all, conn)
        return assign_for_cycle(
            conn, cycle_yyyymm=cycle_yyyymm, profiles_all=candidates, replace=replace
        )


def assignments_to_display_rows(
    conn: sqlite3.Connection,
    *,
    user_id: int,
    cycle_yyyymm: str,
    tag_filter: str = "",
) -> list[dict[str, Any]]:
    """Hydrate assignments with live profile when possible."""
    from wealth_leads.serve import _build_profiles, _find_profile

    rows = list_assignments_for_cycle(conn, cycle_yyyymm, user_id=user_id)
    profiles_all = _build_profiles(conn)
    out: list[dict[str, Any]] = []
    tf = (tag_filter or "").strip().lower()
    for r in rows:
        tags = json.loads(r["tags_json"] or "[]")
        if tf == "s1" and _TAG_S1 not in tags:
            continue
        if tf == "rsu" and _TAG_PRE_IPO not in tags:
            continue
        cik = str(r["cik"])
        pn = str(r["person_norm"] or "")
        prof = _find_profile(profiles_all, cik, pn)
        snap = json.loads(r["profile_snapshot_json"] or "{}")
        out.append(
            {
                "assignment_id": int(r["id"]),
                "cik": cik,
                "person_norm": pn,
                "score": float(r["score"] or 0),
                "tags": tags,
                "liquidity_stage": r["liquidity_stage"] or "",
                "why_summary": r["why_summary"] or "",
                "outreach_angle": r["outreach_angle"] or "",
                "email_guess": r["email_guess"] or "",
                "email_confidence": r["email_confidence"],
                "territory_key": r["territory_key"] or "",
                "assigned_at": r["assigned_at"] or "",
                "profile": prof,
                "snapshot": snap,
            }
        )
    return out
