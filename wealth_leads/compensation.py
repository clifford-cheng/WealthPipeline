from __future__ import annotations

import re
from dataclasses import dataclass
from functools import lru_cache
from typing import Any, Optional

import requests
from bs4 import BeautifulSoup

from wealth_leads.person_quality import (
    is_acceptable_lead_person_name,
    refine_lead_person_name,
)

"""Bump when summary-compensation extraction logic changes (triggers DB re-backfill)."""
NEO_COMP_PARSE_REVISION = 2

_YEAR_RE = re.compile(r"^20[12]\d$")
_MONEY_CLEAN = re.compile(r"[\$,]")
_DASH = frozenset({"—", "–", "-", "―", "\u2014", "\u2013", "\u00a0"})


def _cell_text(td) -> str:
    return " ".join(td.stripped_strings)


def _parse_money(s: str) -> Optional[float]:
    t = s.strip()
    if not t or t in _DASH or t.upper() in ("N/A", "NA", "*"):
        return None
    t = _MONEY_CLEAN.sub("", t)
    t = re.sub(r"\(\d+\)$", "", t).strip()
    if not t or t in _DASH:
        return None
    try:
        return float(t)
    except ValueError:
        return None


def _is_dash_cell(s: str) -> bool:
    t = s.strip()
    if not t or t in _DASH or t == "\u2014":
        return True
    if "\ufffd" in t:
        return True
    if len(t) <= 2 and not any(c.isdigit() for c in t):
        return True
    return False


def _clean_person_name(raw: str) -> str:
    s = re.sub(r"<[^>]+>", "", raw)
    s = s.replace("\xa0", " ")
    s = re.sub(r"[\u200b\u200c\u200d\ufeff]+", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    s = re.sub(r"\s*\(\d+\)(?:\s*,\s*\(\d+\))*", "", s)
    s = re.sub(r"\s*\(\d+\)\s*$", "", s)
    return s.strip()


def _row_cells(tr) -> list[str]:
    return [_cell_text(td) for td in tr.find_all("td", recursive=False)]


def _looks_like_summary_comp_header(tr) -> bool:
    blob = " ".join(_row_cells(tr)).lower()
    if "name" not in blob:
        return False
    if "year" not in blob and "fiscal" not in blob:
        return False
    if "salary" not in blob and "stock" not in blob:
        return False
    return True


def _looks_like_summary_comp_two_row(tr0, tr1) -> bool:
    blob = " ".join(_row_cells(tr0) + _row_cells(tr1)).lower()
    if "name" not in blob and "principal" not in blob:
        return False
    if "year" not in blob and "fiscal" not in blob:
        return False
    if "salary" not in blob and "stock" not in blob:
        return False
    return True


def _looks_like_role_line(first_nonempty: str) -> bool:
    t = first_nonempty.lower()
    keys = (
        "officer",
        "chief",
        "president",
        "director",
        "secretary",
        "treasurer",
        "chairman",
        "chairperson",
        "executive",
        "vice president",
        "cfo",
        "ceo",
        "coo",
        "general counsel",
    )
    return any(k in t for k in keys)


@dataclass(frozen=True)
class NeoCompRow:
    person_name: str
    role_hint: Optional[str]
    fiscal_year: int
    salary: Optional[float]
    bonus: Optional[float]
    stock_awards: Optional[float]
    option_awards: Optional[float]
    non_equity_incentive: Optional[float]
    pension_change: Optional[float]
    other_comp: Optional[float]
    total: Optional[float]


def _squash_spacer_moneys(vals: list[Optional[float]]) -> list[Optional[float]]:
    """
    Some S-1 finTables insert extra spacer columns (dashes) between
    Salary / Bonus / Stock / Other / Total so we get 7 slots with Nones
    at odd indices. Collapse to 5 values when that pattern matches.
    """
    if (
        len(vals) == 7
        and vals[1] is None
        and vals[3] is None
        and vals[5] is None
    ):
        return [vals[0], vals[1], vals[2], vals[4], vals[6]]
    return vals


def _assign_money_slots(vals: list[Optional[float]]) -> tuple[
    Optional[float],
    Optional[float],
    Optional[float],
    Optional[float],
    Optional[float],
    Optional[float],
    Optional[float],
    Optional[float],
]:
    """
    Map money columns to fixed indices:
    0 salary, 1 bonus, 2 stock, 3 option, 4 non-equity incentive,
    5 pension change, 6 other, 7 total
    """
    n = len(vals)
    z = [None] * 8
    if n == 0:
        return tuple(z)  # type: ignore
    if n == 1:
        z[7] = vals[0]
    elif n == 2:
        z[0], z[7] = vals[0], vals[1]
    elif n == 3:
        z[0], z[2], z[7] = vals[0], vals[1], vals[2]
    elif n == 4:
        z[0], z[1], z[2], z[7] = vals[0], vals[1], vals[2], vals[3]
    elif n == 5:
        z[0], z[1], z[2], z[6], z[7] = vals[0], vals[1], vals[2], vals[3], vals[4]
    elif n == 6:
        z[0], z[1], z[2], z[3], z[6], z[7] = (
            vals[0],
            vals[1],
            vals[2],
            vals[3],
            vals[4],
            vals[5],
        )
    elif n == 7:
        z[0], z[1], z[2], z[3], z[4], z[6], z[7] = (
            vals[0],
            vals[1],
            vals[2],
            vals[3],
            vals[4],
            vals[5],
            vals[6],
        )
    else:
        z[0], z[1], z[2], z[3], z[4], z[5], z[6], z[7] = (
            vals[0],
            vals[1],
            vals[2],
            vals[3],
            vals[4],
            vals[5],
            vals[6],
            vals[7],
        )
    return tuple(z)  # type: ignore


def _classify_comp_header_label(label: str) -> Optional[str]:
    """
    Map one header cell (possibly with footnote markers) to a NeoCompRow field or
    pseudo-field ``_name`` / ``_year`` for column alignment.
    """
    s = label.replace("\xa0", " ").strip().lower()
    if not s:
        return None
    if "name" in s and "principal" in s:
        return "_name"
    if s == "year" or s.startswith("year "):
        return "_year"
    if "non-equity" in s or "non equity" in s:
        return "non_equity_incentive"
    if "nonqualified" in s and "deferred" in s:
        return "pension_change"
    if "change in pension" in s:
        return "pension_change"
    if "pension" in s and "deferred" not in s and "nonqualified" not in s:
        return "pension_change"
    if "all other" in s:
        return "other_comp"
    if "total" in s and "subtotal" not in s and "grant date" not in s:
        return "total"
    if "option" in s and "stock" not in s:
        return "option_awards"
    if "stock" in s:
        return "stock_awards"
    if "bonus" in s:
        return "bonus"
    if "salary" in s:
        return "salary"
    return None


def _build_summary_comp_field_by_col(header_tr) -> Optional[dict[int, str]]:
    """
    ix-style summary comp: header row uses ``colspan``; map each grid column index
    to a NeoCompRow field. Returns None if this does not look like a mapped table.
    """
    field_by_col: dict[int, str] = {}
    i = 0
    for td in header_tr.find_all("td", recursive=False):
        label = " ".join(td.stripped_strings).strip()
        cs = int(td.get("colspan") or 1)
        if label:
            fn = _classify_comp_header_label(label)
            if fn:
                for k in range(cs):
                    field_by_col[i + k] = fn
        i += cs
    vals = set(field_by_col.values())
    money = {v for v in vals if v not in ("_name", "_year")}
    if not money:
        return None
    if "salary" not in money and "stock_awards" not in money:
        return None
    if "total" not in money and "stock_awards" not in money and "bonus" not in money:
        return None
    return field_by_col


def _year_column_index(raw: list[str], field_by_col: dict[int, str]) -> Optional[int]:
    for idx, fn in field_by_col.items():
        if fn == "_year":
            return idx
    for i, t in enumerate(raw):
        if _YEAR_RE.match(t.strip()):
            return i
    return None


def _first_money_in_columns(raw: list[str], cols: list[int]) -> Optional[float]:
    for j in sorted(set(cols)):
        if j < 0 or j >= len(raw):
            continue
        t = raw[j].strip()
        v = _parse_money(t)
        if v is not None:
            return v
        if _is_dash_cell(t):
            continue
    return None


def _neo_row_from_header_mapped_row(
    raw: list[str],
    field_by_col: dict[int, str],
    *,
    last_name: Optional[str],
    role_for_last: Optional[str],
) -> tuple[Optional[NeoCompRow], Optional[str], Optional[str]]:
    """Parse one data ``tr`` when the table header supplied ``field_by_col``."""
    if not any(x.strip() for x in raw):
        return None, last_name, role_for_last

    year_idx = _year_column_index(raw, field_by_col)
    if year_idx is None:
        first = next((t for t in raw if t.strip()), "")
        if first and _looks_like_role_line(first) and last_name:
            return None, last_name, first.strip()
        return None, last_name, role_for_last

    try:
        year = int(raw[year_idx].strip())
    except ValueError:
        return None, last_name, role_for_last

    name_idxs = [i for i, f in field_by_col.items() if f == "_name"]
    name_i = min(name_idxs) if name_idxs else 0
    name_cell = raw[name_i].strip() if name_i < len(raw) else ""
    role_hint: Optional[str] = None
    if name_cell and not _looks_like_role_line(name_cell):
        name = _clean_person_name(name_cell)
        last_name = name
        role_for_last = None
    elif name_cell and _looks_like_role_line(name_cell) and last_name:
        name = last_name
        role_hint = name_cell.strip()
    elif last_name:
        name = last_name
        role_hint = role_for_last
        role_for_last = None
    else:
        return None, last_name, role_for_last

    if not is_acceptable_lead_person_name(name):
        refined = refine_lead_person_name(name_cell) or (
            refine_lead_person_name(role_hint) if role_hint else None
        )
        if refined:
            name = refined
    if not is_acceptable_lead_person_name(name):
        return None, last_name, role_for_last

    cols_by_field: dict[str, list[int]] = {}
    for col, fn in field_by_col.items():
        if fn.startswith("_"):
            continue
        cols_by_field.setdefault(fn, []).append(col)

    salary = _first_money_in_columns(raw, cols_by_field.get("salary", []))
    bonus = _first_money_in_columns(raw, cols_by_field.get("bonus", []))
    stock = _first_money_in_columns(raw, cols_by_field.get("stock_awards", []))
    option_a = _first_money_in_columns(raw, cols_by_field.get("option_awards", []))
    non_eq = _first_money_in_columns(raw, cols_by_field.get("non_equity_incentive", []))
    pension = _first_money_in_columns(raw, cols_by_field.get("pension_change", []))
    other = _first_money_in_columns(raw, cols_by_field.get("other_comp", []))
    total = _first_money_in_columns(raw, cols_by_field.get("total", []))

    row = NeoCompRow(
        person_name=name,
        role_hint=role_hint,
        fiscal_year=year,
        salary=salary,
        bonus=bonus,
        stock_awards=stock,
        option_awards=option_a,
        non_equity_incentive=non_eq,
        pension_change=pension,
        other_comp=other,
        total=total,
    )
    return row, last_name, role_for_last


def _parse_comp_table_with_header_map(table, header_tr) -> list[NeoCompRow]:
    field_by_col = _build_summary_comp_field_by_col(header_tr)
    if not field_by_col:
        return []
    rows = table.find_all("tr")
    try:
        skip = rows.index(header_tr) + 1
    except ValueError:
        return []
    out: list[NeoCompRow] = []
    last_name: Optional[str] = None
    role_for_last: Optional[str] = None
    for tr in rows[skip:]:
        raw = _row_cells(tr)
        neo, last_name, role_for_last = _neo_row_from_header_mapped_row(
            raw, field_by_col, last_name=last_name, role_for_last=role_for_last
        )
        if neo is not None:
            out.append(neo)
    return out


def _parse_comp_table_legacy(table) -> list[NeoCompRow]:
    rows = table.find_all("tr")
    if len(rows) < 3:
        return []
    skip_header = 1
    if _looks_like_summary_comp_header(rows[0]):
        skip_header = 1
    elif len(rows) >= 3 and _looks_like_summary_comp_two_row(rows[0], rows[1]):
        skip_header = 2
    else:
        return []

    out: list[NeoCompRow] = []
    last_name: Optional[str] = None
    role_for_last: Optional[str] = None

    for tr in rows[skip_header:]:
        raw = _row_cells(tr)
        if not any(x.strip() for x in raw):
            continue

        year_idx: Optional[int] = None
        for i, t in enumerate(raw):
            u = t.strip()
            if _YEAR_RE.match(u):
                year_idx = i
                break

        if year_idx is None:
            first = next((t for t in raw if t.strip()), "")
            if first and _looks_like_role_line(first) and last_name:
                role_for_last = first.strip()
            continue

        year = int(raw[year_idx].strip())
        name_cell = next((t for t in raw[:year_idx] if t.strip()), "")
        role_hint: Optional[str] = None
        if name_cell and not _looks_like_role_line(name_cell):
            name = _clean_person_name(name_cell)
            last_name = name
            role_for_last = None
        elif name_cell and _looks_like_role_line(name_cell) and last_name:
            name = last_name
            role_hint = name_cell.strip()
        elif last_name:
            name = last_name
            role_hint = role_for_last
            role_for_last = None
        else:
            continue

        if not is_acceptable_lead_person_name(name):
            refined = refine_lead_person_name(name_cell) or (
                refine_lead_person_name(role_hint) if role_hint else None
            )
            if refined:
                name = refined
        if not is_acceptable_lead_person_name(name):
            continue

        money_raw: list[Optional[float]] = []
        for t in raw[year_idx + 1 :]:
            if not t.strip():
                continue
            v = _parse_money(t)
            if v is not None:
                money_raw.append(v)
            elif _is_dash_cell(t):
                money_raw.append(None)

        if not money_raw:
            continue

        money_raw = _squash_spacer_moneys(money_raw)
        (
            salary,
            bonus,
            stock,
            option_a,
            non_eq,
            pension,
            other,
            total,
        ) = _assign_money_slots(money_raw)

        out.append(
            NeoCompRow(
                person_name=name,
                role_hint=role_hint,
                fiscal_year=year,
                salary=salary,
                bonus=bonus,
                stock_awards=stock,
                option_awards=option_a,
                non_equity_incentive=non_eq,
                pension_change=pension,
                other_comp=other,
                total=total,
            )
        )

    return out


def _parse_comp_table(table) -> list[NeoCompRow]:
    rows = table.find_all("tr")
    if len(rows) < 3:
        return []
    header_tr = rows[0]
    if _looks_like_summary_comp_header(header_tr):
        mapped = _parse_comp_table_with_header_map(table, header_tr)
        if mapped:
            return mapped
        return _parse_comp_table_legacy(table)
    if len(rows) >= 3 and _looks_like_summary_comp_two_row(rows[0], rows[1]):
        return _parse_comp_table_legacy(table)
    return []


def _sec_archives_url_ok(url: str) -> bool:
    u = (url or "").strip().lower()
    return u.startswith("https://www.sec.gov/archives/") or u.startswith(
        "http://www.sec.gov/archives/"
    )


def is_sec_archives_document_url(url: str) -> bool:
    """True if ``url`` is an SEC Archives document (safe to fetch for profile enrichment)."""
    return _sec_archives_url_ok(url)


_EDGAR_UA = (
    "Mozilla/5.0 (compatible; WealthLeads/1.0; institutional filing research; "
    "+https://www.sec.gov/os/webmaster-faq#code-support)"
)


@lru_cache(maxsize=128)
def _fetch_edgar_html_cached(url: str) -> str:
    if not _sec_archives_url_ok(url):
        raise ValueError("URL must be an SEC Archives primary document")
    r = requests.get(
        url,
        headers={"User-Agent": _EDGAR_UA},
        timeout=30,
    )
    r.raise_for_status()
    return r.text


def load_edgar_primary_doc_html(url: str) -> str:
    """Cached GET of an SEC Archives ``primary_doc`` HTML (same as profile equity helper)."""
    return _fetch_edgar_html_cached(url)


def _person_cell_matches_name(cell: str, display_name: str) -> bool:
    c = re.sub(r"\s*\([^)]*\)", "", cell).strip().lower()
    d = re.sub(r"\s*\([^)]*\)", "", display_name).strip().lower()
    if not c or not d:
        return False
    if c.startswith(d) or d.startswith(c):
        return True
    parts_d = d.split()
    if len(parts_d) >= 2:
        last = parts_d[-1]
        first = parts_d[0]
        return last in c and first in c
    return d in c or c in d


def _option_awards_header_columns(header_cells: list[str]) -> Optional[dict[str, int]]:
    """Map logical keys to column indices for outstanding option award tables."""
    lower = [re.sub(r"\s+", " ", x).strip().lower() for x in header_cells]

    def find(pred) -> Optional[int]:
        for i, s in enumerate(lower):
            if s and pred(s):
                return i
        return None

    grant_i = find(lambda s: "grant date" in s)
    strike_i = find(lambda s: "exercise price" in s and "option" in s)
    exp_i = find(lambda s: "expiration" in s)
    ex_i = find(lambda s: "exercisable" in s and "unexercisable" not in s)
    un_i = find(lambda s: "unexercisable" in s)
    if grant_i is None or strike_i is None:
        return None
    return {
        "grant": grant_i,
        "exercisable": ex_i,
        "unexercisable": un_i,
        "strike": strike_i,
        "expiration": exp_i,
    }


def extract_outstanding_option_awards_from_s1(
    html: str, display_name: str
) -> list[dict[str, Any]]:
    """
    Best-effort parse of “outstanding option awards” style ix tables (exercisable /
    unexercisable / strike / expiration). Filing layouts vary.
    """
    soup = BeautifulSoup(html, "html.parser")
    out: list[dict[str, Any]] = []
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if len(rows) < 2:
            continue
        colmap: Optional[dict[str, int]] = None
        header_row_idx = 0
        for hi in range(min(4, len(rows))):
            header_cells = _row_cells(rows[hi])
            if len(header_cells) < 6:
                continue
            colmap = _option_awards_header_columns(header_cells)
            if colmap:
                header_row_idx = hi
                break
        if not colmap:
            continue
        for tr in rows[header_row_idx + 1 :]:
            raw = _row_cells(tr)
            if len(raw) < 4 or not raw[0].strip():
                continue
            if not _person_cell_matches_name(raw[0], display_name):
                continue
            grant = raw[colmap["grant"]].strip() if colmap["grant"] < len(raw) else ""
            ex_i, un_i = colmap.get("exercisable"), colmap.get("unexercisable")
            ex_v = (
                _parse_money(raw[ex_i]) if ex_i is not None and ex_i < len(raw) else None
            )
            un_v = (
                _parse_money(raw[un_i]) if un_i is not None and un_i < len(raw) else None
            )
            strike = (
                _parse_money(raw[colmap["strike"]])
                if colmap["strike"] < len(raw)
                else None
            )
            exp_i = colmap.get("expiration")
            exp_s = (
                raw[exp_i].strip()
                if exp_i is not None and exp_i < len(raw)
                else ""
            )
            if not grant and strike is None and ex_v is None and un_v is None:
                continue
            out.append(
                {
                    "kind": "option",
                    "grant_date": grant or "—",
                    "exercisable_shares": ex_v,
                    "unexercisable_shares": un_v,
                    "exercise_price_usd": strike,
                    "expiration": exp_s or "—",
                }
            )
    return out


def extract_rsu_or_stock_award_grants_from_s1(
    html: str, display_name: str
) -> list[dict[str, Any]]:
    """Parse simple “Type of Award / # Granted” NEO grant summary tables."""
    soup = BeautifulSoup(html, "html.parser")
    out: list[dict[str, Any]] = []
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if len(rows) < 2:
            continue
        h0 = " ".join(_row_cells(rows[0])).lower()
        if "type of award" not in h0 or "named executive" not in h0:
            continue
        for tr in rows[1:]:
            raw = _row_cells(tr)
            if len(raw) < 5:
                continue
            name_c = raw[0].strip()
            if not name_c or not _person_cell_matches_name(name_c, display_name):
                continue
            kind = raw[2].strip() if len(raw) > 2 else ""
            qty = _parse_money(raw[4]) if len(raw) > 4 else None
            if not kind or kind.upper() == "N/A":
                continue
            out.append(
                {
                    "kind": "grant",
                    "award_type": kind,
                    "shares_or_units": qty,
                }
            )
    return out


def extract_neo_compensation_from_s1(html: str) -> list[NeoCompRow]:
    """
    Extract rows from Summary Compensation-style tables in an S-1 HTML body.
    Values are as disclosed (often grant-date fair value for stock awards).
    """
    soup = BeautifulSoup(html, "html.parser")
    seen: set[tuple[str, int]] = set()
    merged: list[NeoCompRow] = []

    for table in soup.find_all("table"):
        rows = _parse_comp_table(table)
        for r in rows:
            if not r.person_name.strip():
                continue
            key = (r.person_name.lower(), r.fiscal_year)
            if key in seen:
                continue
            seen.add(key)
            merged.append(r)

    merged.sort(key=lambda x: (x.person_name.lower(), -x.fiscal_year))
    return merged
