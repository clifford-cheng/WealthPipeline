from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional

from bs4 import BeautifulSoup

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


def _parse_comp_table(table) -> list[NeoCompRow]:
    rows = table.find_all("tr")
    if len(rows) < 3:
        return []
    if not _looks_like_summary_comp_header(rows[0]):
        return []

    out: list[NeoCompRow] = []
    last_name: Optional[str] = None
    role_for_last: Optional[str] = None

    for tr in rows[1:]:
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
            key = (r.person_name.lower(), r.fiscal_year)
            if key in seen:
                continue
            seen.add(key)
            merged.append(r)

    merged.sort(key=lambda x: (x.person_name.lower(), -x.fiscal_year))
    return merged
