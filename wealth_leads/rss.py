from __future__ import annotations

import html as html_lib
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Optional

from wealth_leads.config import RSS_URL, rss_count, rss_url_for_form
from wealth_leads.sec_client import get_text

ATOM = "{http://www.w3.org/2005/Atom}"


@dataclass(frozen=True)
class RssFiling:
    accession: str
    cik: str
    company_name: str
    form_type: str
    filing_date: Optional[str]
    index_url: str


_TITLE_RE = re.compile(
    r"^(?P<form>S-1/A|S-1|10-K/A|10-K)\s+-\s+(?P<company>.+?)\s+\((?P<cik>\d+)\)\s+\(Filer\)\s*$"
)


def _parse_summary(summary_html: str) -> tuple[Optional[str], Optional[str]]:
    """Return (filing_date, accession) from RSS summary HTML."""
    text = html_lib.unescape(summary_html)
    filed_m = re.search(r"Filed:</b>\s*(\d{4}-\d{2}-\d{2})", text, re.I)
    acc_m = re.search(r"AccNo:</b>\s*([0-9]{10}-\d{2}-\d{6})", text, re.I)
    return (
        filed_m.group(1) if filed_m else None,
        acc_m.group(1) if acc_m else None,
    )


def _accession_from_entry_id(entry_id_text: str) -> Optional[str]:
    m = re.search(r"accession-number=([0-9]{10}-\d{2}-\d{6})", entry_id_text)
    return m.group(1) if m else None


def parse_atom_feed(xml_text: str) -> list[RssFiling]:
    root = ET.fromstring(xml_text)
    out: list[RssFiling] = []
    for entry in root.findall(f"{ATOM}entry"):
        title_el = entry.find(f"{ATOM}title")
        link_el = entry.find(f"{ATOM}link")
        summary_el = entry.find(f"{ATOM}summary")
        cat_el = entry.find(f"{ATOM}category")
        id_el = entry.find(f"{ATOM}id")
        if title_el is None or link_el is None or title_el.text is None:
            continue
        title = title_el.text.strip()
        m = _TITLE_RE.match(title)
        if not m:
            continue
        href = link_el.get("href") or ""
        if not href:
            continue
        index_url = href if href.startswith("http") else "https://www.sec.gov" + href
        summary_raw = (
            summary_el.text if summary_el is not None and summary_el.text else ""
        )
        filed, acc_from_summary = _parse_summary(summary_raw)
        accession = acc_from_summary or (
            _accession_from_entry_id(id_el.text)
            if id_el is not None and id_el.text
            else None
        )
        if not accession:
            continue
        term = (cat_el.get("term") if cat_el is not None else "") or ""
        cik_raw = m.group("cik")
        cik = str(int(cik_raw)) if cik_raw.isdigit() else cik_raw.lstrip("0") or "0"
        out.append(
            RssFiling(
                accession=accession,
                cik=cik,
                company_name=m.group("company").strip(),
                form_type=term or m.group("form"),
                filing_date=filed,
                index_url=index_url,
            )
        )
    return out


def fetch_current_feed(session=None, *, form_type: str = "S-1") -> list[RssFiling]:
    """Fetch EDGAR 'current' Atom feed for one form type (S-1, 10-K, …)."""
    url = rss_url_for_form(form_type).format(count=rss_count())
    xml_text = get_text(url, session=session)
    return parse_atom_feed(xml_text)


def fetch_current_s1_feed(session=None) -> list[RssFiling]:
    return fetch_current_feed(session, form_type="S-1")
