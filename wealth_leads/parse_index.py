from __future__ import annotations

from typing import Optional
from urllib.parse import parse_qs, urlparse

from bs4 import BeautifulSoup

from wealth_leads.sec_client import absolute_url


def canonical_filing_document_url(url: Optional[str]) -> Optional[str]:
    """
    SEC often links the primary S-1 through the inline-XBRL viewer (/ix?doc=/Archives/...).
    That HTML is not the same as the raw registration statement; compensation tables
    parse reliably from the direct /Archives/.../file.htm URL.
    """
    if not url:
        return url
    u = absolute_url(url)
    if "/ix?doc=" not in u and "/ixviewer" not in u.lower():
        return u
    q = parse_qs(urlparse(u).query)
    doc = (q.get("doc") or [""])[0]
    if doc.startswith("/Archives/"):
        return absolute_url(doc)
    return u


def primary_s1_document_url(index_html: str) -> Optional[str]:
    """
    From an EDGAR filing index.htm, return absolute URL of the main S-1 / S-1/A
    registration statement document (first matching row in the Document Format Files table).
    """
    soup = BeautifulSoup(index_html, "html.parser")
    for table in soup.find_all("table", class_="tableFile"):
        header_cells = table.find_all("th")
        headers = [h.get_text(strip=True).lower() for h in header_cells]
        if not headers or "type" not in " ".join(headers):
            continue
        rows = table.find_all("tr")
        for tr in rows[1:]:
            cells = tr.find_all("td")
            if len(cells) < 4:
                continue
            # Seq, Description, Document, Type, Size
            type_cell = cells[3].get_text(strip=True).upper().replace(" ", "")
            if type_cell not in ("S-1", "S-1/A", "FORMS-1", "FORMS-1/A"):
                continue
            link = cells[2].find("a", href=True)
            if not link:
                continue
            return canonical_filing_document_url(link["href"])
    return None


# Normalized Type column values in EDGAR index tables for annual reports.
_10K_TYPES = frozenset(
    {
        "10-K",
        "10-K/A",
        "FORM10-K",
        "FORM10-K/A",
    }
)


def primary_10k_document_url(index_html: str) -> Optional[str]:
    """
    From an EDGAR filing index.htm, return the main 10-K / 10-K/A document URL
    (first matching row in the Document Format Files table).
    """
    soup = BeautifulSoup(index_html, "html.parser")
    for table in soup.find_all("table", class_="tableFile"):
        header_cells = table.find_all("th")
        headers = [h.get_text(strip=True).lower() for h in header_cells]
        if not headers or "type" not in " ".join(headers):
            continue
        rows = table.find_all("tr")
        for tr in rows[1:]:
            cells = tr.find_all("td")
            if len(cells) < 4:
                continue
            type_cell = cells[3].get_text(strip=True).upper().replace(" ", "")
            if type_cell not in _10K_TYPES:
                continue
            link = cells[2].find("a", href=True)
            if not link:
                continue
            return canonical_filing_document_url(link["href"])
    return None


def primary_document_url_for_form(index_html: str, form_type: str) -> Optional[str]:
    """Resolve primary HTML doc from index based on filing form (S-1 vs 10-K family)."""
    ft = (form_type or "").upper().replace(" ", "")
    if "10-K" in ft:
        u = primary_10k_document_url(index_html)
        if u:
            return u
    if "S-1" in ft:
        u = primary_s1_document_url(index_html)
        if u:
            return u
    u = primary_10k_document_url(index_html)
    if u:
        return u
    return primary_s1_document_url(index_html)
