from __future__ import annotations

from typing import Optional

from bs4 import BeautifulSoup

from wealth_leads.sec_client import absolute_url


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
            return absolute_url(link["href"])
    return None
