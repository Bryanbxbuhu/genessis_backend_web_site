"""
Professional PDF report renderer using Jinja2 + Playwright.
Converts HTML/CSS templates to high-quality PDFs with full CSS support.
Supports multi-page structure with separate cover and body PDFs.
Includes post-merge processing for clickable TOC and PDF outline/bookmarks.
"""

from __future__ import annotations

import asyncio
import base64
import sys
from pathlib import Path
from typing import Any, Dict
from urllib.parse import urlparse

from jinja2 import Environment, FileSystemLoader, select_autoescape
from pypdf import PdfReader, PdfWriter

# Optional PyMuPDF import for TOC links and bookmarks
try:
    import fitz  # PyMuPDF
    HAS_PYMUPDF = True
except ImportError:
    HAS_PYMUPDF = False

# Fix for Windows asyncio + Playwright subprocess issue
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())


def image_to_base64(image_path: str) -> str:
    """
    Convert image file to base64 data URI for embedded PDF images.
    This ensures images render reliably in Playwright PDFs.
    
    Args:
        image_path: Path to image file
        
    Returns:
        Base64 data URI string (e.g., "data:image/png;base64,...")
    """
    try:
        path = Path(image_path)
        if not path.exists():
            return ""
        
        with open(path, "rb") as f:
            image_data = f.read()
        
        # Determine MIME type from extension
        mime_type = {
            ".png": "image/png",
            ".jpg": "image/jpeg",
            ".jpeg": "image/jpeg",
            ".gif": "image/gif",
            ".svg": "image/svg+xml"
        }.get(path.suffix.lower(), "image/png")
        
        b64_data = base64.b64encode(image_data).decode('utf-8')
        return f"data:{mime_type};base64,{b64_data}"
    except Exception as e:
        print(f"Warning: Failed to encode image {image_path}: {e}")
        return ""


def _safe_http_url(value: Any) -> str | None:
    """Return a URL only when it is a valid http/https absolute URL."""
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        parsed = urlparse(text)
    except Exception:
        return None
    if parsed.scheme.lower() not in {"http", "https"}:
        return None
    if not parsed.netloc:
        return None
    return text


def getPrimaryServiceUrl(service: Dict[str, Any] | None, user_agent: str = "") -> str:
    if not isinstance(service, dict):
        return ""

    default_url = _safe_http_url(service.get("url")) or ""
    ios_url = _safe_http_url(service.get("ios_url")) or ""
    android_url = _safe_http_url(service.get("android_url")) or ""

    # Keep non-app entries unchanged.
    if not ios_url and not android_url:
        return default_url

    ua = str(user_agent or "").lower()
    is_android = "android" in ua
    is_ios = ("iphone" in ua) or ("ipad" in ua) or ("ipod" in ua)

    if is_ios and ios_url:
        return ios_url
    if is_android and android_url:
        return android_url

    if ios_url:
        return ios_url
    if android_url:
        return android_url
    return default_url


def _source_link_meta(item: Any) -> Dict[str, str | None]:
    """
    Normalize a source item into a label/url pair for template rendering.

    URL priority:
    1) website/homepage-like fields
    2) feed-url-like fields
    3) no link
    """
    if isinstance(item, dict):
        label_fields = (
            item.get("name"),
            item.get("label"),
            item.get("title"),
            item.get("source"),
            item.get("publisher"),
        )
        label = ""
        for value in label_fields:
            text = str(value or "").strip()
            if text:
                label = text
                break

        website_url = (
            _safe_http_url(item.get("website"))
            or _safe_http_url(item.get("homepage"))
            or _safe_http_url(item.get("home_url"))
        )
        feed_url = (
            _safe_http_url(item.get("feed_url"))
            or _safe_http_url(item.get("rss_url"))
            or _safe_http_url(item.get("url"))
            or _safe_http_url(item.get("link"))
            or _safe_http_url(item.get("source_url"))
        )
        link_url = website_url or feed_url

        if not label:
            label = link_url or "Source"

        return {"label": label, "url": link_url}

    text = str(item or "").strip()
    if not text:
        return {"label": "", "url": None}
    return {"label": text, "url": _safe_http_url(text)}




def render_html(template_dir: Path, template_name: str, context: Dict[str, Any]) -> str:
    """
    Render a Jinja2 template to HTML string.
    
    Args:
        template_dir: Directory containing template files
        template_name: Name of the template file (e.g., "report.html")
        context: Dictionary of template variables
        
    Returns:
        Rendered HTML string
    """
    env = Environment(
        loader=FileSystemLoader(str(template_dir)),
        autoescape=select_autoescape(["html", "xml"]),
    )
    import config as _cfg
    env.globals["SPOTHERO_PARKING_SEARCH_URL"] = getattr(_cfg, "SPOTHERO_PARKING_SEARCH_URL", "")
    env.globals["ESIM_CARDS_URL"] = getattr(_cfg, "ESIM_CARDS_URL", "")
    env.globals["ESIM_CARDS_TOC_CTA"] = getattr(_cfg, "ESIM_CARDS_TOC_CTA", "")
    env.globals["ESIM_CARDS_TOC_BLURB"] = getattr(_cfg, "ESIM_CARDS_TOC_BLURB", "")
    env.globals["source_link_meta"] = _source_link_meta
    env.globals["getPrimaryServiceUrl"] = getPrimaryServiceUrl
    tpl = env.get_template(template_name)
    return tpl.render(**context)


async def html_to_pdf_playwright(
    html: str,
    output_pdf: Path,
    report_id: str = "",
    report_data_hash_short: str = "",
) -> Path:
    """
    Convert HTML to PDF using Playwright's Chromium engine.
    Provides excellent CSS compatibility for professional layouts.
    
    Args:
        html: HTML content to convert
        output_pdf: Output PDF file path
        report_id: Report identifier for footer
        
    Returns:
        Path to generated PDF file
    """
    from playwright.async_api import async_playwright

    output_pdf.parent.mkdir(parents=True, exist_ok=True)

    async with async_playwright() as p:
        browser = await p.chromium.launch()
        page = await browser.new_page()

        # Base URL allows relative image paths like charts/map files
        await page.set_content(html, wait_until="networkidle")
        
        # Configure PDF with page numbers in footer
        hash_span = (
            f"<span style=\"float:right;\">Report Data Hash: {report_data_hash_short}</span>"
            if report_data_hash_short
            else ""
        )

        await page.pdf(
            path=str(output_pdf),
            format="A4",
            print_background=True,
            margin={"top": "16mm", "right": "16mm", "bottom": "20mm", "left": "16mm"},
            display_header_footer=True,
            header_template="<div></div>",  # Empty header
            footer_template=f"""
                <div style="font-size:8px;width:100%;text-align:center;color:#6b7280;padding:0 16mm;">
                    <span style="float:left;">{report_id}</span>
                    {hash_span}
                    <span>Page <span class="pageNumber"></span> of <span class="totalPages"></span></span>
                </div>
            """
        )
        await browser.close()

    return output_pdf


def render_report_pdf(
    context: Dict[str, Any], 
    output_pdf: Path, 
    template_dir: Path = Path("templates")
) -> Path:
    """
    One-shot function to render a report PDF from template context.
    Generates cover and body as separate PDFs, then merges them.
    Cover has no footer; body has page numbers starting at "Page 1 of N".
    
    Args:
        context: Template variables (destination, incidents, etc.)
        output_pdf: Output PDF file path
        template_dir: Directory containing report_new.html template
        
    Returns:
        Path to generated PDF file
    """
    # Convert image paths to base64 data URIs for reliable PDF rendering
    if context.get("map_path"):
        context["map_path"] = image_to_base64(context["map_path"])
    if context.get("cover_image_path"):
        context["cover_image_path"] = image_to_base64(context["cover_image_path"])
    
    # Convert combined trend chart path to base64
    if context.get("combined_trend_chart_path"):
        context["combined_trend_chart_path"] = image_to_base64(context["combined_trend_chart_path"])
    
    # Render full HTML (used for both cover and body)
    html = render_html(template_dir=template_dir, template_name="report_new.html", context=context)
    report_id = context.get("report_id", "")
    report_data_hash_short = context.get("report_data_hash_short", "")
    
    # Generate temporary PDFs
    output_pdf.parent.mkdir(parents=True, exist_ok=True)
    temp_dir = output_pdf.parent / "temp"
    temp_dir.mkdir(exist_ok=True)
    
    cover_pdf = temp_dir / f"{output_pdf.stem}_cover.pdf"
    body_pdf = temp_dir / f"{output_pdf.stem}_body.pdf"
    
    # Generate cover PDF (no footer) and body PDF (with footer)
    asyncio.run(_generate_pdfs(html, cover_pdf, body_pdf, report_id, report_data_hash_short))
    
    # Merge PDFs
    merge_pdfs(cover_pdf, body_pdf, output_pdf)
    
    # Clean up temp files
    cover_pdf.unlink(missing_ok=True)
    body_pdf.unlink(missing_ok=True)
    try:
        if not any(temp_dir.iterdir()):
            temp_dir.rmdir()
    except (PermissionError, OSError):
        pass  # Ignore if directory can't be removed
    
    return output_pdf


async def _generate_pdfs(
    html: str,
    cover_pdf: Path,
    body_pdf: Path,
    report_id: str,
    report_data_hash_short: str,
):
    """
    Generate separate cover and body PDFs from the same HTML.
    Uses CSS page-break classes to control pagination.
    
    Args:
        html: Full HTML content
        cover_pdf: Output path for cover PDF (no footer)
        body_pdf: Output path for body PDF (with footer)
        report_id: Report identifier for footer
    """
    from playwright.async_api import async_playwright

    async with async_playwright() as p:
        browser = await p.chromium.launch()
        
        # Cover page (no footer)
        page_cover = await browser.new_page()
        await page_cover.set_content(html, wait_until="networkidle")
        await page_cover.pdf(
            path=str(cover_pdf),
            format="A4",
            print_background=True,
            margin={"top": "0mm", "right": "0mm", "bottom": "0mm", "left": "0mm"},
            display_header_footer=False,
            page_ranges="1"  # Only first page
        )
        await page_cover.close()
        
        # Body pages (with footer and page numbers)
        page_body = await browser.new_page()
        await page_body.set_content(html, wait_until="networkidle")
        hash_span = (
            f"<span style=\"float:right; font-size:6px; color:#9ca3af;\">Report Data Hash: {report_data_hash_short}</span>"
            if report_data_hash_short
            else ""
        )

        await page_body.pdf(
            path=str(body_pdf),
            format="A4",
            print_background=True,
            margin={"top": "16mm", "right": "16mm", "bottom": "20mm", "left": "16mm"},
            display_header_footer=True,
            header_template="<div></div>",
            footer_template=f"""
                <div style="font-size:8px;width:100%;text-align:center;color:#6b7280;padding:0 16mm;">
                    <span style="float:left;">{report_id}</span>
                    {hash_span}
                    <span>Page <span class="pageNumber"></span> of <span class="totalPages"></span></span>
                </div>
            """,
            page_ranges="2-"  # All pages except first
        )
        await page_body.close()
        
        await browser.close()


def merge_pdfs(cover_pdf: Path, body_pdf: Path, output_pdf: Path):
    """
    Merge cover and body PDFs into single output file.
    Then add clickable TOC links and PDF outline/bookmarks.
    
    Args:
        cover_pdf: Cover page PDF (no footer)
        body_pdf: Body pages PDF (with footer)
        output_pdf: Final merged PDF output
    """
    writer = PdfWriter()
    
    # Add cover page
    cover_reader = PdfReader(cover_pdf)
    for page in cover_reader.pages:
        writer.add_page(page)
    
    # Add body pages
    body_reader = PdfReader(body_pdf)
    for page in body_reader.pages:
        writer.add_page(page)
    
    # Write merged PDF
    with open(output_pdf, "wb") as f:
        writer.write(f)
    
    # Post-process: Add clickable TOC links and PDF outline
    add_toc_links_and_outline(output_pdf)


def find_toc_page(pdf_path: Path) -> int:
    """
    Find the page index containing the Table of Contents.
    
    Args:
        pdf_path: Path to merged PDF file
        
    Returns:
        Page index (0-based) of TOC page, or -1 if not found
    """
    doc = fitz.open(pdf_path)
    
    for page_num, page in enumerate(doc):
        text = page.get_text("text")
        # Check first 300 characters for TOC heading
        text_start = text[:300] if len(text) > 300 else text
        if "Table of Contents" in text_start:
            doc.close()
            return page_num
    
    doc.close()
    return -1


def find_section_pages(pdf_path: Path) -> Dict[str, int]:
    """
    Scan PDF pages to find where each section starts.
    Searches for section heading text at the start of pages.
    
    Args:
        pdf_path: Path to merged PDF file
        
    Returns:
        Dictionary mapping section titles to page indices (0-based)
    """
    sections = {}
    section_headings = [
        "Executive Summary",
        "Current Incidents",
        "Incident Analysis",  # Alternative heading
        "Airports",
        "City Essentials",
        "Supplies & Services",
        "OPSEC & Personal Safety",
        "Driving Pack",
        "Contacts & Sources",
        "Intelligence Sources"  # Alternative heading
    ]
    
    doc = fitz.open(pdf_path)
    
    for page_num, page in enumerate(doc):
        text = page.get_text("text")
        # Check first 500 characters of page for section headings
        text_start = text[:500] if len(text) > 500 else text
        
        for heading in section_headings:
            if heading in text_start:
                # Map to canonical section name
                if heading == "Incident Analysis":
                    sections["Current Incidents"] = page_num
                elif heading == "Intelligence Sources":
                    sections["Contacts & Sources"] = page_num
                else:
                    sections[heading] = page_num
                break  # One section per page
    
    doc.close()
    return sections


def add_toc_links_and_outline(pdf_path: Path):
    """
    Post-process merged PDF to:
    1. Find each section's page number
    2. Overlay page numbers on TOC placeholders
    3. Add clickable links on TOC page that jump to sections
    4. Add PDF outline/bookmarks for sidebar navigation
    
    Args:
        pdf_path: Path to merged PDF file
    """
    if not HAS_PYMUPDF:
        raise RuntimeError(
            "PyMuPDF is required for clickable TOC links and PDF bookmarks. "
            "Install it with: pip install PyMuPDF"
        )
    
    try:
        # Find where TOC page is
        toc_page_idx = find_toc_page(pdf_path)
        if toc_page_idx < 0:
            print("Warning: Could not find Table of Contents page. Skipping TOC processing.")
            return
        
        # Find where each section starts (returns 0-based page indices)
        section_pages = find_section_pages(pdf_path)
        
        if not section_pages:
            print("Warning: No section headings found in PDF. Skipping TOC processing.")
            return
        
        # Open PDF with PyMuPDF for editing
        doc = fitz.open(pdf_path)
        toc_page = doc[toc_page_idx]
        
        # Section ID to title mapping (matches data-target attributes in HTML)
        section_mapping = {
            "sec_exec_summary": "Executive Summary",
            "sec_current_incidents": "Current Incidents",
            "sec_airports": "Airports",
            "sec_city_essentials": "City Essentials",
            "sec_supplies_services": "Supplies & Services",
            "sec_opsec_personal_safety": "OPSEC & Personal Safety",
            "sec_driving_pack": "Driving Pack",
            "sec_contacts_sources": "Contacts & Sources"
        }
        
        # Find TOC items and add page numbers + clickable links
        added_links = 0
        toc_outline = []
        
        for section_id, section_title in section_mapping.items():
            # Skip sections not in the PDF
            if section_title not in section_pages:
                continue
            
            # Get destination page (0-based index)
            dest_page_idx = section_pages[section_title]
            # Convert to 1-based page number for display
            page_num = dest_page_idx + 1
            
            # Search for the TOC item by its title text
            # The TOC format is: "01 Executive Summary ... —"
            title_rects = toc_page.search_for(section_title)
            
            if title_rects:
                # Found the TOC item
                title_rect = title_rects[0]
                
                # Find the "—" placeholder and replace with page number
                placeholder_rects = toc_page.search_for("—", clip=title_rect.include_rect(
                    fitz.Rect(title_rect.x0, title_rect.y0, toc_page.rect.width, title_rect.y1 + 5)
                ))
                
                if placeholder_rects:
                    # Replace placeholder with page number
                    for placeholder_rect in placeholder_rects:
                        # Cover the "—" with white rectangle
                        toc_page.draw_rect(placeholder_rect, color=(1, 1, 1), fill=(1, 1, 1))
                        
                        # Insert page number text using standard font
                        toc_page.insert_text(
                            fitz.Point(placeholder_rect.x0, placeholder_rect.y1 - 2),
                            str(page_num),
                            fontsize=10.5,
                            fontname="helv",  # Helvetica (standard PDF font)
                            color=(0.067, 0.094, 0.157)  # --color-text (#111827)
                        )
                        break  # Only replace first match
                
                # Create clickable area for entire TOC row
                clickable_rect = fitz.Rect(
                    title_rect.x0 - 40,  # Include number column
                    title_rect.y0 - 2,
                    toc_page.rect.width - 16,
                    title_rect.y1 + 2
                )
                
                # Add clickable link
                link = {
                    "kind": fitz.LINK_GOTO,
                    "from": clickable_rect,
                    "page": dest_page_idx,
                    "to": fitz.Point(0, 0)
                }
                toc_page.insert_link(link)
                added_links += 1
                
                # Add to PDF outline/bookmarks
                toc_outline.append([1, section_title, page_num])
        
        # Set PDF outline
        if toc_outline:
            doc.set_toc(toc_outline)
        
        # Save modified PDF
        doc.save(pdf_path, incremental=True, encryption=fitz.PDF_ENCRYPT_KEEP)
        doc.close()
        
        print(f"✓ TOC: Populated {added_links} page numbers, added {added_links} clickable links and {len(toc_outline)} PDF bookmarks")
        
    except Exception as e:
        print(f"Warning: Failed to add TOC links/bookmarks: {e}")
        import traceback
        traceback.print_exc()
