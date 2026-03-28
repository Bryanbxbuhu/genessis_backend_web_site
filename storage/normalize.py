"""
Domain normalization utilities for hospital/facility websites.
"""

import re
from urllib.parse import urlparse

import tldextract


def normalize_domain(url: str) -> str:
    """
    Normalize URL to registrable domain (main site, no subdomains).

    Example:
        https://doctors.mountsinai.org/location/... => https://mountsinai.org
        http://www.baptisthealth.net/en/pages/home.aspx => http://www.baptisthealth.net

    Rules:
        - Preserve existing scheme; only add https:// if missing.
        - Never prepend "www." unless it already exists.
        - Collapse repeated "www." prefixes (www.www.example.com -> www.example.com).

    Args:
        url: Full URL or domain

    Returns:
        Canonical URL with main domain only (no subdomains)
    """
    if not url or not url.strip():
        return ""

    try:
        raw = url.strip()
        has_scheme = "://" in raw
        parsed = urlparse(raw if has_scheme else f"https://{raw}")
        hostname = parsed.hostname or ""
        if not hostname:
            return url

        hostname = re.sub(r"^(?:www\.){2,}", "www.", hostname.lower())
        scheme = parsed.scheme if parsed.scheme else "https"
        if not has_scheme:
            scheme = "https"

        extracted = tldextract.extract(hostname)
        if extracted.domain and extracted.suffix:
            base = f"{extracted.domain}.{extracted.suffix}"
            keep_www = hostname.startswith("www.")
            prefix = "www." if keep_www and extracted.domain.lower() != "www" else ""
            return f"{scheme}://{prefix}{base}"
    except Exception:
        return url

    return url  # Return original if parsing fails


def is_aggregator_domain(url: str) -> bool:
    """
    Check if URL is from an aggregator/directory site (not official hospital website).
    
    Args:
        url: URL to check
        
    Returns:
        True if URL is from a known aggregator site
    """
    if not url:
        return False
    
    aggregators = [
        "yelp.com",
        "healthgrades.com",
        "mapquest.com",
        "yellowpages.com",
        "wikipedia.org",
        "facebook.com",
        "instagram.com",
        "twitter.com",
        "tripadvisor.com",
        "vitals.com",
        "zocdoc.com",
        "webmd.com",
        "sharecare.com",
        "google.com",
        "bing.com",
        "apple.com",
        "foursquare.com",
        "linkedin.com",
        # Travel/rental car aggregators
        "wotif.com",
        "rentalcars.com",
        "booking.com",
        "expedia.com",
        "kayak.com",
        "priceline.com",
        "orbitz.com",
        "hotwire.com",
        "travelocity.com",
        "cheaptickets.com",
        "hotels.com",
        "trip.com",
        "trivago.com",
    ]
    
    url_lower = url.lower()
    return any(agg in url_lower for agg in aggregators)


def normalize_facility_name(name: str) -> str:
    """
    Normalize facility name for deduplication.
    
    Args:
        name: Facility name
        
    Returns:
        Normalized lowercase alphanumeric string
    """
    import re
    return re.sub(r'[^a-z0-9]+', '', name.lower())
