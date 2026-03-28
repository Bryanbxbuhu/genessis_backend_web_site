"""
Website canonicalization for rental car brands by country.
Ensures curated_places use official country-specific domains.
"""

from typing import Optional
import re

# Canonical rental car brand websites by ISO country code
RENTAL_CAR_CANONICAL = {
    "ES": {
        "enterprise": "https://www.enterprise.es",
        "avis": "https://www.avis.es",
        "budget": "https://www.budget.es",
        "europcar": "https://www.europcar.es",
    },
    "US": {
        "enterprise": "https://www.enterprise.com",
        "avis": "https://www.avis.com",
        "budget": "https://www.budget.com",
        "europcar": "https://www.europcar.com",
    },
    "FR": {
        "enterprise": "https://www.enterprise.fr",
        "avis": "https://www.avis.fr",
        "budget": "https://www.budget.fr",
        "europcar": "https://www.europcar.fr",
    },
    "PT": {
        # Use global .com domains for these US-rooted brands in Portugal
        "enterprise": "https://www.enterprise.com",
        "avis": "https://www.avis.com",
        "budget": "https://www.budget.com",
        "europcar": "https://www.europcar.com",
    },
    "CZ": {
        "enterprise": "https://www.enterprise.cz",
        "avis": "https://www.avis.cz",
        "budget": "https://www.budget.cz",
        "europcar": "https://www.europcar.cz",
    },
}


def detect_rental_brand(name: str) -> Optional[str]:
    """
    Detect rental car brand from a place name.
    
    Args:
        name: Place name (e.g., "Enterprise Rent-A-Car", "Avis")
        
    Returns:
        Brand key (e.g., "enterprise", "avis") or None if not detected
    """
    if not name:
        return None
    
    # Normalize: lowercase, strip common suffixes
    normalized = name.lower()
    normalized = re.sub(r'\s*(rent[- ]a[- ]car|car rental|alquiler de coches)\s*', ' ', normalized)
    normalized = normalized.strip()
    
    # Match known brands
    if "enterprise" in normalized:
        return "enterprise"
    elif "avis" in normalized:
        return "avis"
    elif "budget" in normalized:
        return "budget"
    elif "europcar" in normalized:
        return "europcar"
    
    return None


def canonicalize_rental_car_website(
    name: str,
    country_code: str,
    website: Optional[str]
) -> Optional[str]:
    """
    Return the canonical website for a rental car brand in a given country.
    
    Args:
        name: Place name
        country_code: ISO country code (e.g., "ES", "US")
        website: Current website URL (may be None or wrong country)
        
    Returns:
        Canonical website URL for the brand in that country, or original website if no mapping exists
    """
    if not country_code:
        return website

    brand = detect_rental_brand(name)
    country_code = country_code.strip().upper()
    
    if brand and country_code in RENTAL_CAR_CANONICAL:
        country_brands = RENTAL_CAR_CANONICAL[country_code]
        if brand in country_brands:
            return country_brands[brand]
    
    # No mapping found, return original
    return website
