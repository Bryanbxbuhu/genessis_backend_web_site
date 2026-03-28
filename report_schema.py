"""
Pydantic models for validating report data structure.
Ensures type safety and provides clear schema for AI-generated content.
"""

from typing import List, Optional
from pydantic import BaseModel, Field


class RiskBadge(BaseModel):
    """Risk indicator with severity level."""
    name: str = Field(..., description="Risk category name (e.g., 'Crime', 'Weather', 'Health')")
    level: str = Field(..., description="Risk level: 'low', 'med', or 'high'")
    note: Optional[str] = Field(None, description="Brief context (e.g., 'robbery/shootings in feed')")


class Cue(BaseModel):
    """Conditional action based on observed situation."""
    cue: str = Field(..., description="Observable situation or trigger")
    action: str = Field(..., description="Recommended action to take")
    avoid: Optional[str] = Field(None, description="What to avoid (short phrase)")


class Incident(BaseModel):
    """Traveler-relevant incident from news feeds."""
    date: str = Field(..., description="Date of incident (e.g., 'Dec 25' or 'Yesterday')")
    type: str = Field(..., description="Incident category (e.g., 'Weather', 'Crime', 'Transportation')")
    impact: str = Field(..., description="Brief description of traveler impact")
    where: Optional[str] = Field(None, description="Location/neighborhood if available")
    recency: Optional[str] = Field(None, description="Relative time: 'Today', 'Xd ago' for past, 'in Xd'/'in Xw' for future")


class Hospital(BaseModel):
    """Hospital or medical facility information."""
    name: str = Field(..., description="Hospital name")
    notes: Optional[str] = Field(None, description="Additional info (e.g., 'Level 1 Trauma Center')")
    website: Optional[str] = Field(None, description="Hospital website URL")


class AirportPOI(BaseModel):
    """Airport information."""
    name: str = Field(..., description="Airport name")
    iata: Optional[str] = Field(None, description="IATA airport code (e.g., 'LHR')")
    website: Optional[str] = Field(None, description="Airport website URL")


class PharmacyPOI(BaseModel):
    """Pharmacy or drugstore point of interest."""
    name: str = Field(..., description="Pharmacy name")
    address: Optional[str] = Field(None, description="Street address")
    website: Optional[str] = Field(None, description="Pharmacy website URL")
    notes: Optional[str] = Field(None, description="Additional info (e.g., '24-hour', 'Drive-thru')")


class StorePOI(BaseModel):
    """Retail store point of interest (supermarket, convenience store, etc.)."""
    name: str = Field(..., description="Store name")
    address: Optional[str] = Field(None, description="Street address")
    website: Optional[str] = Field(None, description="Store website URL")
    notes: Optional[str] = Field(None, description="Additional info (e.g., 'Open 24hrs', 'Groceries + pharmacy')")


class RentalCarPOI(BaseModel):
    """Car rental company point of interest."""
    name: str = Field(..., description="Rental company name (e.g., 'Enterprise', 'Hertz', 'Europcar')")
    address: Optional[str] = Field(None, description="Street address of rental location")
    website: Optional[str] = Field(None, description="Official website URL")
    phone: Optional[str] = Field(None, description="Contact phone number")
    notes: Optional[str] = Field(None, description="Additional info (e.g., 'Airport location', '24hr pickup')")


class Transit(BaseModel):
    """Public transportation system information."""
    name: str = Field(..., description="Transit system name (e.g., 'Miami-Dade Transit')")
    modes: List[str] = Field(..., description="List of transit modes (e.g., ['Metrorail', 'Metrobus'])")
    website: Optional[str] = Field(None, description="Official website URL")
    description: Optional[str] = Field(None, description="Brief description of available services")
    taxis: Optional[str] = Field(None, description="Taxi availability info")
    rideshare: Optional[str] = Field(None, description="Rideshare services (Uber, Lyft, etc.)")


class EmergencyOtherContact(BaseModel):
    """Additional emergency contact (non-primary lines)."""
    label: str = Field(..., description="Label for the contact (e.g., 'Deaf SMS')")
    number: str = Field(..., description="Phone number or short code")


class EmergencyContacts(BaseModel):
    """Emergency contact numbers for a location."""
    general: str = Field(..., description="General emergency number")
    police: Optional[str] = Field(None, description="Police emergency number")
    ambulance: Optional[str] = Field(None, description="Ambulance emergency number")
    fire: Optional[str] = Field(None, description="Fire emergency number")
    other: List[EmergencyOtherContact] = Field(default_factory=list, description="Additional emergency contacts")


class WeatherSummary(BaseModel):
    """Weather forecast summary for travelers."""
    current: str = Field(..., description="Current weather conditions summary")
    outlook: str = Field(..., description="7-day forecast outlook summary")
    traveler_impact: Optional[str] = Field(None, description="How weather affects travel plans (e.g., 'Pack rain gear', 'Expect high heat')")


class OpsecTip(BaseModel):
    """Generic OPSEC/safety tip applicable to travelers."""
    category: str = Field(..., description="Tip category (e.g., 'Hotel Security', 'Transit Safety', 'Digital Security', 'Emergency Response')")
    tip: str = Field(..., description="Concise actionable safety recommendation")


class ContextualOpsecTip(OpsecTip):
    """Evidence-based OPSEC tip tied to current incidents."""
    why_now: str = Field(..., description="Why this tip is relevant right now based on current conditions")
    evidence: List[str] = Field(..., description="Source strings from news/incidents that justify this tip (must cite actual incidents/sources)")
    confidence: float = Field(..., ge=0.0, le=1.0, description="Confidence level 0.0-1.0 based on evidence strength")


class OpsecSection(BaseModel):
    """Operational security and personal safety guidance."""
    baseline_tips: List[OpsecTip] = Field(..., description="Always-applicable safety best practices (3-5 tips)")
    contextual_tips: List[ContextualOpsecTip] = Field(..., description="Current situation-specific tips with evidence (2-4 tips)")


class TripProfile(BaseModel):
    """Trip configuration and traveler preferences."""
    will_drive: bool = Field(..., description="Whether the trip includes driving or car rental")
    needs_idp: bool = Field(False, description="Whether an International Driving Permit is recommended")
    rental_provider: Optional[str] = Field(None, description="Car rental company if known")
    notes: Optional[str] = Field(None, description="Additional trip context or preferences")


class DrivingPack(BaseModel):
    """Driving documentation and road safety guidance."""
    checklist: List[str] = Field(..., description="Document and preparation checklist items")
    rental_notes: List[str] = Field(..., description="Rental-specific requirements and tips")
    accident_steps: List[str] = Field(..., description="Steps to take if involved in an accident")


class ReportContext(BaseModel):
    """
    Complete report data structure for template rendering.
    All fields validated for type safety before PDF generation.
    """
    # Header metadata
    destination: str = Field(..., description="City, State/Province, Country")
    generated_utc: str = Field(..., description="ISO timestamp (YYYY-MM-DD HH:MM UTC)")
    lookback_label: str = Field(..., description="Time window (e.g., 'Past 48 hours')")
    coverage_label: str = Field(..., description="Data sources coverage (e.g., 'Local news + City context')")
    
    # At-a-glance metrics
    overall_risk: str = Field(..., description="Overall risk level: 'low', 'med', or 'high'")
    top_risk_driver: str = Field(..., description="Primary risk category (e.g., 'Crime', 'Weather')")
    coverage_status: str = Field(..., description="Feed status (e.g., 'All feeds OK', 'Partial data')")
    
    # Left column content
    top_actions: List[str] = Field(..., description="Priority actions for travelers (max 5)")
    whats_happening: List[str] = Field(..., description="Current events summary bullets")
    incidents: List[Incident] = Field(default_factory=list, description="Structured incident table data")
    
    # Right column content
    risk_badges: List[RiskBadge] = Field(..., description="Risk indicators with severity levels")
    cues: List[Cue] = Field(..., description="Situational awareness guidance")
    hospitals: List[Hospital] = Field(default_factory=list, description="Major medical facilities")
    airports: List[AirportPOI] = Field(default_factory=list, description="Nearby airports")
    transit: List[Transit] = Field(default_factory=list, description="Public transportation systems")
    emergency_contacts: EmergencyContacts = Field(..., description="Emergency phone numbers by category")
    
    # Supplies & Services section
    pharmacies: List[PharmacyPOI] = Field(default_factory=list, description="Pharmacies and drugstores")
    supermarkets: List[StorePOI] = Field(default_factory=list, description="Supermarkets and grocery stores")
    convenience_stores: List[StorePOI] = Field(default_factory=list, description="Convenience stores and corner shops")
    rental_cars: List[RentalCarPOI] = Field(default_factory=list, description="Car rental companies")
    parking: Optional[dict] = Field(None, description="Parking affiliate link (SpotHero) for US cities")
    
    # Weather section
    weather_summary: Optional[WeatherSummary] = Field(None, description="7-day weather forecast summary for travelers")
    
    # OPSEC & Personal Safety section
    opsec: Optional[OpsecSection] = Field(None, description="Operational security and personal safety guidance")
    
    # Driving Pack section (conditional)
    trip_profile: Optional[TripProfile] = Field(None, description="Trip configuration for conditional modules")
    driving_pack: Optional[DrivingPack] = Field(None, description="Driving documentation and road safety guidance (only when trip_profile.will_drive is true)")
    
    # Footer metadata
    data_coverage: str = Field(..., description="Data sources and freshness summary")
    sources: List[str] = Field(default_factory=list, description="News article titles or feed names")
    report_id: str = Field(..., description="Unique report identifier")
    
    # Travel advisory
    travel_advisory: Optional[str] = Field(None, description="Government travel advisory text")
    advisory_level: Optional[str] = Field(None, description="Advisory level description")
    advisory_source: Optional[str] = Field(None, description="Advisory source attribution")
    travel_advisory_url: Optional[str] = Field(None, description="Official government advisory URL")
    
    # Optional visualizations
    chart_path: Optional[str] = Field(None, description="Path to chart image (relative to template)")
    map_path: Optional[str] = Field(None, description="Path to map image (relative to template)")
    cover_image_path: Optional[str] = Field(None, description="Path to cover image (relative to template)")
    
    # Cover page fields
    disclaimer: Optional[str] = Field(None, description="Custom disclaimer text for cover page")
    
    # Additional contacts
    key_contacts: List[dict] = Field(default_factory=list, description="Additional emergency contacts with category/phone/website/notes")
    
    class Config:
        """Pydantic configuration."""
        validate_assignment = True
        extra = "forbid"  # Reject unknown fields
