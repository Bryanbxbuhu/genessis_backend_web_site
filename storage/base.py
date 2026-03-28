"""
Base interfaces and data structures for storage abstraction.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Any


@dataclass
class FeedItem:
    """Represents a single feed item (news article, advisory, etc.)."""
    id: str
    source_key: str
    city_key: Optional[str]
    guid: Optional[str]
    url: Optional[str]
    title: str
    summary: str
    published_at: Optional[datetime]
    fetched_at: datetime
    raw: Dict[str, Any]
    travel_relevance_score: Optional[float] = None
    travel_keywords_matched: Optional[List[str]] = None
    travel_relevance_reason: Optional[str] = None
    match_meta: Optional[Dict[str, Any]] = None


@dataclass
class CityContext:
    """Represents city context data (hospitals, urgent care, etc.)."""
    city_key: str
    context: Dict[str, Any]
    fetched_at: datetime


@dataclass
class TransitSnapshot:
    """Represents transit information snapshot."""
    city_key: str
    transit: Dict[str, Any]
    fetched_at: datetime


class DataStore(ABC):
    """Abstract interface for data storage operations."""
    
    # ============================================================================
    # Feed Items (News, Advisories)
    # ============================================================================
    
    @abstractmethod
    def get_feed_items(
        self,
        source_key: str,
        city_key: Optional[str] = None,
        since_hours: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> List[FeedItem]:
        """
        Get feed items from a source, optionally filtered by city and time.
        
        Args:
            source_key: Source identifier (e.g., "local10_miami")
            city_key: Optional city filter
            since_hours: Only return items from the last N hours
            limit: Maximum number of items to return
            
        Returns:
            List of FeedItem objects, newest first
        """
        pass
    
    @abstractmethod
    def upsert_feed_items(self, items: List[FeedItem]) -> int:
        """
        Insert or update feed items. Deduplicates by (source_key, guid) or (source_key, url).
        
        Args:
            items: List of FeedItem objects to upsert
            
        Returns:
            Number of items inserted/updated
        """
        pass
    
    # ============================================================================
    # City Context (Hospitals, Infrastructure)
    # ============================================================================
    
    @abstractmethod
    def get_city_context(self, city_key: str) -> Optional[CityContext]:
        """
        Get cached city context (hospitals, urgent care, etc.).
        
        Args:
            city_key: City identifier (e.g., "miami")
            
        Returns:
            CityContext if available, None otherwise
        """
        pass
    
    @abstractmethod
    def upsert_city_context(self, context: CityContext) -> bool:
        """
        Insert or update city context snapshot.
        
        Args:
            context: CityContext object
            
        Returns:
            True if successful
        """
        pass
    
    # ============================================================================
    # Transit Snapshots
    # ============================================================================
    
    @abstractmethod
    def get_transit_snapshot(self, city_key: str) -> Optional[TransitSnapshot]:
        """
        Get cached transit information.
        
        Args:
            city_key: City identifier
            
        Returns:
            TransitSnapshot if available, None otherwise
        """
        pass
    
    @abstractmethod
    def upsert_transit_snapshot(self, snapshot: TransitSnapshot) -> bool:
        """
        Insert or update transit snapshot.
        
        Args:
            snapshot: TransitSnapshot object
            
        Returns:
            True if successful
        """
        pass
    
    # ============================================================================
    # Curated Places (Must-Include Facilities)
    # ============================================================================
    
    @abstractmethod
    def get_curated_places(self, city_key: str, category: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get curated must-include places for a city.
        
        Args:
            city_key: City identifier
            category: Optional category filter (e.g., "hospital", "urgent_care")
            
        Returns:
            List of curated place dictionaries
        """
        pass
    
    @abstractmethod
    def get_curated_places_all(self, category: Optional[str] = None) -> List[Dict[str, Any]]:
        """Fetch curated places across all cities (must_include only)."""
        raise NotImplementedError
    
    # ============================================================================
    # Utility Methods
    # ============================================================================
    
    @abstractmethod
    def is_city_context_fresh(self, city_key: str, max_age_days: int = 30) -> bool:
        """
        Check if city context is recent enough.
        
        Args:
            city_key: City identifier
            max_age_days: Maximum age in days
            
        Returns:
            True if context exists and is fresh
        """
        pass
    
    @abstractmethod
    def is_feed_fresh(self, source_key: str, max_age_hours: int = 24) -> bool:
        """
        Check if feed cache is recent enough.
        
        Args:
            source_key: Source identifier
            max_age_hours: Maximum age in hours
            
        Returns:
            True if cache exists and is fresh
        """
        pass
    
    @abstractmethod
    def purge_old_feed_items(self, days: int, city_key: Optional[str] = None) -> int:
        """
        Delete feed items older than specified number of days.
        
        Args:
            days: Delete items older than this many days
            city_key: Optional city filter (only purge items for this city)
            
        Returns:
            Number of items deleted
        """
        pass
    
    # ============================================================================
    # City Reports (Generated Intelligence Briefs)
    # ============================================================================
    
    @abstractmethod
    def upsert_city_report(
        self,
        city_key: str,
        report_data: Dict[str, Any],
        trend_data: Optional[Dict[str, Any]],
        model: str,
        window_hours: int,
        pdf_file_url: Optional[str] = None,
        updated_by: Optional[str] = None,
    ) -> bool:
        """
        Save or update the latest AI-generated report for a city.
        Replaces previous report (upsert).
        
        Args:
            city_key: City identifier
            report_data: Full report JSON (narrative sections, etc.)
            trend_data: Optional trend statistics JSON
            model: AI model used (e.g., "gpt-4o-mini")
            window_hours: Report time window (e.g., 168)
            pdf_file_url: Optional URL to generated PDF
            updated_by: Optional user/system identifier
            
        Returns:
            True if successful
        """
        pass
    
    @abstractmethod
    def get_city_report(self, city_key: str) -> Optional[Dict[str, Any]]:
        """
        Get the latest report for a city.
        
        Args:
            city_key: City identifier
            
        Returns:
            Report dict or None
        """
        pass
