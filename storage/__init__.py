"""
Storage abstraction layer for OSINT Project.

Uses Supabase as the primary storage backend.
"""

from .base import DataStore, FeedItem, CityContext, TransitSnapshot
from .normalize import normalize_domain, is_aggregator_domain
from .supabase_store import SupabaseStore

__all__ = [
    "DataStore",
    "FeedItem", 
    "CityContext",
    "TransitSnapshot",
    "normalize_domain",
    "is_aggregator_domain",
    "get_datastore",
]


def get_datastore() -> DataStore:
    """
    Factory function to get the Supabase DataStore implementation.
    
    Returns:
        SupabaseStore connected to your Supabase project
    """
    return SupabaseStore()
