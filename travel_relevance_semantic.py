"""
Multilingual semantic relevance scoring for travel-related content.

Uses multilingual embeddings to score RSS feeds in any language without
maintaining per-language keyword lists. Works with 50+ languages out of the box.
"""
from __future__ import annotations

from functools import lru_cache
from typing import Tuple

from bs4 import BeautifulSoup
from sentence_transformers import SentenceTransformer, util

# Define travel-related intents and their query phrases
# These are used to compute semantic similarity with RSS items
INTENTS = [
    ("protest_unrest", "protest unrest demonstration riot violence civil unrest"),
    ("transit_strike", "strike work stoppage metro train bus transit disruption transport"),
    ("airport_disruption", "airport closure flight cancellations delays airspace disruption"),
    ("severe_weather", "severe weather storm flooding wildfire heatwave snow ice hurricane"),
    ("terrorism", "terror attack threat shooting explosion bomb"),
    ("health_emergency", "public health emergency outbreak contamination epidemic disease"),
]


def _clean_html(html: str) -> str:
    """Convert HTML to plain text using BeautifulSoup."""
    if not html:
        return ""
    try:
        soup = BeautifulSoup(html, "lxml")
        return " ".join(soup.get_text(" ", strip=True).split())
    except Exception:
        # Fallback: basic strip
        return html


@lru_cache(maxsize=1)
def _model() -> SentenceTransformer:
    """Load and cache the multilingual embedding model (once per run)."""
    import config
    model_name = config.SEMANTIC_MODEL_NAME
    return SentenceTransformer(model_name)


@lru_cache(maxsize=1)
def _intent_embeddings():
    """Compute and cache intent embeddings."""
    m = _model()
    texts = [q for _, q in INTENTS]
    return m.encode(texts, normalize_embeddings=True)


def score_semantic_relevance(title: str, html_summary: str) -> Tuple[float, str]:
    """
    Score an RSS item using multilingual semantic similarity.
    
    Args:
        title: RSS item title
        html_summary: RSS item description (may contain HTML)
    
    Returns:
        (score, reason) tuple where:
        - score: float in [0, 1] representing similarity to travel intents
        - reason: string explaining which intent matched best
    """
    # Clean text
    text = (title or "").strip() + "\n" + _clean_html(html_summary or "")
    text = text.strip()
    if not text:
        return 0.0, "empty"

    # Encode the item and compute similarity to intent queries
    m = _model()
    emb = m.encode(text, normalize_embeddings=True)
    intent_emb = _intent_embeddings()
    sims = util.cos_sim(emb, intent_emb)[0].tolist()

    # Find best matching intent
    best_idx = max(range(len(sims)), key=lambda i: sims[i])
    best_score = float(sims[best_idx])
    best_intent = INTENTS[best_idx][0]
    
    return best_score, f"semantic_match:{best_intent}"
