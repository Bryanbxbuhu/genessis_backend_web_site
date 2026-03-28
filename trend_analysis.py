"""
Trend Analysis Module - Compute incident trends and risk thresholds.

Provides:
- Daily counts and 7-day moving averages
- Per-capita rates (per 100k population)
- Percentile-based risk thresholds (P60/P90 for MED/HIGH)
"""

from typing import Dict, List


def compute_moving_average(data: List[float], window: int = 7) -> List[float]:
    """
    Compute centered moving average.
    
    Args:
        data: List of values
        window: Window size for MA
        
    Returns:
        List of MA values (same length as input, padded with None for edges)
    """
    if len(data) < window:
        return data  # Not enough data for MA
    
    result = []
    half_window = window // 2
    
    for i in range(len(data)):
        if i < half_window or i >= len(data) - half_window:
            # Edge case: not enough data for centered window
            result.append(data[i])
        else:
            # Centered window
            window_data = data[i - half_window:i + half_window + 1]
            result.append(sum(window_data) / len(window_data))
    
    return result


def compute_per_capita_rate(count: int, population: int, per: int = 100000) -> float:
    """
    Compute per-capita rate.
    
    Args:
        count: Incident count
        population: City population
        per: Per X population (default 100k)
        
    Returns:
        Rate per population unit
    """
    if population <= 0:
        return 0.0
    return (count / population) * per


def compute_percentile_thresholds(
    data: List[float],
    p_medium: int = 60,
    p_high: int = 90,
) -> Dict[str, float]:
    """
    Compute percentile-based thresholds for risk levels.
    
    Args:
        data: List of daily rates
        p_medium: Percentile for MEDIUM threshold (default 60)
        p_high: Percentile for HIGH threshold (default 90)
        
    Returns:
        Dict with "low", "medium", "high" thresholds
    """
    if not data or len(data) < 2:
        # Not enough data - return default thresholds
        return {"low": 0.0, "medium": 1.0, "high": 2.0}
    
    # Filter out None/NaN values
    clean_data = [x for x in data if x is not None and not (isinstance(x, float) and x != x)]
    
    if len(clean_data) < 2:
        return {"low": 0.0, "medium": 1.0, "high": 2.0}
    
    # Compute percentiles
    try:
        sorted_data = sorted(clean_data)
        n = len(sorted_data)
        
        # P60 threshold for MEDIUM
        p_med_idx = int(n * p_medium / 100)
        threshold_medium = sorted_data[min(p_med_idx, n - 1)]
        
        # P90 threshold for HIGH
        p_high_idx = int(n * p_high / 100)
        threshold_high = sorted_data[min(p_high_idx, n - 1)]
        
        return {
            "low": 0.0,
            "medium": threshold_medium,
            "high": threshold_high,
        }
    except Exception as e:
        print(f"Warning: Failed to compute percentiles: {e}")
        return {"low": 0.0, "medium": 1.0, "high": 2.0}


def classify_risk_level(value: float, thresholds: Dict[str, float]) -> str:
    """
    Classify a value into LOW/MEDIUM/HIGH based on thresholds.
    
    Args:
        value: Value to classify
        thresholds: Dict with "low", "medium", "high" thresholds
        
    Returns:
        "low", "med", or "high"
    """
    if value <= thresholds["medium"]:
        return "low"
    elif value <= thresholds["high"]:
        return "med"
    else:
        return "high"


def compute_trend_statistics(
    daily_buckets: List[Dict],
    population: int,
    window: int = 7,
    p_medium: int = 60,
    p_high: int = 90,
) -> Dict:
    """
    Compute comprehensive trend statistics from daily buckets.
    
    Args:
        daily_buckets: List of {"date": "2025-01-01", "count": 5} dicts
        population: City population for per-capita rates
        window: Moving average window (default 7 days)
        p_medium: Percentile for MEDIUM threshold (default 60)
        p_high: Percentile for HIGH threshold (default 90)
        
    Returns:
        Dict with:
        {
            "dates": ["2025-01-01", ...],
            "counts": [5, 3, 0, ...],
            "moving_average": [4.2, 3.8, ...],
            "per_capita_rates": [1.13, 0.68, ...],  # per 100k
            "thresholds": {"low": 0.0, "medium": 0.8, "high": 1.5},
            "current_rate": 1.13,
            "current_level": "high",
            "trend_direction": "increasing",  # or "stable", "decreasing"
        }
    """
    if not daily_buckets:
        return {
            "dates": [],
            "counts": [],
            "moving_average": [],
            "per_capita_rates": [],
            "thresholds": {"low": 0.0, "medium": 0.0, "high": 0.0},
            "current_rate": 0.0,
            "current_level": "low",
            "trend_direction": "stable",
        }
    
    # Extract data
    dates = [bucket["date"] for bucket in daily_buckets]
    counts = [bucket["count"] for bucket in daily_buckets]
    
    # Compute moving average
    ma = compute_moving_average(counts, window=window)
    
    # Compute per-capita rates
    per_capita_rates = [compute_per_capita_rate(c, population) for c in counts]
    
    # Compute percentile thresholds from historical data
    thresholds = compute_percentile_thresholds(per_capita_rates, p_medium=p_medium, p_high=p_high)
    
    # Current rate (most recent day)
    current_rate = per_capita_rates[-1] if per_capita_rates else 0.0
    current_level = classify_risk_level(current_rate, thresholds)
    
    # Trend direction (compare last 7 days to previous 7 days)
    trend_direction = "stable"
    if len(counts) >= 14:
        recent_avg = sum(counts[-7:]) / 7
        previous_avg = sum(counts[-14:-7]) / 7
        
        if recent_avg > previous_avg * 1.2:
            trend_direction = "increasing"
        elif recent_avg < previous_avg * 0.8:
            trend_direction = "decreasing"
    
    return {
        "dates": dates,
        "counts": counts,
        "moving_average": ma,
        "per_capita_rates": per_capita_rates,
        "thresholds": thresholds,
        "current_rate": current_rate,
        "current_level": current_level,
        "trend_direction": trend_direction,
    }
