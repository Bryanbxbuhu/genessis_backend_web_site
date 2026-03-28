"""
Chart generation utilities for intelligence reports.
Creates matplotlib visualizations for incidents and trends.
"""

from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict

import matplotlib.pyplot as plt
import matplotlib.dates as mdates

from report_schema import Incident
from design_tokens import SIGNAL_CHART_STYLE


def create_combined_trends_chart(
    trend_buckets: Dict[str, Dict],
    output_path: Path,
) -> Path:
    """
    Generate a single combined chart showing all 4 risk category trends.
    
    Args:
        trend_buckets: Dict with keys 'crime', 'terrorism', 'traffic', 'health' containing trend data
        output_path: Path to save chart
        
    Returns:
        Path to saved chart
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Create larger chart for 4 categories
    fig, ax = plt.subplots(figsize=(12, 5), facecolor='white')
    
    # Category colors
    category_colors = {
        'crime': '#ef4444',      # Red
        'terrorism': '#f59e0b',  # Orange
        'traffic': '#3b82f6',    # Blue
        'health': '#10b981',     # Green
    }
    
    category_labels = {
        'crime': 'Crime',
        'terrorism': 'Terrorism',
        'traffic': 'Traffic',
        'health': 'Health',
    }
    
    has_data = False
    
    for category_key in ['crime', 'terrorism', 'traffic', 'health']:
        if category_key in trend_buckets and trend_buckets[category_key]:
            bucket_data = trend_buckets[category_key]
            
            # Extract dates and counts from list of dicts
            dates = []
            counts = []
            for entry in bucket_data:
                try:
                    date_str = entry.get("date")
                    count = entry.get("count", 0)
                    date_obj = datetime.fromisoformat(date_str).date()
                    dates.append(date_obj)
                    counts.append(count)
                except:
                    continue
            
            if dates and counts:
                has_data = True
                # Plot line for this category
                ax.plot(dates, counts,
                       color=category_colors[category_key],
                       linewidth=2.0,
                       label=category_labels[category_key],
                       marker='o',
                       markersize=3,
                       alpha=0.8)
    
    if not has_data:
        ax.text(0.5, 0.5,
               "No trend data available",
               ha='center', va='center',
               fontsize=12, color=SIGNAL_CHART_STYLE["muted_text"])
        ax.axis('off')
    else:
        # Styling
        ax.set_xlabel("Date", fontsize=10, color=SIGNAL_CHART_STYLE["text"])
        ax.set_ylabel("Incident Count", fontsize=10, color=SIGNAL_CHART_STYLE["text"])
        ax.set_title("30-Day Risk Trends by Category", fontsize=12, fontweight='600', 
                    color=SIGNAL_CHART_STYLE["text"], pad=15)
        
        # Format x-axis
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
        ax.xaxis.set_major_locator(mdates.DayLocator(interval=5))
        plt.xticks(rotation=0, fontsize=9)
        
        # Grid
        ax.grid(True, alpha=0.15, color=SIGNAL_CHART_STYLE["grid"], linewidth=0.5)
        ax.set_facecolor('white')
        
        # Legend
        ax.legend(loc='upper left', frameon=True, fancybox=False, 
                 shadow=False, fontsize=9, edgecolor=SIGNAL_CHART_STYLE["grid"])
        
        # Spines
        for spine in ax.spines.values():
            spine.set_color(SIGNAL_CHART_STYLE["grid"])
            spine.set_linewidth(0.5)
        
        # Tick colors
        ax.tick_params(colors=SIGNAL_CHART_STYLE["muted_text"], labelsize=9)
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight', facecolor='white')
    plt.close(fig)
    
    return output_path


def create_incidents_chart(incidents: List[Incident], output_path: Path) -> Path:
    """
    Generate a minimalist Signal-style timeline chart showing incident trends.
    
    Args:
        incidents: List of incident objects
        output_path: Path to save chart image
        
    Returns:
        Path to saved chart image
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    if not incidents:
        # Create empty chart with message
        fig, ax = plt.subplots(figsize=(8, 4), facecolor='white')
        ax.text(
            0.5, 0.5, 
            "No incidents in reporting window",
            ha='center', va='center',
            fontsize=12, color=SIGNAL_CHART_STYLE["muted_text"]
        )
        ax.axis('off')
    else:
        # Parse dates and count total incidents per day
        today = datetime.now()
        incidents_by_date = defaultdict(int)
        
        for inc in incidents:
            # Parse recency to actual date
            if inc.recency:
                recency_lower = inc.recency.lower()
                if 'today' in recency_lower:
                    date = today
                elif 'd ago' in recency_lower:
                    days = int(recency_lower.replace('d ago', '').strip())
                    date = today - timedelta(days=days)
                elif 'in ' in recency_lower:
                    # Future date (e.g., "in 3d", "in 1w")
                    if 'd' in recency_lower:
                        days = int(recency_lower.replace('in', '').replace('d', '').strip())
                        date = today + timedelta(days=days)
                    elif 'w' in recency_lower:
                        weeks = int(recency_lower.replace('in', '').replace('w', '').strip())
                        date = today + timedelta(weeks=weeks)
                    else:
                        date = today
                else:
                    # Fallback: try to parse date field
                    try:
                        date = datetime.strptime(inc.date, '%b %d')
                        date = date.replace(year=today.year)
                    except:
                        date = today
            else:
                # Try to parse date field
                try:
                    date = datetime.strptime(inc.date, '%b %d')
                    date = date.replace(year=today.year)
                except:
                    date = today
            
            date_key = date.date()
            incidents_by_date[date_key] += 1
        
        # Get all dates in range (last 7 days or span of incidents)
        if incidents_by_date:
            min_date = min(incidents_by_date.keys())
            max_date = max(incidents_by_date.keys())
            date_range = (max_date - min_date).days + 1
            
            # Use at least 7 days for better visualization
            if date_range < 7:
                min_date = max_date - timedelta(days=6)
        else:
            max_date = today.date()
            min_date = max_date - timedelta(days=6)
        
        # Create date list and count data
        dates = [min_date + timedelta(days=x) for x in range((max_date - min_date).days + 1)]
        counts = [incidents_by_date.get(d, 0) for d in dates]
        
        # Create minimalist chart with widescreen aspect ratio
        fig, ax = plt.subplots(figsize=(10, 3.5), facecolor='white')
        
        # Plot smooth line with rounded caps
        ax.plot(dates, counts, 
                color=SIGNAL_CHART_STYLE["accent"], 
                linewidth=SIGNAL_CHART_STYLE["line_width"],
                solid_capstyle='round',
                solid_joinstyle='round',
                zorder=2)
        
        # Add light area fill under the line
        ax.fill_between(dates, counts, 
                        color=SIGNAL_CHART_STYLE["accent"], 
                        alpha=SIGNAL_CHART_STYLE["fill_alpha"],
                        zorder=1)
        
        # Highlight most recent point with a dot
        if dates and counts:
            latest_date = dates[-1]
            latest_count = counts[-1]
            
            # Larger dot for the most recent point
            ax.scatter([latest_date], [latest_count], 
                      color=SIGNAL_CHART_STYLE["accent"],
                      s=80,  # Dot size
                      zorder=3,
                      edgecolor='white',
                      linewidth=2)
            
            # Display latest value as a small label above the dot
            if latest_count > 0:
                ax.annotate(f'{latest_count}',
                           xy=(latest_date, latest_count),
                           xytext=(0, 8),  # 8 points above
                           textcoords='offset points',
                           ha='center',
                           fontsize=10,
                           fontweight='bold',
                           color=SIGNAL_CHART_STYLE["text"],
                           bbox=dict(boxstyle='round,pad=0.4', 
                                   facecolor='white', 
                                   edgecolor=SIGNAL_CHART_STYLE["accent"],
                                   linewidth=1.5,
                                   alpha=0.95))
        
        # White background, no border box
        ax.set_facecolor('white')
        fig.patch.set_facecolor('white')
        
        # Hide top and right spines
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        
        # Very subtle left and bottom spines
        ax.spines['left'].set_color(SIGNAL_CHART_STYLE["spine_color"])
        ax.spines['bottom'].set_color(SIGNAL_CHART_STYLE["spine_color"])
        ax.spines['left'].set_linewidth(SIGNAL_CHART_STYLE["spine_width"])
        ax.spines['bottom'].set_linewidth(SIGNAL_CHART_STYLE["spine_width"])
        
        # Y-grid only (very light gray), no x-grid
        ax.grid(axis='y', 
                color=SIGNAL_CHART_STYLE["grid"], 
                alpha=SIGNAL_CHART_STYLE["grid_alpha"],
                linewidth=SIGNAL_CHART_STYLE["grid_width"],
                linestyle='-')
        ax.set_axisbelow(True)  # Grid behind data
        
        # Labels with dark text
        ax.set_xlabel('Date', fontsize=10, color=SIGNAL_CHART_STYLE["text"], fontweight='500')
        ax.set_ylabel('Count', fontsize=10, color=SIGNAL_CHART_STYLE["text"], fontweight='500')
        ax.set_title('Incident Trend (7-day window)', 
                    fontsize=11, 
                    fontweight='600', 
                    color=SIGNAL_CHART_STYLE["text"], 
                    pad=10,
                    loc='left')
        
        # Tick styling with muted text
        ax.tick_params(colors=SIGNAL_CHART_STYLE["muted_text"], labelsize=9)
        
        # Integer y-ticks only (no decimals)
        from matplotlib.ticker import MaxNLocator
        ax.yaxis.set_major_locator(MaxNLocator(integer=True, nbins=5))
        
        # Nicer date labels using ConciseDateFormatter
        ax.xaxis.set_major_formatter(mdates.ConciseDateFormatter(ax.xaxis.get_major_locator()))
        
        # Set y-axis to start at 0
        ax.set_ylim(bottom=0)
        
        # Add subtle padding to prevent clipping
        if counts and max(counts) > 0:
            y_max = max(counts)
            ax.set_ylim(0, y_max * 1.15)  # 15% padding above highest point
    
    # Save with tight layout
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight', facecolor='white')
    plt.close(fig)
    
    return output_path


def create_trend_chart(
    trend_stats: Dict,
    category: str,
    output_path: Path,
    show_threshold_labels: bool = True,
) -> Path:
    """
    Generate a trend chart with daily counts + 7-day moving average.
    
    Args:
        trend_stats: Dict from compute_trend_statistics() with dates, counts, moving_average, etc.
        category: Category name (e.g., "Crime", "Traffic")
        output_path: Path to save chart
        show_threshold_labels: Whether to show HIGH/MED/LOW threshold lines
        
    Returns:
        Path to saved chart
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    if not trend_stats.get("dates"):
        # Empty chart
        fig, ax = plt.subplots(figsize=(10, 3.5), facecolor='white')
        ax.text(
            0.5, 0.5,
            f"No {category.lower()} trend data available",
            ha='center', va='center',
            fontsize=12, color=SIGNAL_CHART_STYLE["muted_text"]
        )
        ax.axis('off')
        plt.savefig(output_path, dpi=150, bbox_inches='tight', facecolor='white')
        plt.close(fig)
        return output_path
    
    # Extract data
    dates = [datetime.fromisoformat(d).date() if isinstance(d, str) else d for d in trend_stats["dates"]]
    counts = trend_stats["counts"]
    ma = trend_stats.get("moving_average", counts)
    thresholds = trend_stats.get("thresholds", {})
    current_level = trend_stats.get("current_level", "low")
    
    # Create widescreen chart with Signal aesthetics
    fig, ax = plt.subplots(figsize=(10, 3.5), facecolor='white')
    
    # Plot daily counts (thin line)
    ax.plot(dates, counts,
            color=SIGNAL_CHART_STYLE["muted_text"],
            linewidth=1.0,
            alpha=0.6,
            label="Daily count",
            zorder=2)
    
    # Plot 7-day moving average (thicker line)
    ax.plot(dates, ma,
            color=SIGNAL_CHART_STYLE["accent"],
            linewidth=2.5,
            solid_capstyle='round',
            solid_joinstyle='round',
            label="7-day average",
            zorder=3)
    
    # Highlight latest point
    if counts:
        ax.scatter([dates[-1]], [counts[-1]],
                   color=SIGNAL_CHART_STYLE["accent"],
                   s=60,
                   zorder=4,
                   edgecolors='white',
                   linewidths=2)
        
        # Annotate latest value
        ax.annotate(
            f'{counts[-1]}',
            xy=(dates[-1], counts[-1]),
            xytext=(8, 8),
            textcoords='offset points',
            fontsize=9,
            fontweight='600',
            color=SIGNAL_CHART_STYLE["text"],
            bbox=dict(
                boxstyle='round,pad=0.4',
                facecolor='white',
                edgecolor=SIGNAL_CHART_STYLE["grid"],
                linewidth=1
            ),
            zorder=5
        )
    
    # Add threshold lines if requested
    if show_threshold_labels and thresholds:
        # Convert per-capita thresholds back to raw counts (approximate)
        # This is illustrative - actual impl may vary
        y_max = max(counts + ma) if counts else 1
        
        # HIGH threshold
        if thresholds.get("high", 0) > 0:
            ax.axhline(
                y=max(counts) * 0.8,  # Approximate placement
                color='#ef4444',
                linestyle='--',
                linewidth=1,
                alpha=0.3,
                zorder=1
            )
            ax.text(
                dates[0], max(counts) * 0.8,
                ' HIGH',
                fontsize=8,
                color='#ef4444',
                va='bottom',
                alpha=0.7
            )
    
    # Configure axes
    ax.set_xlabel('')
    ax.set_ylabel(f'{category} Incidents', fontsize=10, color=SIGNAL_CHART_STYLE["text"])
    ax.set_title(
        f'{category} Trend (Last 30 Days)',
        fontsize=11,
        fontweight='600',
        color=SIGNAL_CHART_STYLE["text"],
        pad=12,
        loc='left'
    )
    
    # Format x-axis dates
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=max(1, len(dates) // 6)))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=0, fontsize=9, color=SIGNAL_CHART_STYLE["muted_text"])
    
    # Format y-axis
    ax.tick_params(axis='y', labelsize=9, colors=SIGNAL_CHART_STYLE["muted_text"])
    
    # Minimal chrome: only Y-grid, left/bottom spines
    ax.grid(axis='y', color=SIGNAL_CHART_STYLE["grid"], linewidth=0.5, alpha=0.5, zorder=0)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_color(SIGNAL_CHART_STYLE["spine_color"])
    ax.spines['bottom'].set_color(SIGNAL_CHART_STYLE["spine_color"])
    ax.set_axisbelow(True)
    
    # Add legend
    ax.legend(loc='upper left', fontsize=8, frameon=False)
    
    # Set y-axis limits with padding
    if counts and max(counts) > 0:
        y_max = max(counts + ma)
        ax.set_ylim(0, y_max * 1.15)
    
    # Save chart
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight', facecolor='white')
    plt.close(fig)
    
    return output_path


def create_timeline_chart(incidents: List[Incident], output_path: Path) -> Path:
    """
    Generate a timeline visualization of incidents (future enhancement).
    
    Args:
        incidents: List of incident objects with date information
        output_path: Path to save chart image
        
    Returns:
        Path to saved chart image
    """
    # Placeholder for future timeline chart
    # Would parse incident.date and plot chronologically
    return create_incidents_chart(incidents, output_path)
