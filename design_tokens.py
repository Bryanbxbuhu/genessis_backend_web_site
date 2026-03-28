"""
Design tokens for consistent styling across PDF reports and charts.
Centralizes colors, fonts, and visual parameters.
"""

# Color palette for risk levels
RISK_COLORS = {
    "low": {
        "primary": "#22c55e",
        "background": "#f0fdf4",
        "border": "#86efac",
        "text": "#166534"
    },
    "med": {
        "primary": "#f59e0b",
        "background": "#fffbeb",
        "border": "#fcd34d",
        "text": "#92400e"
    },
    "high": {
        "primary": "#ef4444",
        "background": "#fef2f2",
        "border": "#fca5a5",
        "text": "#991b1b"
    }
}

# Incident type colors for charts
INCIDENT_COLORS = {
    "Crime": "#ef4444",        # Red
    "Weather": "#f59e0b",      # Orange
    "Transportation": "#3b82f6", # Blue
    "Health": "#8b5cf6",       # Purple
    "Other": "#6b7280"         # Gray
}

# Grayscale palette
GRAYS = {
    "50": "#f9fafb",
    "100": "#f3f4f6",
    "200": "#e5e7eb",
    "300": "#d1d5db",
    "400": "#9ca3af",
    "500": "#6b7280",
    "600": "#4b5563",
    "700": "#374151",
    "800": "#1f2937",
    "900": "#111827"
}

# Typography
FONTS = {
    "sans": '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Arial, sans-serif',
    "mono": 'ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace'
}

FONT_SIZES = {
    "xs": "8px",
    "sm": "9px",
    "base": "10.5px",
    "lg": "11px",
    "xl": "14px",
    "2xl": "16px",
    "3xl": "20px"
}

# Print-friendly monochrome palette (for optional use)
MONOCHROME = {
    "dark": "#111827",
    "medium": "#6b7280",
    "light": "#d1d5db",
    "bg": "#f9fafb"
}

# Chart styling for matplotlib
CHART_STYLE = {
    "figure_facecolor": "white",
    "axes_facecolor": "#f9fafb",
    "grid_color": "#e5e7eb",
    "grid_alpha": 0.5,
    "spine_color": "#d1d5db",
    "text_color": "#374151",
    "title_size": 11,
    "label_size": 9,
    "tick_size": 8,
    "line_width": 1.5,
    "alpha": 0.7
}

# Signal-style minimalist chart design
SIGNAL_CHART_STYLE = {
    "accent": "#3A76F0",        # Signal blue
    "grid": "#E5E7EB",          # Light gray grid
    "text": "#111827",          # Dark text
    "muted_text": "#6B7280",    # Muted gray text
    "fill_alpha": 0.15,         # Light area fill
    "line_width": 3.0,          # Smooth line width
    "spine_color": "#E5E7EB",   # Very subtle spines
    "spine_width": 0.5,         # Thin spines
    "grid_alpha": 0.4,          # Light grid
    "grid_width": 0.5,          # Thin grid lines
}
