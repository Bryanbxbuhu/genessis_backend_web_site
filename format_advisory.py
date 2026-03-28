"""Helper to format travel advisory text by stripping HTML and normalizing whitespace."""

import re
import html


def format_advisory_text(input_text: str) -> str:
    """
    Convert HTML travel advisory text to clean, readable plain text.
    
    Args:
        input_text: Raw HTML string from advisory feed
        
    Returns:
        Clean plain text with bullets for list items and preserved paragraph breaks
        
    Examples:
        >>> format_advisory_text("<p>Exercise caution.</p><ul><li>Item 1</li></ul>")
        'Exercise caution.\\n\\n• Item 1'
    """
    if not input_text:
        return ""
    
    text = input_text
    
    # Convert list items to bullets
    text = re.sub(r'<li[^>]*>\s*', '• ', text, flags=re.IGNORECASE)
    text = re.sub(r'</li>', '\n', text, flags=re.IGNORECASE)
    
    # Convert paragraph breaks to newlines
    text = re.sub(r'</p>\s*<p[^>]*>', '\n\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</p>', '\n\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<p[^>]*>', ' ', text, flags=re.IGNORECASE)  # Add space before removing tag
    
    # Convert <br> tags to newlines
    text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
    
    # Strip all remaining HTML tags (add space to prevent word concatenation)
    text = re.sub(r'<[^>]+>', ' ', text)
    
    # Decode HTML entities (e.g., &nbsp;, &amp;)
    text = html.unescape(text)
    
    # Replace non-breaking spaces with regular spaces
    text = text.replace('\xa0', ' ')
    
    # Normalize whitespace
    # Replace multiple spaces with single space
    text = re.sub(r' +', ' ', text)
    # Replace 3+ newlines with 2 newlines (preserve paragraph breaks)
    text = re.sub(r'\n{3,}', '\n\n', text)
    # Remove leading/trailing whitespace from each line
    text = '\n'.join(line.strip() for line in text.split('\n'))
    
    # Trim overall
    text = text.strip()
    
    return text
