import math
import re

def parse_bathrooms(b):
    """Convert bathroom counts into integers or floats if possible."""
    # Handle None or NaN
    if b is None:
        return None
    if isinstance(b, float) and math.isnan(b):
        return None

    # Handle numeric values
    if isinstance(b, (int, float)):
        return b

    # Handle strings
    text = str(b).lower()
    match = re.search(r"\d+(\.\d+)?", text)
    if match:
        return float(match.group())
    if "shared" in text:
        return 1  # treat shared as 1
    return None
