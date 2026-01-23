"""
Common Utility Functions
Shared across all features

Contains helper functions for:
- Percentage calculations
- Number formatting  
- Date/time operations
- Safe math operations
- String manipulation
- Validation helpers
"""

from datetime import datetime, timezone
from typing import Any

# DATE/TIME UTILITIES

def get_current_timestamp() -> datetime:
    
    return datetime.now(timezone.utc)


def format_timestamp(dt: datetime, format_str:  str = "%Y-%m-%d %H:%M:%S") -> str:
    return dt.strftime(format_str)

# PERCENTAGE UTILITIES

def calculate_percentage(part:  int, total: int, decimals: int = 2) -> float:
    if total == 0:
        return 0.0
    percentage = (part / total) * 100
    return round(percentage, decimals)


def format_percentage(value: float, decimals: int = 1) -> str:
    return f"{value:. {decimals}f}%"

# MATH UTILITIES

def safe_divide(numerator: float, denominator: float, default: float = 0.0) -> float:
    if denominator == 0:
        return default
    return numerator / denominator


def round_to_decimals(value: float, decimals: int = 2) -> float:
    return round(value, decimals)

def format_number(value: int) -> str:
    return f"{value: ,}"


def truncate_string(text: str, max_length: int = 50, suffix: str = "...") -> str:
    if len(text) <= max_length:
        return text
    return text[:max_length - len(suffix)] + suffix

# VALIDATION UTILITIES

def is_valid_percentage(value: float) -> bool:
    return 0 <= value <= 100


def is_empty_or_none(value: Any) -> bool:
    if value is None:   
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    if isinstance(value, (list, dict)) and len(value) == 0:
        return True
    return False

# EXPORTS

__all__ = [
    # Date/time
    'get_current_timestamp',
    'format_timestamp',
    
    # Percentages
    'calculate_percentage',
    'format_percentage',
    
    # Math
    'safe_divide',
    'round_to_decimals',
    
    # Strings
    'format_number',
    'truncate_string',
    
    # Validation
    'is_valid_percentage',
    'is_empty_or_none',
]