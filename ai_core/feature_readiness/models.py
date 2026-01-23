"""
Models for feature readiness checks.

This module provides standalone models that don't depend on the codebase.
For the standalone base class, use base_standalone.py instead.
"""

# Re-export from standalone for convenience
from .base_standalone import (
    CheckResult,
    CheckStatus,
    FeatureIssueSeverity,
    calculate_readiness_score,
    get_readiness_status,
)

__all__ = [
    "CheckResult",
    "CheckStatus",
    "FeatureIssueSeverity",
    "calculate_readiness_score",
    "get_readiness_status",
]
