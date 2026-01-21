"""
Feature Readiness Checks Module.

This module provides base classes and utilities for checking if AI features
have sufficient data to run. Interns can extend BaseFeatureReadinessCheck
to create feature-specific readiness checks.

For standalone (independent) base class, use:
    from ai_core.feature_readiness.base_standalone import BaseFeatureReadinessCheck

For codebase-integrated base class (with MongoDB, stats, etc.), use:
    from ai_core.feature_readiness.base import BaseFeatureReadinessCheck
"""

# Standalone base class (recommended for interns - no dependencies)
from .base_standalone import (
    BaseFeatureReadinessCheck,
    CheckResult,
    CheckStatus,
    FeatureIssueSeverity,
    calculate_readiness_score,
    get_readiness_status,
)

# Codebase-integrated base class (for advanced use)
#from .base import BaseFeatureReadinessCheck as BaseFeatureReadinessCheckIntegrated

__all__ = [
    # Standalone (default)
    "BaseFeatureReadinessCheck",
    "CheckResult",
    "CheckStatus",
    "FeatureIssueSeverity",
    "calculate_readiness_score",
    "get_readiness_status",
    # Integrated (advanced)
    "BaseFeatureReadinessCheckIntegrated",
]
