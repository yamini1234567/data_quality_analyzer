"""
Standalone base class for feature readiness checks.

This is a completely independent base class that doesn't rely on any
codebase-specific dependencies. Interns can use this to implement
feature readiness checks without understanding the full codebase.

Usage:
    class MyFeatureCheck(BaseFeatureReadinessCheck):
        feature_name = "my_feature"
        
        async def run_checks(self, source_name: str, payer: Optional[str] = None):
            # Your implementation here
            # Access data however you need (MongoDB, API, etc.)
            pass
"""

from abc import ABC, abstractmethod
from datetime import datetime, timezone
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


# ============================================================================
# Enums and Models (Standalone)
# ============================================================================

class FeatureIssueSeverity(str, Enum):
    """Severity level of a failed check."""
    critical = "critical"  # Feature cannot run (0% readiness)
    high = "high"          # Significant limitations (< 50% readiness)
    medium = "medium"      # Some limitations (50-80% readiness)
    low = "low"            # Minor issues (80-100% readiness)


class CheckStatus(str, Enum):
    """Status of a check."""
    passed = "passed"
    failed = "failed"


class CheckResult(BaseModel):
    """
    Result of a single readiness check.
    
    This is a standalone model that doesn't depend on any codebase.
    """
    module: str = Field(description="The module this check belongs to (e.g., 'ais')")
    key: str = Field(description="Unique identifier for the check (e.g., 'ais.charge_analysis.cpt_diversity')")
    name: str = Field(description="Human-readable name of the check")
    description: str = Field(description="What this check validates and the result")
    status: CheckStatus = Field(description="Whether the check passed or failed")
    severity: Optional[FeatureIssueSeverity] = Field(
        None, 
        description="Severity if check failed (only set if status is 'failed')"
    )
    solution: Optional[str] = Field(
        None, 
        description="Suggested action if check failed (only set if status is 'failed')"
    )
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="When this check was run"
    )
    
    def get_key(self) -> str:
        """Get the unique key for this check."""
        return self.key


# ============================================================================
# Base Check Class (Standalone)
# ============================================================================

class BaseFeatureReadinessCheck(ABC):
    """
    Standalone base class for feature readiness checks.
    
    This class is completely independent and doesn't require any codebase
    dependencies. Interns can extend this to create feature-specific checks.
    
    Features:
    - Provides structure for implementing checks
    - Helper methods for creating CheckResult objects
    - Error handling wrapper
    - No dependencies on MongoDB, app settings, etc.
    
    Example:
        class ChargeAnalysisCheck(BaseFeatureReadinessCheck):
            feature_name = "charge_analysis"
            feature_module = "ais"
            
            async def run_checks(
                self, 
                source_name: str, 
                payer: Optional[str] = None
            ) -> list[CheckResult]:
                results = []
                
                # Your checks here
                # Access data however you need
                # Return CheckResult objects
                
                return results
    """
    
    # Override these in subclasses
    feature_name: str = "unknown"
    feature_module: str = "ais"  # Default to "ais" for AI suggestions
    
    def __init__(self):
        """
        Initialize the check.
        
        No dependencies required - interns can add their own initialization
        if needed (e.g., MongoDB client, API clients, etc.)
        """
        pass
    
    def create_check_result(
        self,
        key: str,
        name: str,
        description: str,
        status: CheckStatus,
        severity: Optional[FeatureIssueSeverity] = None,
        solution: Optional[str] = None,
    ) -> CheckResult:
        """
        Create a CheckResult with consistent formatting.
        
        This helper method automatically:
        - Prefixes the key with module.feature_name
        - Sets module name
        - Only sets severity/solution if status is 'failed'
        
        Args:
            key: Unique identifier (e.g., "cpt_diversity")
                 Will become "ais.charge_analysis.cpt_diversity"
            name: Human-readable name (e.g., "CPT Code Diversity")
            description: What this check validates and the result
                         (e.g., "Found 15 unique CPT codes")
            status: passed or failed
            severity: Severity if failed (only set if status is 'failed')
            solution: Suggested action if failed (only set if status is 'failed')
            
        Returns:
            CheckResult instance
            
        Example:
            result = self.create_check_result(
                key="cpt_diversity",
                name="CPT Code Diversity",
                description="Found 15 unique CPT codes",
                status=CheckStatus.passed,
            )
        """
        full_key = f"{self.feature_module}.{self.feature_name}.{key}"
        
        # Only set severity and solution if check failed
        if status == CheckStatus.failed:
            return CheckResult(
                module=self.feature_module,
                key=full_key,
                name=name,
                description=description,
                status=status,
                severity=severity,
                solution=solution,
            )
        else:
            return CheckResult(
                module=self.feature_module,
                key=full_key,
                name=name,
                description=description,
                status=status,
                severity=None,
                solution=None,
            )
    
    @abstractmethod
    async def run_checks(
        self, 
        source_name: str, 
        payer: Optional[str] = None
    ) -> list[CheckResult]:
        """
        Run readiness checks for the feature.
        
        This method must be implemented by subclasses to perform
        feature-specific checks.
        
        Args:
            source_name: Name of the data source to check (e.g., "client1")
            payer: Optional payer name to check (if None, checks all payers)
            
        Returns:
            List of CheckResult objects
            
        Example:
            async def run_checks(
                self, 
                source_name: str, 
                payer: Optional[str] = None
            ) -> list[CheckResult]:
                results = []
                
                # Check 1: Data availability
                count = await self._count_claims(source_name, payer)
                if count >= 10:
                    results.append(self.create_check_result(
                        key="data_availability",
                        name="Data Availability",
                        description=f"Found {count} claims",
                        status=CheckStatus.passed,
                    ))
                else:
                    results.append(self.create_check_result(
                        key="data_availability",
                        name="Data Availability",
                        description=f"Only found {count} claims (need 10+)",
                        status=CheckStatus.failed,
                        severity=FeatureIssueSeverity.critical,
                        solution="Add more claims to the database",
                    ))
                
                return results
        """
        pass
    
    async def run(
        self, 
        source_name: str, 
        payer: Optional[str] = None
    ) -> list[CheckResult]:
        """
        Main entry point for running checks.
        
        This method wraps run_checks with error handling. Interns don't
        need to worry about exceptions - they'll be caught and returned
        as a failure result.
        
        Args:
            source_name: Name of the data source to check
            payer: Optional payer name to check
            
        Returns:
            List of CheckResult objects (always returns at least one result)
            
        Example:
            check = MyFeatureCheck()
            results = await check.run(source_name="client1", payer="Payer1")
            for result in results:
                print(f"{result.name}: {result.status}")
        """
        try:
            results = await self.run_checks(source_name, payer)
            
            # Log success (optional - interns can remove if they don't have logging)
            try:
                from loguru import logger
                logger.info(
                    f"Completed {len(results)} checks for {self.feature_name} "
                    f"(source: {source_name}, payer: {payer or 'all'})"
                )
            except ImportError:
                # No logging available - that's fine
                pass
            
            return results
            
        except Exception as e:
            # Return a failure result for the exception
            error_result = self.create_check_result(
                key="check_execution_failed",
                name=f"{self.feature_name} Check Execution",
                description=f"Failed to execute checks: {str(e)}",
                status=CheckStatus.failed,
                severity=FeatureIssueSeverity.critical,
                solution="Check logs for details and ensure data access is configured correctly",
            )
            
            # Log error (optional)
            try:
                from loguru import logger
                logger.exception(f"Error running checks for {self.feature_name}: {e}")
            except ImportError:
                pass
            
            return [error_result]


# ============================================================================
# Utility Functions (Optional Helpers)
# ============================================================================

def calculate_readiness_score(results: list[CheckResult]) -> float:
    """
    Calculate overall readiness score from check results.
    
    This is a standalone utility function that interns can use to
    calculate a readiness score (0-100%) from their check results.
    
    The score is weighted by severity:
    - Critical checks = 40% weight
    - High checks = 30% weight
    - Medium checks = 20% weight
    - Low checks = 10% weight
    
    Args:
        results: List of CheckResult objects
        
    Returns:
        Readiness score as a float (0.0 to 100.0)
        
    Example:
        results = await check.run(source_name="client1")
        score = calculate_readiness_score(results)
        print(f"Readiness: {score:.1f}%")
    """
    if not results:
        return 0.0
    
    # Weight by severity
    weights = {
        FeatureIssueSeverity.critical: 0.4,
        FeatureIssueSeverity.high: 0.3,
        FeatureIssueSeverity.medium: 0.2,
        FeatureIssueSeverity.low: 0.1,
    }
    
    total_weight = 0.0
    passed_weight = 0.0
    
    for result in results:
        # Determine weight based on severity (if failed) or default to medium
        if result.status == CheckStatus.failed and result.severity:
            weight = weights.get(result.severity, 0.2)
        else:
            # Passed checks get medium weight
            weight = 0.2
        
        total_weight += weight
        if result.status == CheckStatus.passed:
            passed_weight += weight
    
    if total_weight == 0:
        return 0.0
    
    return (passed_weight / total_weight) * 100.0


def get_readiness_status(score: float) -> str:
    """
    Get human-readable readiness status from score.
    
    Args:
        score: Readiness score (0.0 to 100.0)
        
    Returns:
        Status string: "Ready", "Partial", or "Not Ready"
        
    Example:
        score = calculate_readiness_score(results)
        status = get_readiness_status(score)
        print(f"Status: {status}")  # "Ready", "Partial", or "Not Ready"
    """
    if score >= 80:
        return "Ready"
    elif score >= 50:
        return "Partial"
    else:
        return "Not Ready"
