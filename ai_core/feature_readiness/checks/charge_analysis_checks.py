"""
Charge Analysis Feature Readiness Checks

Implements readiness checks for the Charge Analysis feature.   
Starting with app_settings validation.   
"""

from typing import Optional
import logging

from motor.motor_asyncio import AsyncIOMotorClient

from ai_core.feature_readiness.base_standalone import (
    BaseFeatureReadinessCheck,
    CheckResult,
    CheckStatus,
    FeatureIssueSeverity
)
from ai_core.feature_readiness.appsettings import MAppSettings


# Logging setup
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] %(name)s - %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)


class ChargeAnalysisReadinessCheck(BaseFeatureReadinessCheck):
    """
    Readiness checks for Charge Analysis feature
    """

    feature_name = "charge_analysis"
    feature_module = "ais"

    def __init__(
        self,
        mongo_client: AsyncIOMotorClient,
        database_name: str = "rcm_test_db",
        collection_name: str = "claims"
    ):
        super().__init__()

        self.client = mongo_client
        self.db = mongo_client[database_name]
        self. collection = self.db[collection_name]
        self.database_name = database_name

        self.app_settings = None
        self.stats_settings = None
        self.readiness_settings = None

    async def run_checks(
        self,
        source_name: str,
        payer:  Optional[str] = None
    ) -> list[CheckResult]: 

        logger.info("=" * 70)
        logger.info("CHARGE ANALYSIS READINESS CHECKS")
        logger.info("=" * 70)
        logger.info("Source: %s", source_name)
        logger.info("")

        results = []

        # CHECK 1: App Settings Validation

        logger.info("CHECK 1: App Settings Validation")
        logger.info("-" * 70)

        result = await self._check_app_settings_validation()
        results.append(result)

        logger.info("Status: %s", result.status. value. upper())
        logger.info(result.description)
        logger.info("")

        # If app_settings check fails critically, stop
        if result.status == CheckStatus.failed and result.severity == FeatureIssueSeverity.critical:
            logger.error("Critical failure in app_settings - stopping further checks")
            self._log_summary(results)
            return results

        # CHECK 2: Claims Data Analysis

        logger.info("CHECK 2: Claims Data Analysis")
        logger.info("-" * 70)

        result = await self._check_claims_data_analysis()
        results.append(result)

        logger.info("Status: %s", result.status.value.upper())
        logger.info(result.description)
        logger.info("")

        # CHECK 3: Historical Stats Availability

        logger.info("CHECK 3: Historical Stats Availability")
        logger.info("-" * 70)

        result = await self._check_historical_stats_availability()
        results.append(result)

        logger.info("Status: %s", result.status.value.upper())
        logger.info(result.description)
        logger.info("")

        self._log_summary(results)
        return results

    def _log_summary(self, results: list[CheckResult]):
        logger.info("=" * 70)
        logger.info("SUMMARY")
        logger.info("=" * 70)

        passed = sum(1 for r in results if r.status == CheckStatus.passed)
        failed = sum(1 for r in results if r.status == CheckStatus.failed)

        logger.info("Total Checks: %d", len(results))
        logger.info("Passed: %d", passed)
        logger.info("Failed: %d", failed)

        logger.info("=" * 70)

    # CHECK 1: App Settings Validation

    async def _check_app_settings_validation(self) -> CheckResult:
        """
        Check 1: App Settings Validation
        """

        validation_issues = []

        try: 

            # Step 1: Checking if the document exists

            logger.info("[1/5] Checking if app_settings document exists")

            try:
                self.app_settings = await MAppSettings.find_one()

                if not self.app_settings:
                    validation_issues.append("app_settings document not found")
                    logger.error("app_settings document NOT FOUND")
                else:
                    logger.info("app_settings document found")

            except Exception as e:
                validation_issues.append(f"Error querying app_settings: {str(e)}")
                logger. exception("Error querying app_settings")

            if validation_issues:
                return self.create_check_result(
                    key="app_settings_validation",
                    name="App Settings Validation",
                    description="; ".join(validation_issues),
                    status=CheckStatus.failed,
                    severity=FeatureIssueSeverity.critical,
                    solution="Create app_settings document in MongoDB"
                )

            # Step 2: Checking the presence of Stats and readiness settings

            logger.info("[2/5] Checking required sections")

            if not hasattr(self.app_settings, "stats_settings") or not self.app_settings.stats_settings:
                validation_issues.append("stats_settings missing")
                logger.error("stats_settings missing")
            else:
                self.stats_settings = self.app_settings.stats_settings
                logger.info("stats_settings present")

            if not hasattr(self.app_settings, "readiness_settings") or not self.app_settings.readiness_settings:
                validation_issues.append("readiness_settings missing")
                logger.error(" readiness_settings missing")
            else:
                self.readiness_settings = self.app_settings.readiness_settings
                logger.info("readiness_settings present")

            if validation_issues:
                return self. create_check_result(
                    key="app_settings_validation",
                    name="App Settings Validation",
                    description="; ".join(validation_issues),
                    status=CheckStatus. failed,
                    severity=FeatureIssueSeverity. critical,
                    solution="Fix missing sections in app_settings"
                )

            # Step 3: Checking the presence of required fields

            logger.info("[3/5] Checking required fields")

            if not getattr(self.stats_settings, "payer_field", None):
                validation_issues.append("payer_field missing or empty")
                logger.error(" payer_field missing")
            else:
                logger.info(" payer_field = %s", self.stats_settings.payer_field)

            if not hasattr(self.readiness_settings, "claims_with_charges_threshold"):
                validation_issues.append("claims_with_charges_threshold missing")
                logger.error(" claims_with_charges_threshold missing")
            else:
                logger.info(
                    " claims_with_charges_threshold = %s",
                    self.readiness_settings.claims_with_charges_threshold
                )

            if not hasattr(self.readiness_settings, "cpt_diversity_threshold"):
                validation_issues.append("cpt_diversity_threshold missing")
                logger.error(" cpt_diversity_threshold missing")
            else:
                logger.info(
                    " cpt_diversity_threshold = %s",
                    self.readiness_settings.cpt_diversity_threshold
                )

            if not hasattr(self.readiness_settings, "stats_coverage_threshold"):
                validation_issues.append("stats_coverage_threshold missing")
                logger.error(" stats_coverage_threshold missing")
            else:
                logger.info(
                    " stats_coverage_threshold = %s",
                    self.readiness_settings.stats_coverage_threshold
                )

            if not hasattr(self.readiness_settings, "stats_minimum_record_count"):
                validation_issues.append("stats_minimum_record_count missing")
                logger.error(" stats_minimum_record_count missing")
            else:
                logger. info(
                    " stats_minimum_record_count = %s",
                    self.readiness_settings.stats_minimum_record_count
                )

            if not hasattr(self.readiness_settings, "stats_minimum_avg_record_count"):
                validation_issues.append("stats_minimum_avg_record_count missing")
                logger.error(" stats_minimum_avg_record_count missing")
            else:
                logger.info(
                    "stats_minimum_avg_record_count = %s",
                    self.readiness_settings.stats_minimum_avg_record_count
                )

            if not hasattr(self.readiness_settings, "stats_minimum_cpts_per_payer"):
                validation_issues.append("stats_minimum_cpts_per_payer missing")
                logger.error(" stats_minimum_cpts_per_payer missing")
            else:
                logger.info(
                    " stats_minimum_cpts_per_payer = %s",
                    self.readiness_settings.stats_minimum_cpts_per_payer
                )

            if not hasattr(self.readiness_settings, "stats_maximum_staleness_days"):
                validation_issues.append("stats_maximum_staleness_days missing")
                logger.error(" stats_maximum_staleness_days missing")
            else:
                logger.info(
                    " stats_maximum_staleness_days = %s",
                    self. readiness_settings.stats_maximum_staleness_days
                )

            # Step 4: Validating threshold values

            logger.info("[4/5] Validating threshold values")

            if hasattr(self.readiness_settings, "claims_with_charges_threshold"):
                value = self.readiness_settings. claims_with_charges_threshold
                if value <= 0:
                    validation_issues.append("claims_with_charges_threshold must be > 0")
                    logger. error(" claims_with_charges_threshold invalid:  %s", value)

            if hasattr(self.readiness_settings, "cpt_diversity_threshold"):
                value = self.readiness_settings.cpt_diversity_threshold
                if value <= 0:
                    validation_issues.append("cpt_diversity_threshold must be > 0")
                    logger.error("cpt_diversity_threshold invalid: %s", value)

            # Check 3 threshold VALUE validations

            if hasattr(self.readiness_settings, "stats_coverage_threshold"):
                value = self.readiness_settings.stats_coverage_threshold
                if value <= 0 or value > 1:
                    validation_issues. append("stats_coverage_threshold must be between 0 and 1")
                    logger.error("stats_coverage_threshold invalid:  %s", value)

            if hasattr(self.readiness_settings, "stats_minimum_record_count"):
                value = self.readiness_settings.stats_minimum_record_count
                if value <= 0:
                    validation_issues.append("stats_minimum_record_count must be > 0")
                    logger. error("stats_minimum_record_count invalid: %s", value)

            if hasattr(self.readiness_settings, "stats_minimum_avg_record_count"):
                value = self.readiness_settings.stats_minimum_avg_record_count
                if value <= 0:
                    validation_issues.append("stats_minimum_avg_record_count must be > 0")
                    logger.error("stats_minimum_avg_record_count invalid: %s", value)

            if hasattr(self.readiness_settings, "stats_minimum_cpts_per_payer"):
                value = self.readiness_settings.stats_minimum_cpts_per_payer
                if value <= 0:
                    validation_issues. append("stats_minimum_cpts_per_payer must be > 0")
                    logger. error("stats_minimum_cpts_per_payer invalid: %s", value)

            if hasattr(self.readiness_settings, "stats_maximum_staleness_days"):
                value = self.readiness_settings.stats_maximum_staleness_days
                if value <= 0:
                    validation_issues. append("stats_maximum_staleness_days must be > 0")
                    logger.error("stats_maximum_staleness_days invalid: %s", value)

            # Step 5: Validation complete

            logger.info("[5/5] Validation complete")

            if validation_issues:
                return self.create_check_result(
                    key="app_settings_validation",
                    name="App Settings Validation",
                    description="; ".join(validation_issues),
                    status=CheckStatus.failed,
                    severity=FeatureIssueSeverity.critical,
                    solution="Fix invalid values in app_settings"
                )

            return self.create_check_result(
                key="app_settings_validation",
                name="App Settings Validation",
                description="All validations passed",
                status=CheckStatus.passed
            )

        except Exception as e:
            logger.exception("Unexpected error during app_settings validation")
            return self. create_check_result(
                key="app_settings_validation",
                name="App Settings Validation",
                description=str(e),
                status=CheckStatus.failed,
                severity=FeatureIssueSeverity.critical,
                solution="Check MongoDB connectivity and app_settings schema"
            )

    # CHECK 2: Claims Data Analysis

    async def _check_claims_data_analysis(self) -> CheckResult:
        """
        Check 2: Claims Data Analysis

        Validates that claims data has sufficient information for Charge Analysis: 
        - Minimum total claims volume
        - Claims with charges (CPT codes)
        - Claims with diagnoses
        - CPT code diversity

        Returns:
            CheckResult with PASSED or FAILED status
        """
        logger.info("=" * 70)
        logger.info("CHECK 2: Claims Data Analysis")
        logger.info("=" * 70)

        validation_issues = []
        metrics = {}

        try:

            # Step 1: Total Claims Volume

            logger.info("Checking total claims volume")

            total_claims = await self.collection.count_documents({})
            metrics["total_claims"] = total_claims

            min_total = self.readiness_settings.claims_minimum_total

            if total_claims == 0:
                logger.error("No claims found in collection")
                return self.create_check_result(
                    key="claims_data_analysis",
                    name="Claims Data Analysis",
                    description="Claims collection is empty",
                    status=CheckStatus.failed,
                    severity=FeatureIssueSeverity.critical,
                    solution="Import claims data into the collection"
                )

            if total_claims < min_total: 
                logger.warning("Found %d claims (threshold: %d)", total_claims, min_total)
                validation_issues.append(
                    f"Only {total_claims} claims found, need at least {min_total}"
                )
            else:
                logger.info("Found %d claims (threshold: %d)", total_claims, min_total)

            logger.info("")

            # Step 2: Claims with Charges

            logger.info("Checking claims with charges")

            claims_with_charges = await self.collection.count_documents({
                "charges": {
                    "$exists": True,
                    "$ne": [],
                    "$elemMatch": {
                        "cptHcpcs": {
                            "$exists":  True,
                            "$ne":  None,
                            "$ne": ""
                        }
                    }
                }
            })

            charges_percentage = (claims_with_charges / total_claims) * 100 if total_claims > 0 else 0
            metrics["claims_with_charges"] = claims_with_charges
            metrics["charges_percentage"] = round(charges_percentage, 2)

            # Compare actual % to threshold (80%)

            threshold_percentage = self.readiness_settings. claims_with_charges_percentage * 100

            logger.info("  Claims with valid charges: %d", claims_with_charges)

            if charges_percentage < (self.readiness_settings.claims_with_charges_percentage * 100):
                logger.warning("Coverage:  %.1f%% (threshold: %.1f%%)", charges_percentage, threshold_percentage)
                validation_issues.append(
                    f"Only {charges_percentage:.1f}% of claims have charges, need {threshold_percentage:.1f}%"
                )
            else:
                logger.info("Coverage: %.1f%% (threshold: %.1f%%)", charges_percentage, threshold_percentage)

            logger. info("")

            # Step 3: Claims with Diagnoses

            logger.info("Checking claims with diagnoses")

            claims_with_diagnoses = await self.collection.count_documents({
                "diagnoses": {
                    "$exists": True,
                    "$ne": [],
                    "$elemMatch": {
                        "code": {
                            "$exists":  True,
                            "$ne":  None,
                            "$ne":  ""
                        }
                    }
                }
            })

            diagnoses_percentage = (claims_with_diagnoses / total_claims) * 100 if total_claims > 0 else 0
            metrics["claims_with_diagnoses"] = claims_with_diagnoses
            metrics["diagnoses_percentage"] = round(diagnoses_percentage, 2)

            # Compare actual to the threshold 70%

            threshold_percentage = self.readiness_settings.claims_with_diagnoses_percentage * 100

            logger.info("  Claims with valid diagnoses: %d", claims_with_diagnoses)

            if diagnoses_percentage < (self.readiness_settings. claims_with_diagnoses_percentage * 100):
                logger. warning("Coverage: %.1f%% (threshold: %.1f%%)", diagnoses_percentage, threshold_percentage)
                validation_issues.append(
                    f"Only {diagnoses_percentage:.1f}% of claims have diagnoses, need {threshold_percentage:.1f}%"
                )
            else:
                logger.info("Coverage: %.1f%% (threshold: %.1f%%)", diagnoses_percentage, threshold_percentage)

            logger.info("")

            # Step 4: Eligible Claims (Both Charges AND Diagnoses)

            logger. info("Checking eligible claims (both charges and diagnoses)")

            eligible_claims = await self.collection.count_documents({
                "$and": [
                    {
                        "charges": {
                            "$exists": True,
                            "$ne": [],
                            "$elemMatch": {
                                "cptHcpcs": {
                                    "$exists": True,
                                    "$ne": None,
                                    "$ne": ""
                                }
                            }
                        }
                    },
                    {
                        "diagnoses": {
                            "$exists": True,
                            "$ne": [],
                            "$elemMatch": {
                                "code": {
                                    "$exists": True,
                                    "$ne": None,
                                    "$ne": ""
                                }
                            }
                        }
                    }
                ]
            })

            eligible_percentage = (eligible_claims / total_claims) * 100 if total_claims > 0 else 0
            metrics["eligible_claims"] = eligible_claims
            metrics["eligible_percentage"] = round(eligible_percentage, 2)

            logger.info(" Eligible claims: %d (%.1f%%)", eligible_claims, eligible_percentage)
            logger.info("Charge Analysis can run on %d claims", eligible_claims)

            logger.info("")

            # Step 5: CPT Code Diversity

            logger.info("Checking CPT code diversity")

            # Use aggregation to get distinct CPT codes
            pipeline = [
                {"$unwind": "$charges"},
                {"$match": {
                    "charges.cptHcpcs": {
                        "$exists": True,
                        "$ne": None,
                        "$ne": ""
                    }
                }},
                {"$group": {"_id": "$charges.cptHcpcs"}},
                {"$count": "unique_cpt_count"}
            ]

            result = await self.collection.aggregate(pipeline).to_list(1)
            unique_cpt_count = result[0]["unique_cpt_count"] if result else 0

            metrics["unique_cpt_codes"] = unique_cpt_count

            min_unique = self.readiness_settings.cpt_minimum_unique_codes

            if unique_cpt_count < min_unique:
                logger.warning("Found %d unique CPT codes (threshold: %d)", unique_cpt_count, min_unique)
                validation_issues.append(
                    f"Only {unique_cpt_count} unique CPT codes, need at least {min_unique}"
                )
            else:
                logger.info("Found %d unique CPT codes (threshold: %d)", unique_cpt_count, min_unique)

            logger.info("")

            # Final Result

            if validation_issues:
                # Determine severity based on issues
                severity = FeatureIssueSeverity.high
                if total_claims < min_total:
                    severity = FeatureIssueSeverity.critical

                description = "; ".join(validation_issues)
                solution = "Verify data import/population; check data quality; ensure charges and diagnoses are properly populated"

                logger.error("Status: FAILED")
                logger. error("Severity: %s", severity. value)
                logger.error("Description: %s", description)
                logger.info("")

                return self.create_check_result(
                    key="claims_data_analysis",
                    name="Claims Data Analysis",
                    description=description,
                    status=CheckStatus.failed,
                    severity=severity,
                    solution=solution
                )
            else:
                description = (
                    f"Charge Analysis can run on {eligible_claims} claims ({eligible_percentage:.1f}%) "
                    f"with {unique_cpt_count} unique CPT codes"
                )

                logger.info("Status: PASSED")
                logger.info("Description: %s", description)
                logger.info("")

                return self.create_check_result(
                    key="claims_data_analysis",
                    name="Claims Data Analysis",
                    description=description,
                    status=CheckStatus.passed
                )

        except Exception as e:
            logger.error("Error during claims data analysis:  %s", str(e))
            import traceback
            traceback.print_exc()

            return self.create_check_result(
                key="claims_data_analysis",
                name="Claims Data Analysis",
                description=f"Error analyzing claims data: {str(e)}",
                status=CheckStatus.failed,
                severity=FeatureIssueSeverity.critical,
                solution="Check logs for details; verify database connection and data structure"
            )

    # CHECK 3: Historical Stats Availability

    async def _check_historical_stats_availability(self) -> CheckResult:
        """
        Check 3: Historical Stats Availability

        Validates historical stats collection quality:
        - Stats collection exists and is populated
        - Good coverage (% CPT codes with stats)
        - Good quality (record_count >= threshold)
        - Per-payer distribution
        - Data freshness

        Returns:
            CheckResult with PASSED or FAILED status
        """
        logger.info("=" * 70)
        logger.info("CHECK 3: Historical Stats Availability")
        logger.info("=" * 70)

        validation_issues = []
        metrics = {}

        # Get stats collection
        
        stats_collection = self.db["charge_analysis_stats"]

        try: 

            # Step 1: Check if stats collection exists and is populated

            logger.info("Checking if stats collection exists")

            total_stats = await stats_collection.count_documents({})
            metrics["total_stats"] = total_stats

            if total_stats == 0:
                logger.error("Stats collection is empty")
                return self.create_check_result(
                    key="historical_stats_availability",
                    name="Historical Stats Availability",
                    description="Stats collection is empty",
                    status=CheckStatus.failed,
                    severity=FeatureIssueSeverity.critical,
                    solution="Generate stats collection from claims data using generate_stats_collection. py script"
                )

            logger. info("Found %d stat documents", total_stats)
            logger.info("")

            # Step 2: Check stats coverage (% of CPT codes with stats)

            logger.info("Checking stats coverage")

            # Get unique CPT codes from claims
            cpt_codes_in_claims = await self.collection. distinct("charges.cptHcpcs", {
                "charges.cptHcpcs": {"$exists": True, "$ne":  None, "$ne": ""}
            })

            # Get unique CPT codes from stats
            cpt_codes_in_stats = await stats_collection.distinct("cpt_code")

            total_cpt_codes = len(cpt_codes_in_claims)
            cpt_with_stats = len(cpt_codes_in_stats)
            coverage_percentage = (cpt_with_stats / total_cpt_codes * 100) if total_cpt_codes > 0 else 0

            metrics["total_cpt_codes_in_claims"] = total_cpt_codes
            metrics["cpt_codes_with_stats"] = cpt_with_stats
            metrics["coverage_percentage"] = round(coverage_percentage, 2)

            threshold_percentage = self.readiness_settings.stats_coverage_threshold * 100

            logger.info("     CPT codes in claims: %d", total_cpt_codes)
            logger.info("     CPT codes with stats: %d", cpt_with_stats)

            if coverage_percentage < (self.readiness_settings.stats_coverage_threshold * 100):
                logger.warning("Coverage: %.1f%% (threshold: %.1f%%)",
                              coverage_percentage, threshold_percentage)
                validation_issues.append(
                    f"Only {coverage_percentage:.1f}% of CPT codes have stats, need {threshold_percentage:.1f}%"
                )
            else:
                logger.info("Coverage: %.1f%% (threshold: %.1f%%)",
                           coverage_percentage, threshold_percentage)

            logger.info("")

            # Step 3: Check stats quality - sufficient record counts

            logger.info("Checking stats quality")

            # Part A:  Percentage of stats with sufficient records
            min_record_count = self.readiness_settings.stats_minimum_record_count

            sufficient_stats = await stats_collection.count_documents({
                "record_count": {"$gte": min_record_count}
            })

            quality_percentage = (sufficient_stats / total_stats * 100) if total_stats > 0 else 0
            metrics["sufficient_stats"] = sufficient_stats
            metrics["quality_percentage"] = round(quality_percentage, 2)

            logger.info("     Stats with record_count >= %d: %d", min_record_count, sufficient_stats)

            if quality_percentage < 50:
                logger.warning("Quality:  %.1f%% of stats have sufficient records (threshold: 50%%)",
                              quality_percentage)
                validation_issues.append(
                    f"Only {quality_percentage:.1f}% of stats have record_count >= {min_record_count}"
                )
            else:
                logger.info("Quality: %.1f%% of stats have sufficient records", quality_percentage)

            # Part B: Average record count
            avg_pipeline = [
                {"$group": {"_id": None, "avg_record_count": {"$avg": "$record_count"}}}
            ]
            avg_result = await stats_collection.aggregate(avg_pipeline).to_list(1)
            avg_record_count = avg_result[0]["avg_record_count"] if avg_result else 0

            metrics["avg_record_count"] = round(avg_record_count, 2)

            min_avg = self.readiness_settings.stats_minimum_avg_record_count

            logger.info("Average record count: %.1f", avg_record_count)

            if avg_record_count < min_avg:
                logger. warning(" Average record count %.1f is below threshold %.1f",
                              avg_record_count, min_avg)
                validation_issues.append(
                    f"Average record count is {avg_record_count:.1f}, need at least {min_avg}"
                )
            else:
                logger.info(" Average record count %.1f meets threshold %.1f",
                           avg_record_count, min_avg)

            logger.info("")

            # Step 4: Check per-payer distribution

            logger.info("Checking per-payer distribution")

            # Get CPT count per payer (only quality stats)
            payer_pipeline = [
                {"$match": {"record_count": {"$gte":  min_record_count}}},
                {"$group": {
                    "_id": "$payer",
                    "cpt_count": {"$sum":  1}
                }},
                {"$sort": {"cpt_count": -1}}
            ]

            payer_stats = await stats_collection.aggregate(payer_pipeline).to_list(None)

            min_cpts_per_payer = self. readiness_settings.stats_minimum_cpts_per_payer

            total_payers = len(payer_stats)
            payers_with_insufficient_coverage = []

            for payer_stat in payer_stats:
                payer_name = payer_stat["_id"]
                cpt_count = payer_stat["cpt_count"]

                if cpt_count < min_cpts_per_payer:
                    payers_with_insufficient_coverage. append(f"{payer_name} ({cpt_count} CPTs)")

            payers_with_sufficient = total_payers - len(payers_with_insufficient_coverage)

            metrics["total_payers"] = total_payers
            metrics["payers_with_sufficient_coverage"] = payers_with_sufficient
            metrics["payers_with_insufficient_coverage"] = len(payers_with_insufficient_coverage)

            logger.info("     Total payers: %d", total_payers)
            logger.info("     Payers with >= %d CPT codes: %d",
                       min_cpts_per_payer, payers_with_sufficient)

            if payers_with_insufficient_coverage:
                logger.warning("Payers with insufficient coverage: %d",
                              len(payers_with_insufficient_coverage))
                for payer_info in payers_with_insufficient_coverage[: 5]:  # Show first 5
                    logger.warning("        - %s", payer_info)

                validation_issues.append(
                    f"{len(payers_with_insufficient_coverage)} payers have < {min_cpts_per_payer} CPT codes with stats"
                )

                metrics["problematic_payers"] = payers_with_insufficient_coverage[: 10]  # Store first 10
            else:
                logger.info("All payers have sufficient CPT coverage")

            logger.info("")

            # Step 5: Check stats freshness

            logger.info("Checking stats freshness")

            # Find most recent update
            most_recent = await stats_collection.find_one(
                sort=[("last_updated", -1)]
            )

            if most_recent and "last_updated" in most_recent:
                from datetime import datetime, timezone

                most_recent_date = most_recent["last_updated"]

                # Ensure both datetimes are timezone-aware
                if most_recent_date.tzinfo is None:
                    # If naive, assume UTC
                    most_recent_date = most_recent_date.replace(tzinfo=timezone.utc)

                current_date = datetime.now(timezone.utc)

                # Calculate age in days
                age_days = (current_date - most_recent_date).days

                metrics["most_recent_update"] = most_recent_date. isoformat()
                metrics["age_days"] = age_days

                max_staleness = self.readiness_settings.stats_maximum_staleness_days

                logger.info("     Most recent update: %s", most_recent_date.strftime("%Y-%m-%d"))
                logger.info("     Age:  %d days", age_days)

                if age_days > max_staleness:
                    logger. warning("Stats are %d days old (threshold: %d days)",
                                  age_days, max_staleness)
                    validation_issues.append(
                        f"Stats are {age_days} days old, should be updated within {max_staleness} days"
                    )
                    metrics["is_fresh"] = False
                else: 
                    logger.info("Stats are fresh (within %d days)", max_staleness)
                    metrics["is_fresh"] = True
            else:
                logger.warning(" No last_updated timestamp found")
                metrics["is_fresh"] = None

            logger.info("")

            # Final Result

            if validation_issues:
                # Determine severity
                severity = FeatureIssueSeverity.high

                # Critical issues
                if total_stats == 0:
                    severity = FeatureIssueSeverity.critical
                elif coverage_percentage < 25 or quality_percentage < 25:
                    severity = FeatureIssueSeverity.critical

                # Medium issues (only payer distribution or freshness)
                elif len(validation_issues) == 1 and (
                    "payers" in validation_issues[0]. lower() or
                    "days old" in validation_issues[0]. lower()
                ):
                    severity = FeatureIssueSeverity.medium

                description = "; ".join(validation_issues)
                solution = "Consider regenerating stats or improving data quality; ensure all payers have sufficient historical data"

                logger.error("Status: FAILED")
                logger.error("Severity: %s", severity.value)
                logger.error("Description: %s", description)
                logger.info("")

                return self.create_check_result(
                    key="historical_stats_availability",
                    name="Historical Stats Availability",
                    description=description,
                    status=CheckStatus.failed,
                    severity=severity,
                    solution=solution
                )
            else:
                description = (
                    f"Stats ready:  {total_stats} documents, {coverage_percentage:.1f}% CPT coverage, "
                    f"avg {avg_record_count:.1f} records/stat"
                )

                logger.info("Status: PASSED")
                logger.info("Description: %s", description)
                logger.info("")

                return self.create_check_result(
                    key="historical_stats_availability",
                    name="Historical Stats Availability",
                    description=description,
                    status=CheckStatus. passed
                )

        except Exception as e:
            logger.error("Error during stats availability check: %s", str(e))
            import traceback
            traceback.print_exc()

            return self.create_check_result(
                key="historical_stats_availability",
                name="Historical Stats Availability",
                description=f"Error checking stats availability: {str(e)}",
                status=CheckStatus.failed,
                severity=FeatureIssueSeverity.critical,
                solution="Check logs for details; verify database connection and stats collection structure"
            )