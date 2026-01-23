"""
Additional Charge Feature Readiness Checks

Implements readiness checks for the Additional Charge feature.
Validates that we have sufficient diagnosis-CPT pattern data and MCD coverage.
"""

from typing import Optional
from loguru import logger

from motor.motor_asyncio import AsyncIOMotorClient

from ai_core.feature_readiness.base_standalone import (
    BaseFeatureReadinessCheck,
    CheckResult,
    CheckStatus,
    FeatureIssueSeverity
)
from ai_core.feature_readiness.appsettings import MAppSettings


class AdditionalChargeReadinessCheck(BaseFeatureReadinessCheck):
    """
    Readiness checks for Additional Charge feature
    """

    feature_name = "additional_charge"
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
        self.collection = self.db[collection_name]
        self.database_name = database_name

        self.app_settings = None
        self.stats_settings = None
        self.readiness_settings = None
        
        # Default thresholds for Additional Charge
        self.diagnosis_cpt_min_combinations = 10
        self.diagnosis_cpt_record_count_threshold = 5
        self.diagnosis_diversity_threshold = 5
        self.claims_with_diagnoses_threshold = 10

    async def run_checks(
        self,
        source_name: str,
        payer: Optional[str] = None
    ) -> list[CheckResult]:

        logger.info("=" * 70)
        logger.info("ADDITIONAL CHARGE READINESS CHECKS")
        logger.info("=" * 70)
        logger.info(f"Source: {source_name}")
        if payer:
            logger.info(f"Payer: {payer}")
        logger.info("")

        results = []

        # CHECK 1: App Settings Validation

        logger.info("CHECK 1: App Settings Validation")
        logger.info("-" * 70)

        result = await self._check_app_settings_validation()
        results.append(result)

        logger.info(f"Status: {result.status.value.upper()}")
        logger.info(result.description)
        logger.info("")

        # If app_settings check fails critically, stop
        if result.status == CheckStatus.failed and result.severity == FeatureIssueSeverity.critical:
            logger.error("Critical failure in app_settings - stopping further checks")
            self._log_summary(results)
            return results

        # CHECK 2: Claims with Diagnoses

        logger.info("CHECK 2: Claims with Diagnoses")
        logger.info("-" * 70)

        result = await self._check_claims_with_diagnoses(source_name, payer)
        results.append(result)

        logger.info(f"Status: {result.status.value.upper()}")
        logger.info(result.description)
        logger.info("")

        # CHECK 3: Diagnosis Code Diversity

        logger.info("CHECK 3: Diagnosis Code Diversity")
        logger.info("-" * 70)

        result = await self._check_diagnosis_diversity(source_name, payer)
        results.append(result)

        logger.info(f"Status: {result.status.value.upper()}")
        logger.info(result.description)
        logger.info("")

        # CHECK 4: Diagnosis-CPT Pattern Stats

        logger.info("CHECK 4: Diagnosis-CPT Pattern Stats")
        logger.info("-" * 70)

        result = await self._check_diagnosis_cpt_patterns(source_name, payer)
        results.append(result)

        logger.info(f"Status: {result.status.value.upper()}")
        logger.info(result.description)
        logger.info("")

        # CHECK 5: Data Quality

        logger.info("CHECK 5: Data Quality")
        logger.info("-" * 70)

        result = await self._check_data_quality(source_name, payer)
        results.append(result)

        logger.info(f"Status: {result.status.value.upper()}")
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

        logger.info(f"Total Checks: {len(results)}")
        logger.info(f"Passed: {passed}")
        logger.info(f"Failed: {failed}")

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
                app_settings_doc = await self.db["app_settings"].find_one({})

                if not app_settings_doc:
                    validation_issues.append("app_settings document not found in database")
                else:
                    self.app_settings = MAppSettings(**app_settings_doc)
                    logger.info("✓ app_settings document found")

            except Exception as e:
                validation_issues.append(f"Error loading app_settings: {str(e)}")
                logger.error(f"✗ Error loading app_settings: {str(e)}")

            if validation_issues:
                return self.create_check_result(
                    key="app_settings_validation",
                    name="App Settings Validation",
                    description=f"Failed to load app_settings: {', '.join(validation_issues)}",
                    status=CheckStatus.failed,
                    severity=FeatureIssueSeverity.critical,
                    solution="Create app_settings document with required fields (stats_settings, readiness_settings)"
                )

            # Step 2: Checking the presence of Stats and readiness settings

            logger.info("[2/5] Checking required sections")

            if not hasattr(self.app_settings, "stats_settings") or not self.app_settings.stats_settings:
                validation_issues.append("stats_settings section missing")
                logger.error("✗ stats_settings section missing")
            else:
                self.stats_settings = self.app_settings.stats_settings
                logger.info("✓ stats_settings section found")

            if not hasattr(self.app_settings, "readiness_settings") or not self.app_settings.readiness_settings:
                validation_issues.append("readiness_settings section missing")
                logger.error("✗ readiness_settings section missing")
            else:
                self.readiness_settings = self.app_settings.readiness_settings
                logger.info("✓ readiness_settings section found")

            if validation_issues:
                return self.create_check_result(
                    key="app_settings_validation",
                    name="App Settings Validation",
                    description=f"Missing required sections: {', '.join(validation_issues)}",
                    status=CheckStatus.failed,
                    severity=FeatureIssueSeverity.critical,
                    solution="Add missing sections to app_settings document"
                )

            # Step 3: Checking the presence of required fields for Additional Charge

            logger.info("[3/5] Checking required fields for Additional Charge")

            if not getattr(self.stats_settings, "payer_field", None):
                validation_issues.append("payer_field missing in stats_settings")
                logger.error("✗ payer_field missing")
            else:
                logger.info(f"✓ payer_field: {self.stats_settings.payer_field}")

            # Check for diagnosis-CPT specific thresholds
            self.diagnosis_cpt_min_combinations = getattr(
                self.readiness_settings, 'diagnosis_cpt_min_combinations', 10
            )
            logger.info(f"✓ diagnosis_cpt_min_combinations: {self.diagnosis_cpt_min_combinations}")

            self.diagnosis_cpt_record_count_threshold = getattr(
                self.readiness_settings, 'diagnosis_cpt_record_count_threshold', 5
            )
            logger.info(f"✓ diagnosis_cpt_record_count_threshold: {self.diagnosis_cpt_record_count_threshold}")

            self.diagnosis_diversity_threshold = getattr(
                self.readiness_settings, 'diagnosis_diversity_threshold', 5
            )
            logger.info(f"✓ diagnosis_diversity_threshold: {self.diagnosis_diversity_threshold}")

            self.claims_with_diagnoses_threshold = getattr(
                self.readiness_settings, 'claims_with_diagnoses_threshold', 10
            )
            logger.info(f"✓ claims_with_diagnoses_threshold: {self.claims_with_diagnoses_threshold}")

            if validation_issues:
                return self.create_check_result(
                    key="app_settings_validation",
                    name="App Settings Validation",
                    description=f"Missing required fields: {', '.join(validation_issues)}",
                    status=CheckStatus.failed,
                    severity=FeatureIssueSeverity.high,
                    solution="Add missing fields to app_settings (payer_field, thresholds)"
                )

            # Step 4 & 5: Stats collection validation (similar to charge analysis)

            logger.info("[4/5] Checking stats collection configuration")
            logger.info("✓ Stats collection will be validated in CHECK 4")

            logger.info("[5/5] All app_settings validations passed")

            return self.create_check_result(
                key="app_settings_validation",
                name="App Settings Validation",
                description="All required app_settings are present and valid",
                status=CheckStatus.passed
            )

        except Exception as e:
            logger.exception("Unexpected error in app_settings validation")
            return self.create_check_result(
                key="app_settings_validation",
                name="App Settings Validation",
                description=f"Unexpected error: {str(e)}",
                status=CheckStatus.failed,
                severity=FeatureIssueSeverity.critical,
                solution="Check logs for detailed error information"
            )

    # CHECK 2: Claims with Diagnoses

    async def _check_claims_with_diagnoses(
        self,
        source_name: str,
        payer: Optional[str] = None
    ) -> CheckResult:
        """
        Check 2: Claims with Diagnoses
        
        Validates that we have claims with diagnosis codes:
        - Total claims with diagnoses
        - Claims with primary diagnosis
        - Average diagnoses per claim
        
        Returns:
            CheckResult with PASSED or FAILED status
        """
        logger.info("=" * 70)
        logger.info("CHECK 2: Claims with Diagnoses")
        logger.info("=" * 70)

        validation_issues = []
        metrics = {}

        try:
            payer_field = self.stats_settings.payer_field
            threshold = self.claims_with_diagnoses_threshold

            # Build query
            query = {}
            if payer:
                query[payer_field] = payer

            # Step 1: Count total claims with diagnoses
            logger.info("[1/4] Counting claims with diagnoses")

            query_with_diagnoses = {**query, "diagnoses": {"$exists": True, "$ne": []}}
            total_with_diagnoses = await self.collection.count_documents(query_with_diagnoses)

            metrics["total_with_diagnoses"] = total_with_diagnoses
            logger.info(f"✓ Claims with diagnoses: {total_with_diagnoses}")

            # Step 2: Count claims with primary diagnosis (order=1 or "1")
            logger.info("[2/4] Counting claims with primary diagnosis")

            query_with_primary = {
                **query,
                "diagnoses": {
                    "$elemMatch": {
                        "$or": [
                            {"order": 1},
                            {"order": "1"}
                        ],
                        "code": {"$exists": True, "$ne": None, "$ne": ""}
                    }
                }
            }
            total_with_primary = await self.collection.count_documents(query_with_primary)

            metrics["total_with_primary"] = total_with_primary
            logger.info(f"✓ Claims with primary diagnosis: {total_with_primary}")

            # Step 3: Calculate average diagnoses per claim
            logger.info("[3/4] Calculating average diagnoses per claim")

            pipeline = [
                {"$match": query_with_diagnoses},
                {
                    "$project": {
                        "diagnosis_count": {"$size": "$diagnoses"}
                    }
                },
                {
                    "$group": {
                        "_id": None,
                        "avg_diagnoses": {"$avg": "$diagnosis_count"}
                    }
                }
            ]

            result = await self.collection.aggregate(pipeline).to_list(length=1)
            avg_diagnoses = result[0]["avg_diagnoses"] if result else 0

            metrics["avg_diagnoses_per_claim"] = round(avg_diagnoses, 2)
            logger.info(f"✓ Average diagnoses per claim: {avg_diagnoses:.2f}")

            # Step 4: Validate thresholds
            logger.info("[4/4] Validating thresholds")

            if total_with_diagnoses < threshold:
                validation_issues.append(
                    f"Insufficient claims with diagnoses: {total_with_diagnoses} < {threshold}"
                )
                logger.error("✗ Insufficient claims with diagnoses")

            if total_with_primary < threshold:
                validation_issues.append(
                    f"Insufficient claims with primary diagnosis: {total_with_primary} < {threshold}"
                )
                logger.error("✗ Insufficient claims with primary diagnosis")

            if avg_diagnoses < 1.5:
                validation_issues.append(
                    f"Low diagnosis diversity per claim: {avg_diagnoses:.2f} < 1.5"
                )
                logger.warning("⚠ Low diagnosis diversity per claim")

            # Determine result
            if validation_issues:
                severity = FeatureIssueSeverity.critical if total_with_diagnoses < threshold else FeatureIssueSeverity.high
                
                return self.create_check_result(
                    key="claims_with_diagnoses",
                    name="Claims with Diagnoses",
                    description=f"Failed validation. Issues: {', '.join(validation_issues)}. Metrics: {metrics}",
                    status=CheckStatus.failed,
                    severity=severity,
                    solution=f"Ensure at least {threshold} claims have diagnoses. Import more claims with diagnosis codes."
                )

            return self.create_check_result(
                key="claims_with_diagnoses",
                name="Claims with Diagnoses",
                description=f"Sufficient claims with diagnoses found. Metrics: {metrics}",
                status=CheckStatus.passed
            )

        except Exception as e:
            logger.exception("Error in claims with diagnoses check")
            return self.create_check_result(
                key="claims_with_diagnoses",
                name="Claims with Diagnoses",
                description=f"Error during check: {str(e)}",
                status=CheckStatus.failed,
                severity=FeatureIssueSeverity.critical,
                solution="Check database connection and claim structure"
            )

    # CHECK 3: Diagnosis Code Diversity

    async def _check_diagnosis_diversity(
        self,
        source_name: str,
        payer: Optional[str] = None
    ) -> CheckResult:
        """
        Check 3: Diagnosis Code Diversity
        
        Validates we have enough unique diagnosis codes:
        - Total unique ICD-10 codes
        - Distribution across payers
        
        Returns:
            CheckResult with PASSED or FAILED status
        """
        logger.info("=" * 70)
        logger.info("CHECK 3: Diagnosis Code Diversity")
        logger.info("=" * 70)

        validation_issues = []
        metrics = {}

        try:
            payer_field = self.stats_settings.payer_field
            threshold = self.diagnosis_diversity_threshold

            # Build query
            query = {"diagnoses": {"$exists": True, "$ne": []}}
            if payer:
                query[payer_field] = payer

            # Step 1: Get unique diagnosis codes
            logger.info("[1/2] Getting unique diagnosis codes")

            pipeline = [
                {"$match": query},
                {"$unwind": "$diagnoses"},
                {
                    "$match": {
                        "diagnoses.code": {"$exists": True, "$ne": None, "$ne": ""}
                    }
                },
                {
                    "$group": {
                        "_id": "$diagnoses.code",
                        "count": {"$sum": 1}
                    }
                },
                {"$sort": {"count": -1}}
            ]

            diagnosis_codes = await self.collection.aggregate(pipeline).to_list(length=None)
            unique_diagnoses = len(diagnosis_codes)

            metrics["unique_diagnoses"] = unique_diagnoses
            logger.info(f"✓ Unique diagnosis codes: {unique_diagnoses}")

            # Step 2: Top diagnoses
            logger.info("[2/2] Identifying top diagnosis codes")

            top_diagnoses = diagnosis_codes[:10]
            metrics["top_diagnoses"] = [
                {"code": d["_id"], "count": d["count"]} for d in top_diagnoses
            ]

            logger.info("✓ Top 10 diagnoses:")
            for i, d in enumerate(top_diagnoses, 1):
                logger.info(f"  {i}. {d['_id']} (count: {d['count']})")

            # Validate threshold
            if unique_diagnoses < threshold:
                validation_issues.append(
                    f"Insufficient unique diagnosis codes: {unique_diagnoses} < {threshold}"
                )
                logger.error("✗ Insufficient diagnosis diversity")

            # Determine severity
            if validation_issues:
                if unique_diagnoses < threshold * 0.5:
                    severity = FeatureIssueSeverity.critical
                elif unique_diagnoses < threshold * 0.8:
                    severity = FeatureIssueSeverity.high
                else:
                    severity = FeatureIssueSeverity.medium

                return self.create_check_result(
                    key="diagnosis_diversity",
                    name="Diagnosis Code Diversity",
                    description=f"Low diagnosis diversity. Issues: {', '.join(validation_issues)}. Metrics: {metrics}",
                    status=CheckStatus.failed,
                    severity=severity,
                    solution=f"Ensure at least {threshold} unique diagnosis codes. Import more diverse claims."
                )

            return self.create_check_result(
                key="diagnosis_diversity",
                name="Diagnosis Code Diversity",
                description=f"Good diagnosis diversity. Metrics: {metrics}",
                status=CheckStatus.passed
            )

        except Exception as e:
            logger.exception("Error in diagnosis diversity check")
            return self.create_check_result(
                key="diagnosis_diversity",
                name="Diagnosis Code Diversity",
                description=f"Error during check: {str(e)}",
                status=CheckStatus.failed,
                severity=FeatureIssueSeverity.critical,
                solution="Check database connection and diagnoses field structure"
            )

    # CHECK 4: Diagnosis-CPT Pattern Stats

    async def _check_diagnosis_cpt_patterns(
        self,
        source_name: str,
        payer: Optional[str] = None
    ) -> CheckResult:
        """
        Check 4: Diagnosis-CPT Pattern Stats
        
        Validates historical stats for diagnosis-CPT combinations:
        - Total combinations with stats
        - Stats with sufficient record_count (>= 5 preferred, >= 3 minimum)
        - Stats with paid > 0
        
        Returns:
            CheckResult with PASSED or FAILED status
        """
        logger.info("=" * 70)
        logger.info("CHECK 4: Diagnosis-CPT Pattern Stats")
        logger.info("=" * 70)

        validation_issues = []
        metrics = {}

        try:
            payer_field = self.stats_settings.payer_field
            min_combinations = self.diagnosis_cpt_min_combinations
            record_count_threshold = self.diagnosis_cpt_record_count_threshold

            # Get stats collection
            stats_collection = self.db["stats"]

            # Build query for diagnosis-CPT stats
            query = {
                "diagnosis_code": {"$exists": True, "$ne": None},
                "cpt_code": {"$exists": True, "$ne": None}
            }
            if payer:
                query[payer_field] = payer

            # Step 1: Count total diagnosis-CPT combinations with stats
            logger.info("[1/5] Counting diagnosis-CPT combinations with stats")

            total_combinations = await stats_collection.count_documents(query)
            metrics["total_combinations"] = total_combinations
            logger.info(f"✓ Total diagnosis-CPT combinations: {total_combinations}")

            # Step 2: Count combinations with record_count >= threshold
            logger.info("[2/5] Counting combinations with record_count >= %d", record_count_threshold)

            query_with_threshold = {
                **query,
                "record_count": {"$gte": record_count_threshold}
            }
            combinations_with_threshold = await stats_collection.count_documents(query_with_threshold)

            metrics["combinations_with_sufficient_records"] = combinations_with_threshold
            logger.info("✓ Combinations with record_count >= %d: %d", 
                       record_count_threshold, combinations_with_threshold)

            # Step 3: Count combinations with record_count >= 3 (minimum)
            logger.info("[3/5] Counting combinations with record_count >= 3")

            query_min_records = {
                **query,
                "record_count": {"$gte": 3}
            }
            combinations_min_records = await stats_collection.count_documents(query_min_records)

            metrics["combinations_with_min_records"] = combinations_min_records
            logger.info("✓ Combinations with record_count >= 3: %d", combinations_min_records)

            # Step 4: Count combinations with paid > 0
            logger.info("[4/5] Counting combinations with paid > 0")

            query_with_paid = {
                **query,
                "paid": {"$gt": 0}
            }
            combinations_with_paid = await stats_collection.count_documents(query_with_paid)

            metrics["combinations_with_paid"] = combinations_with_paid
            logger.info(f"✓ Combinations with paid > 0: {combinations_with_paid}")

            # Step 5: Calculate average record_count
            logger.info("[5/5] Calculating average record_count")

            pipeline = [
                {"$match": query},
                {
                    "$group": {
                        "_id": None,
                        "avg_record_count": {"$avg": "$record_count"}
                    }
                }
            ]

            result = await stats_collection.aggregate(pipeline).to_list(length=1)
            avg_record_count = result[0]["avg_record_count"] if result else 0

            metrics["avg_record_count"] = round(avg_record_count, 2)
            logger.info(f"✓ Average record_count: {avg_record_count:.2f}")

            # Validation
            if total_combinations < min_combinations:
                validation_issues.append(
                    f"Insufficient diagnosis-CPT combinations: {total_combinations} < {min_combinations}"
                )
                logger.error("✗ Insufficient combinations")

            if combinations_with_threshold < min_combinations * 0.5:
                validation_issues.append(
                    f"Too few combinations with record_count >= {record_count_threshold}: {combinations_with_threshold}"
                )
                logger.error("✗ Too few high-quality combinations")

            if combinations_with_paid < total_combinations * 0.8:
                validation_issues.append(
                    f"Too many combinations with paid = 0: {total_combinations - combinations_with_paid}"
                )
                logger.warning("⚠ Many combinations have zero paid amount")

            # Determine result
            if validation_issues:
                # Determine severity based on coverage
                coverage_pct = (combinations_with_threshold / min_combinations * 100) if min_combinations > 0 else 0
                
                if coverage_pct < 30:
                    severity = FeatureIssueSeverity.critical
                elif coverage_pct < 60:
                    severity = FeatureIssueSeverity.high
                else:
                    severity = FeatureIssueSeverity.medium

                return self.create_check_result(
                    key="diagnosis_cpt_patterns",
                    name="Diagnosis-CPT Pattern Stats",
                    description=f"Insufficient pattern stats. Issues: {', '.join(validation_issues)}. Metrics: {metrics}",
                    status=CheckStatus.failed,
                    severity=severity,
                    solution=f"Generate diagnosis-CPT stats. Need at least {min_combinations} combinations with record_count >= {record_count_threshold}. Run stats collection script."
                )

            return self.create_check_result(
                key="diagnosis_cpt_patterns",
                name="Diagnosis-CPT Pattern Stats",
                description=f"Sufficient diagnosis-CPT pattern stats available. Metrics: {metrics}",
                status=CheckStatus.passed
            )

        except Exception as e:
            logger.exception("Error in diagnosis-CPT patterns check")
            return self.create_check_result(
                key="diagnosis_cpt_patterns",
                name="Diagnosis-CPT Pattern Stats",
                description=f"Error during check: {str(e)}",
                status=CheckStatus.failed,
                severity=FeatureIssueSeverity.critical,
                solution="Check stats collection and database connection"
            )

    # CHECK 5: Data Quality

    async def _check_data_quality(
        self,
        source_name: str,
        payer: Optional[str] = None
    ) -> CheckResult:
        """
        Check 5: Data Quality
        
        Validates that stats are valid and useful:
        - Stats pass validation logic
        - Stats have paid > 0
        - Stats quality metrics
        
        Returns:
            CheckResult with PASSED or FAILED status
        """
        logger.info("=" * 70)
        logger.info("CHECK 5: Data Quality")
        logger.info("=" * 70)

        validation_issues = []
        metrics = {}

        try:
            payer_field = self.stats_settings.payer_field

            # Get stats collection
            stats_collection = self.db["stats"]

            # Build query
            query = {
                "diagnosis_code": {"$exists": True, "$ne": None},
                "cpt_code": {"$exists": True, "$ne": None}
            }
            if payer:
                query[payer_field] = payer

            # Step 1: Sample stats for validation
            logger.info("[1/3] Sampling stats for validation")

            sample_stats = await stats_collection.find(query).limit(100).to_list(length=100)
            total_sampled = len(sample_stats)

            if total_sampled == 0:
                return self.create_check_result(
                    key="data_quality",
                    name="Data Quality",
                    description="No stats available to validate",
                    status=CheckStatus.failed,
                    severity=FeatureIssueSeverity.critical,
                    solution="Generate diagnosis-CPT stats first"
                )

            logger.info(f"✓ Sampled {total_sampled} stats for validation")

            # Step 2: Validate stats
            logger.info("[2/3] Validating stats")

            valid_count = 0
            invalid_count = 0
            paid_zero_count = 0

            for stat in sample_stats:
                billed = stat.get("billed", 0) or 0
                paid = stat.get("paid", 0) or 0
                adjusted = stat.get("adjusted", 0) or 0
                record_count = stat.get("record_count", 0) or 0

                # Validation logic (same as feature)
                is_valid = self._validate_stats(billed, paid, adjusted, record_count)

                if is_valid:
                    valid_count += 1
                else:
                    invalid_count += 1

                if paid <= 0:
                    paid_zero_count += 1

            valid_pct = (valid_count / total_sampled * 100) if total_sampled > 0 else 0
            paid_pct = ((total_sampled - paid_zero_count) / total_sampled * 100) if total_sampled > 0 else 0

            metrics["total_sampled"] = total_sampled
            metrics["valid_count"] = valid_count
            metrics["invalid_count"] = invalid_count
            metrics["valid_percentage"] = round(valid_pct, 2)
            metrics["paid_zero_count"] = paid_zero_count
            metrics["paid_percentage"] = round(paid_pct, 2)

            logger.info("✓ Valid stats: %d/%d (%.2f%%)", valid_count, total_sampled, valid_pct)
            logger.info("✓ Stats with paid > 0: %d/%d (%.2f%%)", 
                       total_sampled - paid_zero_count, total_sampled, paid_pct)

            # Step 3: Validate quality thresholds
            logger.info("[3/3] Validating quality thresholds")

            if valid_pct < 80:
                validation_issues.append(
                    f"Too many invalid stats: {invalid_count}/{total_sampled} ({100-valid_pct:.2f}%)"
                )
                logger.error("✗ Too many invalid stats")

            if paid_pct < 80:
                validation_issues.append(
                    f"Too many stats with paid = 0: {paid_zero_count}/{total_sampled} ({100-paid_pct:.2f}%)"
                )
                logger.warning("⚠ Many stats have zero paid amount")

            # Determine result
            if validation_issues:
                severity = FeatureIssueSeverity.high if valid_pct < 60 else FeatureIssueSeverity.medium

                return self.create_check_result(
                    key="data_quality",
                    name="Data Quality",
                    description=f"Data quality issues detected. Issues: {', '.join(validation_issues)}. Metrics: {metrics}",
                    status=CheckStatus.failed,
                    severity=severity,
                    solution="Review stats generation process. Ensure proper data validation and filtering. Stats with paid = 0 will be filtered out by the feature."
                )

            return self.create_check_result(
                key="data_quality",
                name="Data Quality",
                description=f"Data quality is good. Metrics: {metrics}",
                status=CheckStatus.passed
            )

        except Exception as e:
            logger.exception("Error in data quality check")
            return self.create_check_result(
                key="data_quality",
                name="Data Quality",
                description=f"Error during check: {str(e)}",
                status=CheckStatus.failed,
                severity=FeatureIssueSeverity.critical,
                solution="Check stats collection and validation logic"
            )

    def _validate_stats(
        self,
        billed: float,
        paid: float,
        adjusted: float,
        record_count: int
    ) -> bool:
        """
        Validate stats using same logic as the Additional Charge feature.
        
        Stats are invalid if:
        - billed < 0, paid < 0, or adjusted < 0
        - record_count < 3
        - paid > billed (overpayment)
        - adjusted > billed (over-adjustment)
        """
        if billed < 0 or paid < 0 or adjusted < 0:
            return False

        if record_count < 3:
            return False

        if paid > billed:
            return False

        if adjusted > billed:
            return False

        return True

