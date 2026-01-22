"""
Main Entry Point - Feature Readiness Validation
"""

import asyncio
import sys
from pathlib import Path
from loguru import logger

# Add project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from shared.db import init_db, close_db
from ai_core.feature_readiness.checks.additional_charge_checks import AdditionalChargeReadinessCheck
from ai_core.feature_readiness.checks.charge_analysis_checks import ChargeAnalysisReadinessCheck
from ai_core.feature_readiness.base_standalone import CheckStatus


async def run_additional_charge_checks(client):
    """Run Additional Charge readiness validation"""
    
    logger.info("\n" + "=" * 80)
    logger.info("ADDITIONAL CHARGE READINESS VALIDATION")
    logger.info("=" * 80 + "\n")
    
    try:
        # Create Readiness Checker
        logger.info("Creating Additional Charge readiness checker...")
        checker = AdditionalChargeReadinessCheck(
            mongo_client=client,
            database_name="rcm_test_db",
            collection_name="claims"
        )
        logger.info("Checker created successfully\n")
        
        # Run Checks
        logger.info("Running readiness checks...\n")
        results = await checker.run_checks(source_name="rcm_test_db")
        
        # Display Results Summary
        logger.info("\n" + "=" * 80)
        logger.info("ADDITIONAL CHARGE - RESULTS SUMMARY")
        logger.info("=" * 80 + "\n")
        
        total_checks = len(results)
        passed = sum(1 for r in results if r.status == CheckStatus.passed)
        failed = total_checks - passed
        readiness_score = (passed / total_checks * 100) if total_checks > 0 else 0
        
        logger.info(f"Total Checks:       {total_checks}")
        logger.info(f"Passed:            {passed}")
        logger.info(f"Failed:             {failed}")
        logger.info(f"Readiness Score:   {readiness_score:.1f}%\n")
        
        logger.info("Detailed Results:")
        logger.info("-" * 80 + "\n")
        
        for i, result in enumerate(results, 1):
            status_text = "[PASS]" if result.status == CheckStatus.passed else "[FAIL]"
            logger.info(f"{i}. {status_text} {result.name}")
            logger.info(f"   Status:       {result.status.value.upper()}")
            logger.info(f"   Description:   {result.description}")
            
            if result.severity:
                logger.info(f"   Severity:     [{result.severity.value.upper()}]")
            if result.solution:
                logger.info(f"   Solution:     {result.solution}")
            logger.info("")
        
        logger.info("=" * 80)
        if readiness_score == 100:
            logger.info("STATUS: READY - All checks passed!")
        elif readiness_score >= 75:
            logger.info("STATUS: MOSTLY READY - Some issues to address")
        elif readiness_score >= 50:
            logger.info("STATUS: PARTIALLY READY - Several issues to fix")
        else:
            logger.info("STATUS: NOT READY - Critical issues must be resolved")
        logger.info("=" * 80 + "\n")
        
        return readiness_score, results
        
    except Exception as e:
        logger.error(f"Error during Additional Charge checks: {e}")
        logger.exception(e)
        return 0, []


async def run_charge_analysis_checks(client):
    """Run Charge Analysis readiness validation"""
    
    logger.info("\n" + "=" * 80)
    logger.info("CHARGE ANALYSIS READINESS VALIDATION")
    logger.info("=" * 80 + "\n")
    
    try:
        # Create Readiness Checker
        logger.info("Creating Charge Analysis readiness checker...")
        checker = ChargeAnalysisReadinessCheck(
            mongo_client=client,
            database_name="rcm_test_db",
            collection_name="claims"
        )
        logger.info("Checker created successfully\n")
        
        # Run Checks
        logger.info("Running readiness checks...\n")
        results = await checker.run_checks(source_name="Data_Quality_Analyzer")
        
        # Display Results Summary
        logger.info("\n" + "=" * 80)
        logger.info("CHARGE ANALYSIS - RESULTS SUMMARY")
        logger.info("=" * 80 + "\n")
        
        total_checks = len(results)
        passed = sum(1 for r in results if r.status == CheckStatus.passed)
        failed = total_checks - passed
        readiness_score = (passed / total_checks * 100) if total_checks > 0 else 0
        
        logger.info(f"Total Checks:       {total_checks}")
        logger.info(f"Passed:            {passed}")
        logger.info(f"Failed:             {failed}")
        logger.info(f"Readiness Score:   {readiness_score:.1f}%\n")
        
        logger.info("Detailed Results:")
        logger.info("-" * 80 + "\n")
        
        for i, result in enumerate(results, 1):
            status_text = "[PASS]" if result.status == CheckStatus.passed else "[FAIL]"
            logger.info(f"{i}. {status_text} {result.name}")
            logger.info(f"   Status:       {result.status.value.upper()}")
            logger.info(f"   Description:   {result.description}")
            
            if result.severity:
                logger.info(f"   Severity:     [{result.severity.value.upper()}]")
            if result.solution:
                logger.info(f"   Solution:     {result.solution}")
            logger.info("")
        
        logger.info("=" * 80)
        if readiness_score == 100:
            logger.info("STATUS: READY - All checks passed!")
        elif readiness_score >= 75:
            logger.info("STATUS: MOSTLY READY - Some issues to address")
        elif readiness_score >= 50:
            logger.info("STATUS: PARTIALLY READY - Several issues to fix")
        else:
            logger.info("STATUS: NOT READY - Critical issues must be resolved")
        logger.info("=" * 80 + "\n")
        
        return readiness_score, results
        
    except Exception as e:
        logger.error(f"Error during Charge Analysis checks: {e}")
        logger.exception(e)
        return 0, []


async def run_all_readiness_checks():
    """Run all feature readiness validations"""
    
    logger.info("\n" + "=" * 80)
    logger.info("DATA QUALITY READINESS ANALYZER - ALL FEATURES")
    logger.info("=" * 80 + "\n")
    
    client = None
    
    try:
        # Initialize Database
        logger.info("Initializing database connection...")
        client, db = await init_db()
        logger.info("Database connected successfully\n")
        
        # Run Additional Charge Checks
        additional_score, additional_results = await run_additional_charge_checks(client)
        
        # Run Charge Analysis Checks
        charge_score, charge_results = await run_charge_analysis_checks(client)
        
        # Overall Summary
        logger.info("\n" + "=" * 80)
        logger.info("OVERALL READINESS SUMMARY")
        logger.info("=" * 80 + "\n")
        
        logger.info(f"Additional Charge:    {additional_score:.1f}% ({sum(1 for r in additional_results if r.status == CheckStatus.passed)}/{len(additional_results)} checks passed)")
        logger.info(f"Charge Analysis:      {charge_score:.1f}% ({sum(1 for r in charge_results if r.status == CheckStatus.passed)}/{len(charge_results)} checks passed)")
        
        overall_score = (additional_score + charge_score) / 2
        logger.info(f"\nOverall Readiness:    {overall_score:.1f}%")
        
        logger.info("\n" + "=" * 80)
        if overall_score == 100:
            logger.info("ALL FEATURES READY FOR PRODUCTION!")
        elif overall_score >= 75:
            logger.info("MOSTLY READY - Minor issues to address")
        elif overall_score >= 50:
            logger.info("PARTIALLY READY - Several issues need attention")
        else:
            logger.info("NOT READY - Critical issues must be resolved")
        logger.info("=" * 80 + "\n")
        
        return 0 if overall_score == 100 else 1
        
    except Exception as e:
        logger.error(f"Error during readiness checks: {e}")
        logger.exception(e)
        return 1
        
    finally:
        if client:
            await close_db(client)


def main():
    """Entry point"""
    try:
        exit_code = asyncio.run(run_all_readiness_checks())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        logger.warning("\n\nInterrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()