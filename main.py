"""
Main Entry Point
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
from ai_core.feature_readiness.base_standalone import CheckStatus


async def run_readiness_checks():
    """Run Additional Charge readiness validation"""
    
    logger.info("\n" + "=" * 80)
    logger.info("ADDITIONAL CHARGE READINESS VALIDATION")
    logger.info("=" * 80 + "\n")
    
    client = None
    
    try:
        # Initialize Database
        logger.info("Initializing database...")
        client, db = await init_db()
        
        # Create Readiness Checker
        logger.info("Creating readiness checker...")
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
        logger.info("FINAL RESULTS SUMMARY")
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
        
        return 0 if readiness_score == 100 else 1
        
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
        exit_code = asyncio.run(run_readiness_checks())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        logger.warning("\n\nInterrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
