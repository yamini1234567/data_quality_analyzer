import asyncio
from loguru import logger
from datetime import datetime
from shared.db import init_db, close_db
import config
from ai_core.feature_readiness.checks.additional_charge_checks import AdditionalChargeReadinessCheck
from ai_core.feature_readiness.checks.charge_analysis_checks import ChargeAnalysisReadinessCheck
from ai_core.feature_readiness.base_standalone import CheckStatus
from ai_core.data_quality.claims_analysis import claims_analysis
from ai_core.data_quality.payer_analysis import payer_analysis
from ai_core.data_quality.chargespattern_analysis import charges_analysis
from ai_core.data_quality.claimsadjustments_analysis import adjustment_analysis
from ai_core.data_quality.cpt_code_analysis import cpt_analysis
from ai_core.data_quality.models import DataQualityResult, Overview
from ai_core.data_quality.diagnosis_analysis import diagnosis_analysis

async def run_data_quality(db):
    logger.info("Data Quality Analysis")
    (
        payer_data,
        charges_data,
        claims_data,
        cpt_data,
        claims_adjustment_data,
        diagnosis_data
    ) = await asyncio.gather(
        payer_analysis(db),
        charges_analysis(db),
        claims_analysis(db),
        cpt_analysis(db),
        adjustment_analysis(db),
        diagnosis_analysis(db)
    )
    
    logger.info("All analyses complete")
    
    overview = Overview(
        total_claims=payer_data.total_claims,
        unique_payers=payer_data.unique_payers_count,
        unique_cpt_codes=cpt_data.cpt_overview["unique_cpt_codes"]
    )
    
    current_time = datetime.now()
    
    logger.info("Creating seperate result documents")
    documents_to_save = [
        DataQualityResult(
            timestamp=current_time,
            version=1,
            analysis_type="overview",
            quality_check=overview
        ),
        DataQualityResult(
            timestamp=current_time,
            version=1,
            analysis_type="diagnosis",
            quality_check=diagnosis_data
        ),
        DataQualityResult(
            timestamp=current_time,
            version=1,
            analysis_type="payer",
            quality_check=payer_data
        ),
        DataQualityResult(
            timestamp=current_time,
            version=1,
            analysis_type="charges",
            quality_check=charges_data
        ),
        DataQualityResult(
            timestamp=current_time,
            version=1,
            analysis_type="cpt",
            quality_check=cpt_data
        ),
        DataQualityResult(
            timestamp=current_time,
            version=1,
            analysis_type="claims",
            quality_check=claims_data
        ),
        DataQualityResult(
            timestamp=current_time,
            version=1,
            analysis_type="adjustment",
            quality_check=claims_adjustment_data
        )
    ]
    logger.info("Saving results as seperate documents to database")
    for doc in documents_to_save:
        await doc.insert()
        logger.info(f" Saved {doc.analysis_type} analysis - Document ID: {doc.id}")
    
    logger.success("Data quality analysis complete!")
    logger.info(f"Total documents saved: {len(documents_to_save)}")
   
   
async def run_checks(client):
    logger.info("RUNNING CHECKS FOLDER")
   
    logger.info("\nAdditional Charge checks...")
    checker1 = AdditionalChargeReadinessCheck(client, config.DATABASE_NAME, config.COLLECTION_NAME)
    results1 = await checker1.run_checks(source_name=config.DATABASE_NAME)
    passed1 = sum(1 for r in results1 if r.status == CheckStatus.passed)
    logger.info(f"Result: {passed1}/{len(results1)} passed")
   
    logger.info("\nCharge Analysis checks...")
    checker2 = ChargeAnalysisReadinessCheck(client, config.DATABASE_NAME, config.COLLECTION_NAME)
    results2 = await checker2.run_checks(source_name=config.DATABASE_NAME)
    passed2 = sum(1 for r in results2 if r.status == CheckStatus.passed)
    logger.info(f"Result: {passed2}/{len(results2)} passed")
   
    total = len(results1) + len(results2)
    passed = passed1 + passed2
    score = (passed / total * 100) if total > 0 else 0
    logger.info(f"\nChecks complete: {score:.1f}% ({passed}/{total})")
 
async def main():

    logger.info("Data quality analyzer")
   
    logger.info("Connecting to database")
    client, db = await init_db()
    logger.info("Database Connected")
    
    if config.RUN_CHECKS:
        await run_checks(client)
   
    if config.RUN_DATA_QUALITY:
        await run_data_quality(db)
   
    await close_db(client)
 

asyncio.run(main())
