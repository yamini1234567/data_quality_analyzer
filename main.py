import asyncio
from loguru import logger
from datetime import datetime
from shared.db import init_db, close_db
import config
from ai_core.shared.validator import Validator
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
 
async def run_new_validation(db):
    validator = Validator(db)
    payers = await db.claims.distinct("payerMCO")
    
    for i, payer in enumerate(payers, 1):
        logger.info(f"[{i}/{len(payers)}] Validating: {payer}")
        await validator.run_validation(filters={'payer': payer})
    
    
async def run_data_quality(db):
    current_time = datetime.now()
    
    total_claims = await db.claims.count_documents({})
    payers = await db.claims.distinct("payerMCO")
    unique_cpts = await db.claims.distinct("charges.cptHcpcs")
    
    overview = Overview(
        total_claims=total_claims,
        unique_payers=len(payers),
        unique_cpt_codes=len(unique_cpts)
    )
    
    overview_document = DataQualityResult(
        timestamp=current_time,
        version=1,
        analysis_type="overview",
        quality_check=overview
    )
    await overview_document.insert()
    
    payer_data = await payer_analysis(db)  
    payer_document = DataQualityResult(
        timestamp=current_time,
        version=1,
        analysis_type="payer",
        quality_check=payer_data
    )
    await payer_document.insert()
    
    for i, payer in enumerate(payers, 1):
        logger.info(f"[{i}/{len(payers)}] Analyzing: {payer}")
        
        filters = {'payer': payer}
        
        (
            charges_data,
            claims_data,
            cpt_data,
            claims_adjustment_data,
            diagnosis_data
        ) = await asyncio.gather(
            charges_analysis(db, filters),
            claims_analysis(db, filters),
            cpt_analysis(db, filters),
            adjustment_analysis(db, filters),
            diagnosis_analysis(db, filters)
        )
        
        documents_to_save = [
            DataQualityResult(
                timestamp=current_time,
                version=1,
                payer=payer,
                analysis_type="diagnosis",
                quality_check=diagnosis_data
            ),
            DataQualityResult(
                timestamp=current_time,
                version=1,
                payer=payer,
                analysis_type="charges",
                quality_check=charges_data
            ),
            DataQualityResult(
                timestamp=current_time,
                version=1,
                payer=payer,
                analysis_type="cpt",
                quality_check=cpt_data
            ),
            DataQualityResult(
                timestamp=current_time,
                version=1,
                payer=payer,
                analysis_type="claims",
                quality_check=claims_data
            ),
            DataQualityResult(
                timestamp=current_time,
                version=1,
                payer=payer,
                analysis_type="adjustment",
                quality_check=claims_adjustment_data
            )
        ]
        
        for doc in documents_to_save:
            await doc.insert()
   
   
async def run_checks(client):
    checker1 = AdditionalChargeReadinessCheck(client, config.DATABASE_NAME, config.COLLECTION_NAME)
    results1 = await checker1.run_checks(source_name=config.DATABASE_NAME)
    passed1 = sum(1 for r in results1 if r.status == CheckStatus.passed)
   
    checker2 = ChargeAnalysisReadinessCheck(client, config.DATABASE_NAME, config.COLLECTION_NAME)
    results2 = await checker2.run_checks(source_name=config.DATABASE_NAME)
    passed2 = sum(1 for r in results2 if r.status == CheckStatus.passed)
   
    total = len(results1) + len(results2)
    passed = passed1 + passed2
    score = (passed / total * 100) if total > 0 else 0
    logger.info(f"Checks: {score:.1f}% ({passed}/{total})")
 
 
async def main():
    client, db = await init_db()
   
    if config.RUN_CHECKS:
        await run_checks(client)
   
    if config.RUN_DATA_QUALITY:
        await run_data_quality(db)
        
    if config.USE_NEW_VALIDATION:
        await run_new_validation(db)
   
    await close_db(client)
 
 
asyncio.run(main())