import asyncio
from loguru import logger
from datetime import datetime
from shared.db import init_db, close_db
from ai_core.feature_readiness.appsettings import get_app_settings
from ai_core.shared.validator import Validation
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
    validator = Validation(db)    
    await validator.run_validations()
    
    
async def run_data_quality(db):
    total_claims = await db.claims.count_documents({})
    payers = await db.claims.distinct("payerMCO")
    unique_cpts = await db.claims.distinct("charges.cptHcpcs")
    
    overview = Overview(
        total_claims=total_claims,
        unique_payers=len(payers),
        unique_cpt_codes=len(unique_cpts)
    )
    overview_doc = DataQualityResult(
        timestamp=datetime.now(),
        version=1,
        analysis_type="overview",
        quality_check=overview
    )
    await overview_doc.insert()
    
    payer_data = await payer_analysis(db)
    payer_doc = DataQualityResult(
        timestamp=datetime.now(),
        version=1,
        analysis_type="payer",
        quality_check=payer_data
    )
    await payer_doc.insert()
    
    for i, payer in enumerate(payers, 1):
        logger.info(f"[{i}/{len(payers)}] Analyzing: {payer}")
        
        filters = {'payer': payer}
        
        await asyncio.gather(
            charges_analysis(db, filters),
            claims_analysis(db, filters),
            cpt_analysis(db, filters),
            adjustment_analysis(db, filters),
            diagnosis_analysis(db, filters)
        )
   
   
async def run_checks(client, settings):
    checker1 = AdditionalChargeReadinessCheck(
        client,
        settings.data_quality_settings.database_name,
        settings.data_quality_settings.collection_name
    )
    results1 = await checker1.run_checks(source_name=settings.data_quality_settings.database_name)
    passed1 = sum(1 for r in results1 if r.status == CheckStatus.passed)
   
    checker2 = ChargeAnalysisReadinessCheck(
        client,
        settings.data_quality_settings.database_name,
        settings.data_quality_settings.collection_name
    )
    results2 = await checker2.run_checks(source_name=settings.data_quality_settings.database_name)
    passed2 = sum(1 for r in results2 if r.status == CheckStatus.passed)
   
    total = len(results1) + len(results2)
    passed = passed1 + passed2
    score = (passed / total * 100) if total > 0 else 0
    logger.info(f"Checks: {score:.1f}% ({passed}/{total})")
 
 
async def main():
    client, db = await init_db()
    
    settings = await get_app_settings()
   
    if settings.data_quality_settings.run_checks:
        await run_checks(client, settings)
   
    if settings.data_quality_settings.run_data_quality:
        await run_data_quality(db)
        
    if settings.data_quality_settings.use_new_validation:
        await run_new_validation(db)
   
    await close_db(client)
 
 
asyncio.run(main())