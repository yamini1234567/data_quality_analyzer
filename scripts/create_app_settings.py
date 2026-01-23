"""Initialize app_settings document in MongoDB."""
import asyncio
import os
import sys
from pathlib import Path
from motor.motor_asyncio import AsyncIOMotorClient
from beanie import init_beanie
from dotenv import load_dotenv
from loguru import logger

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "ai_core" / "feature_readiness"))

from appsettings import MAppSettings

load_dotenv()

MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
DATABASE_NAME = os.getenv("MONGODB_DB_NAME", "rcm_test_db")


async def create_app_settings():
    """Create or recreate app_settings document with defaults."""
    
    logger.info("="*80)
    logger.info("APP_SETTINGS INITIALIZATION")
    logger.info("="*80)
    logger.info("")
    
    # Connect to MongoDB
    logger.info("Connecting to MongoDB...")
    logger.info(f"  Database: {DATABASE_NAME}")
    
    try:
        client = AsyncIOMotorClient(MONGODB_URI)
        db = client[DATABASE_NAME]
        await client.admin.command('ping')
        logger.success("  Connected successfully")
        logger.info("")
    except Exception as e:
        logger.error(f"  Connection failed: {e}")
        return
    
    # Initialize Beanie ODM
    logger.info("Initializing Beanie ODM...")
    
    try:
        await init_beanie(database=db, document_models=[MAppSettings])
        logger.success("  Beanie initialized")
        logger.info("")
    except Exception as e:
        logger.error(f"  Beanie initialization failed: {e}")
        client.close()
        return
    
    # Check existing
    logger.info("Checking for existing app_settings...")
    existing = await MAppSettings.find_one()
    
    if existing:
        logger.warning("  app_settings document already exists!")
        logger.info("")
        response = input("  Delete and recreate? (yes/no): ")
        logger.info("")
        
        if response.lower() != 'yes':
            logger.info("  Cancelled - keeping existing settings")
            client.close()
            return
        
        await existing.delete()
        logger.success("  Deleted existing document")
        logger.info("")
    else:
        logger.success("  No existing settings found")
        logger.info("")
    
    # Create new document
    logger.info("Creating new app_settings document...")
    
    try:
        settings = MAppSettings()
        await settings.insert()
        logger.success("  Document created and saved")
        logger.info("")
    except Exception as e:
        logger.error(f"  Failed to create document: {e}")
        client.close()
        return
    
    # Display settings
    logger.info("="*80)
    logger.success("APP_SETTINGS CREATED SUCCESSFULLY")
    logger.info("="*80)
    logger.info("")
    
    logger.info("Stats Settings:")
    logger.info(f"  payer_field: {settings.stats_settings.payer_field}")
    logger.info("")
    
    logger.info("Readiness Check Thresholds:")
    logger.info(f"  claims_with_charges_threshold: {settings.readiness_settings.claims_with_charges_threshold}")
    logger.info(f"  cpt_diversity_threshold: {settings.readiness_settings.cpt_diversity_threshold}")
    logger.info(f"  claims_minimum_total: {settings.readiness_settings.claims_minimum_total}")
    logger.info(f"  stats_coverage_threshold: {settings.readiness_settings.stats_coverage_threshold}")
    logger.info(f"  stats_minimum_record_count: {settings.readiness_settings.stats_minimum_record_count}")
    logger.info("")
    
    logger.info("Additional Charge Settings:")
    if hasattr(settings.readiness_settings, 'claims_with_diagnoses_threshold'):
        logger.info(f"  claims_with_diagnoses_threshold: {settings.readiness_settings.claims_with_diagnoses_threshold}")
    if hasattr(settings.readiness_settings, 'diagnosis_diversity_threshold'):
        logger.info(f"  diagnosis_diversity_threshold: {settings.readiness_settings.diagnosis_diversity_threshold}")
    if hasattr(settings.readiness_settings, 'diagnosis_cpt_min_combinations'):
        logger.info(f"  diagnosis_cpt_min_combinations: {settings.readiness_settings.diagnosis_cpt_min_combinations}")
    logger.info("")
    
    logger.info("="*80)
    logger.success("Settings ready for use")
    logger.info("="*80)
    logger.info("")
    logger.info("Next steps:")
    logger.info("  1. Verify in MongoDB Compass (app_settings collection)")
    logger.info("  2. Load in code: settings = await MAppSettings.find_one()")
    logger.info("")
    logger.info("="*80)
    
    client.close()


if __name__ == "__main__":
    try:
        asyncio.run(create_app_settings())
    except KeyboardInterrupt:
        logger.warning("\nCancelled by user")
    except Exception as e:
        logger.error(f"\nError: {e}")
