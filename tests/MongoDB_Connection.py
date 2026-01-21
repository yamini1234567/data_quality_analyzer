"""
Test MongoDB connection for Data Quality Analyzer

"""

import asyncio
import os
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv
from loguru import logger

# Load environment variables
load_dotenv()

# Configuration
MONGODB_URI = os. getenv("MONGODB_URI", "mongodb://localhost:27017")
DATABASE_NAME = os.getenv("MONGODB_DB_NAME", "Data_Quality_Analyzer")
COLLECTION_NAME = "claims"
PAYER_FIELD = "payerMCO"
CPT_FIELD = "cptHcpcs"
CHARGES_FIELD = "charges"

async def test_mongodb_connection():
    """Test MongoDB connection and validate data"""
    
    logger.info("=" * 80)
    logger.info("MONGODB CONNECTION TEST - Data Quality Analyzer")
    logger.info("=" * 80)
    logger.info("")
    
    logger.info(" Configuration:")
    logger.info(f"   MongoDB URI: {MONGODB_URI}")
    logger.info(f"   Database: {DATABASE_NAME}")
    logger.info(f"   Collection: {COLLECTION_NAME}")
    logger.info("")
    
    # Step 1: Connect
    logger.info("Step 1: Creating MongoDB connection...")
    try:
        mongo_client = AsyncIOMotorClient(MONGODB_URI)
        logger.info("    Client created")
        logger.info("")
    except Exception as e:
        logger.error(f" ERROR: {e}")
        return
    
    # Step 2: Test connection
    logger.info(" Step 2: Testing connection...")
    try:
        await mongo_client.admin.command('ping')
        logger.info("MongoDB connection successful!")
        logger.info("")
    except Exception as e:
        logger.error(f"ERROR:  {e}")
        mongo_client.close()
        return
    
    # Step 3: Check database
    logger.info(" Step 3: Checking database...")
    try:
        databases = await mongo_client.list_database_names()
        
        if DATABASE_NAME in databases:
            logger.info(f"   Database '{DATABASE_NAME}' found!")
        else:
            logger.warning(f"    Database '{DATABASE_NAME}' NOT found!")
            logger.info(f"   Available:  {databases}")
            mongo_client.close()
            return
        logger.info("")
    except Exception as e: 
        logger.error(f"ERROR: {e}")
        mongo_client.close()
        return
    
    # Step 4: Check collection
    logger.info(" Step 4: Checking collection...")
    try:
        db = mongo_client[DATABASE_NAME]
        collections = await db.list_collection_names()
        
        if COLLECTION_NAME in collections:
            logger.info(f"Collection '{COLLECTION_NAME}' found!")
        else:
            logger.warning(f"Collection '{COLLECTION_NAME}' NOT found!")
            logger.info(f"Available: {collections}")
            mongo_client.close()
            return
        logger.info("")
    except Exception as e:
        logger.error(f"ERROR: {e}")
        mongo_client.close()
        return
    
    # Step 5: Count documents
    logger.info("Step 5: Counting claims...")
    try:
        collection = db[COLLECTION_NAME]
        total_count = await collection.count_documents({})
        logger.info(f"Total claims:  {total_count: ,}")
        logger.info("")
        
        if total_count == 0:
            logger.warning(" Collection is empty!")
            mongo_client.close()
            return
    except Exception as e:
        logger.error(f"ERROR: {e}")
        mongo_client.close()
        return
    
    # Step 6: Analyze sample document
    logger.info("Step 6: Analyzing sample claim...")
    try:
        sample = await collection.find_one()
        
        if sample: 
            logger.info("Sample claim found!")
            logger.info("")
            logger.info("Top-level fields:")
            
            for key in list(sample.keys())[:15]: 
                value_type = type(sample[key]).__name__
                value_preview = ""
                
                if key in ["claimId", "_id", "payerMCO"]:
                    value = str(sample[key])
                    value_preview = f" = {value[: 50]}"
                
                logger.info(f"      - {key}: {value_type}{value_preview}")
            
            if len(sample. keys()) > 15:
                logger.info(f"      ... ({len(sample.keys()) - 15} more fields)")
            
            logger.info("")
            logger.info("Key fields:")
            logger.info(f"      - Claim ID:  {sample.get('claimId', '❌ NOT FOUND')}")
            logger.info(f"      - Payer (payerMCO): {sample.get('payerMCO', '❌ NOT FOUND')}")
            logger.info(f"      - Has charges: {'YES' if sample.get('charges') else '❌ NO'}")
            logger.info(f"      - Has diagnoses: {'YES' if sample.get('diagnoses') else '❌ NO'}")
            logger.info("")
            
            # Check charges structure
            if sample.get('charges'):
                charges = sample['charges']
                logger.info(f"Charges array:  {len(charges)} charge(s)")
                
                if len(charges) > 0:
                    logger.info("First charge structure:")
                    first_charge = charges[0]
                    
                    for key in first_charge.keys():
                        value_type = type(first_charge[key]).__name__
                        value_preview = ""
                        if key == CPT_FIELD:
                            value_preview = f" = {first_charge[key]}"
                        logger.info(f"- charges[0].{key}: {value_type}{value_preview}")
                    
                    logger.info("")
                    
                    if CPT_FIELD in first_charge:
                        logger.info(f"CPT field '{CPT_FIELD}' found!")
                        logger.info(f"      Example: {first_charge[CPT_FIELD]}")
                    else:
                        logger.warning(f"CPT field '{CPT_FIELD}' NOT found!")
                    
                    logger.info("")
    except Exception as e:
        logger.error(f"ERROR: {e}")
        import traceback
        logger.error(traceback.format_exc())
        mongo_client.close()
        return
    
    # Step 7: Test queries
    logger.info("Step 7: Testing readiness check queries...")
    logger.info("")
    
    # Query 1: Claims with charges
    logger.info("Query 1: Claims with charges")
    try:
        claims_with_charges = await collection. count_documents({
            CHARGES_FIELD:  {"$exists": True, "$ne": []}
        })
        percentage = (claims_with_charges / total_count * 100) if total_count > 0 else 0
        logger.info(f"{claims_with_charges:,} claims ({percentage:.1f}%)")
    except Exception as e:
        logger.error(f"ERROR: {e}")
    
        # Query 2: Claims with CPT codes (FIXED)
    logger.info("Query 2: Claims with CPT codes")
    try:
        claims_with_cpt = await collection.count_documents({
            "charges": {
                "$elemMatch": {
                    "cptHcpcs": {"$exists": True, "$ne": None, "$ne": ""}
                }
            }
        })
        percentage = (claims_with_cpt / total_count * 100) if total_count > 0 else 0
        logger.info(f"{claims_with_cpt:,} claims ({percentage:.1f}%)")
    except Exception as e:
        logger.error(f"ERROR: {e}")
    
    # Query 3: Unique payers
    logger.info("Query 3: Unique payers")
    try:
        pipeline = [
            {"$group":  {"_id": f"${PAYER_FIELD}"}},
            {"$count": "total"}
        ]
        result = await collection.aggregate(pipeline).to_list(length=1)
        unique_payers = result[0]["total"] if result else 0
        logger.info(f"{unique_payers} unique payers")
    except Exception as e:
        logger.error(f"ERROR:  {e}")
    
    # Query 4: Unique CPT codes
    logger.info("Query 4: Unique CPT codes")
    try:
        pipeline = [
            {"$unwind": f"${CHARGES_FIELD}"},
            {"$group": {"_id": f"${CHARGES_FIELD}.{CPT_FIELD}"}},
            {"$count": "total"}
        ]
        result = await collection.aggregate(pipeline).to_list(length=1)
        unique_cpts = result[0]["total"] if result else 0
        logger.info(f"{unique_cpts} unique CPT codes")
    except Exception as e:
        logger.error(f"ERROR: {e}")
    
    logger.info("")
    
    # Summary
    logger.info("=" * 80)
    logger.info(" CONNECTION TEST SUCCESSFUL!")
    logger.info("=" * 80)
    logger.info("")
    logger.info(" Data Statistics:")
    logger.info(f"Total claims: {total_count:,}")
    logger.info(f"Claims with charges: {claims_with_charges:,}")
    logger.info(f"Claims with CPT codes: {claims_with_cpt:,}")
    logger.info(f"Unique payers: {unique_payers}")
    logger.info(f"Unique CPT codes: {unique_cpts}")
    logger.info("")
    logger.info(" Configuration for charge_analysis_check. py:")
    logger.info(f"database_name = '{DATABASE_NAME}'")
    logger.info(f"collection_name = '{COLLECTION_NAME}'")
    logger.info(f"payer_field = '{PAYER_FIELD}'")
    logger.info(f"cpt_field = '{CPT_FIELD}'")
    logger.info("")
    logger.info("Ready to implement readiness checks!")
    logger.info("=" * 80)
    
    mongo_client.close()

if __name__ == "__main__":
    asyncio.run(test_mongodb_connection())