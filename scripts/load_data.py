"""Load claims data from JSON into MongoDB."""
import asyncio
import json
import os
from pathlib import Path
from motor.motor_asyncio import AsyncIOMotorClient
from loguru import logger
from dotenv import load_dotenv

load_dotenv()


async def load_claims():
    """Load claims from JSON file into MongoDB."""
    logger.info("="*80)
    logger.info("LOADING CLAIMS DATA")
    logger.info("="*80)
    logger.info("")
    
    # Configuration
    mongodb_uri = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
    db_name = os.getenv("MONGODB_DB_NAME", "rcm_test_db")
    json_file = "data/claims 1.json"
    
    try:
        # Check file
        logger.info(f"Checking for file: {json_file}")
        json_path = Path(json_file)
        
        if not json_path.exists():
            logger.error(f"  File not found: {json_file}")
            return False
        
        file_size = json_path.stat().st_size / (1024 * 1024)
        logger.success(f"  File found: {file_size:.2f} MB")
        logger.info("")
        
        # Load JSON
        logger.info("Loading JSON file...")
        with open(json_file, 'r', encoding='utf-8') as f:
            claims_dict = json.load(f)
        
        logger.success(f"  Loaded {len(claims_dict):,} claims")
        logger.info("")
        
        # Connect to MongoDB
        logger.info("Connecting to MongoDB...")
        client = AsyncIOMotorClient(mongodb_uri)
        db = client[db_name]
        claims_collection = db["claims"]
        
        await client.admin.command('ping')
        logger.success(f"  Connected to {db_name}")
        logger.info("")
        
        # Prepare documents
        logger.info("Preparing documents...")
        claims_list = []
        
        for claim_id, claim_data in claims_dict.items():
            claim_data['_id'] = claim_id
            claim_data['claimId'] = claim_id
            claims_list.append(claim_data)
        
        logger.success(f"  Prepared {len(claims_list):,} documents")
        logger.info("")
        
        # Check existing
        logger.info("Checking existing claims...")
        existing = await claims_collection.count_documents({})
        
        if existing > 0:
            logger.warning(f"  Found {existing:,} existing claims - clearing...")
            await claims_collection.delete_many({})
            logger.success("  Cleared existing data")
        else:
            logger.success("  Collection is empty")
        logger.info("")
        
        # Insert in batches
        logger.info("Inserting claims...")
        batch_size = 1000
        total = 0
        
        for i in range(0, len(claims_list), batch_size):
            batch = claims_list[i:i + batch_size]
            await claims_collection.insert_many(batch, ordered=False)
            total += len(batch)
            logger.info(f"  {total:,}/{len(claims_list):,} inserted...")
        
        logger.success(f"  Inserted {total:,} claims")
        logger.info("")
        
        # Verify
        logger.info("Verifying...")
        final = await claims_collection.count_documents({})
        
        if final == len(claims_list):
            logger.success(f"  Verified: {final:,} claims")
        else:
            logger.warning(f"  Expected {len(claims_list):,}, found {final:,}")
        logger.info("")
        
        # Sample
        sample = await claims_collection.find_one()
        if sample:
            logger.info("Sample claim:")
            logger.info(f"  ID: {sample.get('claimId', 'N/A')}")
            logger.info(f"  Payer: {sample.get('payerMCO', 'N/A')}")
            logger.info(f"  Has diagnoses: {bool(sample.get('diagnoses'))}")
            logger.info(f"  Has charges: {bool(sample.get('charges'))}")
        logger.info("")
        
        # Create indexes
        logger.info("Creating indexes...")
        await claims_collection.create_index("claimId")
        await claims_collection.create_index("payerMCO")
        await claims_collection.create_index("diagnoses.code")
        await claims_collection.create_index("charges.cptHcpcs")
        logger.success("  Indexes created")
        logger.info("")
        
        client.close()
        
        logger.info("="*80)
        logger.success("CLAIMS DATA LOADED SUCCESSFULLY")
        logger.info("="*80)
        logger.info(f"  Database: {db_name}")
        logger.info(f"  Collection: claims")
        logger.info(f"  Total: {final:,} claims")
        logger.info("="*80)
        
        return True
        
    except Exception as e:
        logger.error("="*80)
        logger.error("LOADING FAILED")
        logger.error("="*80)
        logger.error(f"Error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False


if __name__ == "__main__":
    success = asyncio.run(load_claims())
    exit(0 if success else 1)
