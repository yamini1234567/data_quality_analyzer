"""
Generate Historical Stats Collection from Claims Data

This script aggregates claims data to create historical statistics
for each (payer + CPT code) combination. 

Used by AI for charge analysis suggestions.
"""

import asyncio
import os
import sys
from pathlib import Path
from datetime import datetime, timezone
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv
from loguru import logger

# Load environment variables
load_dotenv()

# Configuration
MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
DATABASE_NAME = os.getenv("MONGODB_DB_NAME", "rcm_test_db")
CLAIMS_COLLECTION = "claims"
STATS_COLLECTION = "charge_analysis_stats"

# Field names 
PAYER_FIELD = "payerMCO"
CPT_FIELD = "cptHcpcs"
AMOUNT_FIELD = "amount"              # Billed amount
PAID_FIELD = "amountPaid"            # Paid amount
ADJUSTMENT_FIELD = "adjustmentAmount"  # Adjustment


async def generate_stats():
    """Generate stats collection from claims data"""
    
    logger.info("=" * 80)
    logger.info("HISTORICAL STATS GENERATION")
    logger.info("=" * 80)
    logger.info("")
    
    # Step 1: Connect to MongoDB
    
    logger.info("Step 1: Connecting to MongoDB...")
    logger.info(f"   URI: {MONGODB_URI}")
    logger.info(f"   Database: {DATABASE_NAME}")
    
    try:
        client = AsyncIOMotorClient(MONGODB_URI)
        db = client[DATABASE_NAME]
        
        # Test connection
        await client.admin. command('ping')
        logger.success("   Connected successfully")
        logger.info("")
    except Exception as e:
        logger.error(f"   Connection failed: {e}")
        return
    
    claims_collection = db[CLAIMS_COLLECTION]
    stats_collection = db[STATS_COLLECTION]
    
   
    # Step 2: Check claims data
    
    logger.info("Step 2: Checking claims data...")
    
    total_claims = await claims_collection. count_documents({})
    
    if total_claims == 0:
        logger.error("   No claims found in collection!")
        logger.info("")
        logger.warning("   Please import claims data first.")
        client.close()
        return
    
    logger.success(f"   Found {total_claims} claims")
    logger.info("")
    
   
    # Step 3: Check if stats already exist
   
    logger.info("Step 3: Checking existing stats...")
    
    existing_stats = await stats_collection. count_documents({})
    
    if existing_stats > 0:
        logger.warning(f"   Stats collection already has {existing_stats} documents!")
        logger.info("")
        response = input("   Delete and recreate?   (yes/no): ")
        logger.info("")
        
        if response.lower() != 'yes':
            logger. info("   Keeping existing stats (no changes)")
            logger.info("")
            logger.info("=" * 80)
            logger.info("Operation cancelled")
            logger.info("=" * 80)
            client.close()
            return
        
        # Delete existing
        logger.info("   Deleting existing stats...")
        result = await stats_collection.delete_many({})
        logger.success(f"   Deleted {result.deleted_count} documents")
        logger.info("")
    else:
        logger.success("   No existing stats found (fresh generation)")
        logger.info("")
    

    # Step 4: Build aggregation pipeline
 
    logger.info("Step 4: Building aggregation pipeline...")
    logger.info("   (Grouping by payer + CPT code)")
    logger.info("")
    
    pipeline = [
        # Stage 1: Unwind charges array
        {
            "$unwind": "$charges"
        },
        
        # Stage 2: Filter valid charges with CPT codes
        {
            "$match":  {
                f"charges.{CPT_FIELD}": {
                    "$exists": True,
                    "$ne": None,
                    "$ne": ""
                },
                PAYER_FIELD: {
                    "$exists": True,
                    "$ne": None,
                    "$ne": ""
                }
            }
        },
        
        # Stage 3: Group by payer + CPT code
        {
            "$group": {
                "_id": {
                    "payer": f"${PAYER_FIELD}",
                    "cpt_code": f"$charges.{CPT_FIELD}"
                },
                
                # Count records
                "record_count": {"$sum": 1},
                
                # Collect amounts for statistics
                "billed_amounts": {"$push": f"$charges.{AMOUNT_FIELD}"},
                "paid_amounts": {"$push": f"$charges.{PAID_FIELD}"},
                "adjustment_amounts": {"$push": f"$charges.{ADJUSTMENT_FIELD}"}
            }
        },
        
        # Stage 4: Calculate statistics
        {
            "$project": {
                "_id": 0,
                "payer":  "$_id.payer",
                "cpt_code": "$_id.cpt_code",
                "record_count": 1,
                
                # Billed amount statistics (from 'amount' field)
                "billed_amount_mean": {"$avg": "$billed_amounts"},
                "billed_amount_min": {"$min": "$billed_amounts"},
                "billed_amount_max": {"$max": "$billed_amounts"},
                "billed_amount_std": {"$stdDevPop": "$billed_amounts"},
                
                # Paid amount statistics (from 'amountPaid' field)
                "paid_amount_mean": {"$avg":  "$paid_amounts"},
                "paid_amount_min": {"$min": "$paid_amounts"},
                "paid_amount_max":  {"$max": "$paid_amounts"},
                "paid_amount_std": {"$stdDevPop": "$paid_amounts"},
                
                # Adjustment statistics (from 'adjustmentAmount' field)
                "adjustment_amount_mean": {"$avg": "$adjustment_amounts"},
                "adjustment_amount_min": {"$min": "$adjustment_amounts"},
                "adjustment_amount_max": {"$max": "$adjustment_amounts"},
                
                # Timestamp
                "last_updated": datetime.now(timezone.utc)
            }
        },
        
        # Stage 5: Sort by payer and CPT code
        {
            "$sort": {
                "payer": 1,
                "cpt_code": 1
            }
        }
    ]
    
    
    # Step 5: Run aggregation
   
    logger.info("Step 5: Running aggregation...")
    logger.info("   (This may take a few moments... )")
    logger.info("")
    
    try:
        # Run aggregation and collect results
        cursor = claims_collection.aggregate(pipeline, allowDiskUse=True)
        stats_docs = await cursor.to_list(length=None)
        
        if not stats_docs:
            logger.error("   No stats generated!")
            logger.info("")
            logger.warning("   Possible issues:")
            logger.warning("      - Claims don't have valid charges")
            logger.warning("      - Payer or CPT fields are empty/null")
            logger.info("")
            logger.info("   Field mapping used:")
            logger.info(f"      Payer field: {PAYER_FIELD}")
            logger.info(f"      CPT field: charges.{CPT_FIELD}")
            logger.info(f"      Amount field: charges.{AMOUNT_FIELD}")
            logger.info(f"      Paid field: charges.{PAID_FIELD}")
            client.close()
            return
        
        logger.success(f"   Generated {len(stats_docs)} stat documents")
        logger.info("")
        
    except Exception as e:
        logger.error(f"   Aggregation failed: {e}")
        import traceback
        traceback.print_exc()
        client.close()
        return
    
    
    # Step 6: Insert into stats collection
 
    logger.info("Step 6: Inserting stats into collection...")
    
    try:
        result = await stats_collection.insert_many(stats_docs)
        logger.success(f"   Inserted {len(result.inserted_ids)} documents")
        logger.info("")
    except Exception as e:
        logger.error(f"   Insert failed: {e}")
        client.close()
        return
    
   
    # Step 7: Create indexes
   
    logger.info("Step 7: Creating indexes...")
    
    try:
        # Index on payer + cpt_code (for fast lookups)
        await stats_collection.create_index([("payer", 1), ("cpt_code", 1)], unique=True)
        logger.success("   Created index: (payer, cpt_code)")
        
        # Index on record_count (for quality checks)
        await stats_collection. create_index([("record_count", 1)])
        logger.success("   Created index: record_count")
        
        logger.info("")
    except Exception as e:
        logger.warning(f"   Index creation warning: {e}")
        logger.info("")
    
   
    # Step 8: Display summary statistics
   
    logger. info("=" * 80)
    logger.success("STATS GENERATION COMPLETE!")
    logger.info("=" * 80)
    logger.info("")
    
    # Count stats by quality
    high_quality = await stats_collection.count_documents({"record_count": {"$gte": 10}})
    medium_quality = await stats_collection.count_documents({"record_count": {"$gte": 3, "$lt": 10}})
    low_quality = await stats_collection. count_documents({"record_count": {"$lt": 3}})
    
    # Get unique payers
    payers = await stats_collection.distinct("payer")
    
    # Get unique CPT codes
    cpt_codes = await stats_collection.distinct("cpt_code")
    
    # Average record count
    avg_pipeline = [
        {"$group":  {"_id": None, "avg_record_count": {"$avg": "$record_count"}}}
    ]
    avg_result = await stats_collection.aggregate(avg_pipeline).to_list(1)
    avg_record_count = avg_result[0]["avg_record_count"] if avg_result else 0
    
    logger.info(" Summary Statistics:")
    logger.info("")
    logger.info(f"   Total stat documents: {len(stats_docs)}")
    logger.info(f"   Unique payers: {len(payers)}")
    logger.info(f"   Unique CPT codes: {len(cpt_codes)}")
    logger.info("")
    logger.info("   Quality Distribution:")
    logger.info(f"      High quality (≥10 records):  {high_quality} ({high_quality/len(stats_docs)*100:.1f}%)")
    logger.info(f"      Medium quality (3-9 records): {medium_quality} ({medium_quality/len(stats_docs)*100:.1f}%)")
    logger.info(f"      Low quality (<3 records):     {low_quality} ({low_quality/len(stats_docs)*100:.1f}%)")
    logger.info("")
    logger.info(f"   Average record count: {avg_record_count:.1f}")
    logger.info("")
    
    # Show example documents
    logger.info(" Example Stats Documents:")
    logger.info("")
    
    examples = await stats_collection.find().limit(3).to_list(3)
    for i, doc in enumerate(examples, 1):
        logger.info(f"   Example {i}:")
        logger.info(f"      Payer: {doc['payer']}")
        logger.info(f"      CPT Code: {doc['cpt_code']}")
        logger.info(f"      Record Count: {doc['record_count']}")
        logger.info(f"      Billed Amount (mean): ${doc. get('billed_amount_mean', 0):.2f}")
        logger.info(f"      Paid Amount (mean): ${doc.get('paid_amount_mean', 0):.2f}")
        logger.info("")
    
    logger.info("=" * 80)
    logger.success("Stats collection is ready for Check 3 validation!")
    logger.info("=" * 80)
    logger.info("")
    logger.info(" Next steps:")
    logger.info("   1. Verify stats in MongoDB Compass")
    logger.info("   2. Implement Check 3: Historical Stats Availability")
    logger.info("   3. Run readiness checks to validate stats quality")
    logger.info("")
    logger.info("=" * 80)
    
    # Close connection
    client.close()


if __name__ == "__main__":
    try:
        asyncio.run(generate_stats())
    except KeyboardInterrupt:
        logger. warning("\n\nOperation cancelled by user")
    except Exception as e:
        logger. error(f"\n\nUnexpected error: {e}")
        import traceback
        traceback.print_exc()