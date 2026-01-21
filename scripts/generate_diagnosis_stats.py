"""Generate diagnosis-CPT pattern statistics for Additional Charge feature."""
import asyncio
import os
from datetime import datetime, timezone
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
DATABASE_NAME = os.getenv("MONGODB_DB_NAME", "rcm_test_db")


async def generate_stats():
    """Generate diagnosis-CPT pattern stats from claims."""
    
    logger.info("="*80)
    logger.info("DIAGNOSIS-CPT PATTERN STATS GENERATION")
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
    
    claims = db["claims"]
    stats = db["stats"]
    
    # Check claims data
    logger.info("Checking claims data...")
    total = await claims.count_documents({})
    with_dx = await claims.count_documents({"diagnoses": {"$exists": True, "$ne": []}})
    
    if total == 0:
        logger.error("  No claims found!")
        client.close()
        return
    
    logger.success(f"  Found {total:,} claims ({with_dx:,} with diagnoses)")
    logger.info("")
    
    # Check existing stats
    logger.info("Checking existing diagnosis-CPT stats...")
    existing = await stats.count_documents({"diagnosis_code": {"$exists": True}})
    
    if existing > 0:
        logger.warning(f"  Found {existing:,} existing stats")
        response = input("  Delete and recreate? (yes/no): ")
        logger.info("")
        
        if response.lower() != 'yes':
            logger.info("  Cancelled")
            client.close()
            return
        
        result = await stats.delete_many({"diagnosis_code": {"$exists": True}})
        logger.success(f"  Deleted {result.deleted_count:,} stats")
        logger.info("")
    
    # Build aggregation pipeline
    logger.info("Running aggregation pipeline...")
    logger.info("  Grouping by payer + diagnosis + CPT code...")
    
    pipeline = [
        {"$match": {
            "diagnoses": {"$exists": True, "$ne": []},
            "payerMCO": {"$exists": True, "$ne": None, "$ne": ""}
        }},
        {"$unwind": "$diagnoses"},
        {"$unwind": "$charges"},
        {"$match": {
            "charges.cptHcpcs": {"$exists": True, "$ne": None, "$ne": ""},
            "diagnoses.code": {"$exists": True, "$ne": None, "$ne": ""}
        }},
        {"$group": {
            "_id": {
                "payer": "$payerMCO",
                "diagnosis": "$diagnoses.code",
                "cpt": "$charges.cptHcpcs",
                "modifier": "$charges.modifier",
                "rev_code": "$charges.revCode"
            },
            "count": {"$sum": 1},
            "billed_vals": {"$push": "$charges.amount"},
            "paid_vals": {"$push": "$charges.amountPaid"},
            "adjusted_vals": {"$push": "$charges.adjustmentAmount"}
        }},
        {"$project": {
            "_id": 0,
            "payer": "$_id.payer",
            "diagnosis_code": "$_id.diagnosis",
            "cpt_code": "$_id.cpt",
            "modifier": "$_id.modifier",
            "rev_code": "$_id.rev_code",
            "record_count": "$count",
            "billed": {"$avg": "$billed_vals"},
            "paid": {"$avg": "$paid_vals"},
            "adjusted": {"$avg": "$adjusted_vals"},
            "billed_min": {"$min": "$billed_vals"},
            "billed_max": {"$max": "$billed_vals"},
            "paid_min": {"$min": "$paid_vals"},
            "paid_max": {"$max": "$paid_vals"},
            "last_updated": datetime.now(timezone.utc),
            "stats_type": "diagnosis_cpt_pattern"
        }},
        {"$sort": {"payer": 1, "diagnosis_code": 1, "cpt_code": 1}}
    ]
    
    try:
        cursor = claims.aggregate(pipeline, allowDiskUse=True)
        docs = await cursor.to_list(length=None)
        
        if not docs:
            logger.error("  No stats generated!")
            client.close()
            return
        
        logger.success(f"  Generated {len(docs):,} stat documents")
        logger.info("")
    except Exception as e:
        logger.error(f"  Aggregation failed: {e}")
        client.close()
        return
    
    # Insert into collection
    logger.info("Inserting stats into database...")
    
    try:
        # Drop conflicting index if exists
        indexes = await stats.list_indexes().to_list(length=None)
        for idx in indexes:
            if idx.get('key') == {'payer': 1, 'cpt_code': 1} and idx.get('unique'):
                await stats.drop_index(idx['name'])
                logger.info("  Dropped conflicting index")
        
        result = await stats.insert_many(docs)
        logger.success(f"  Inserted {len(result.inserted_ids):,} documents")
        logger.info("")
    except Exception as e:
        logger.error(f"  Insert failed: {e}")
        client.close()
        return
    
    # Create indexes
    logger.info("Creating indexes...")
    try:
        await stats.create_index([("payer", 1), ("diagnosis_code", 1), ("cpt_code", 1)])
        await stats.create_index([("diagnosis_code", 1)])
        logger.success("  Indexes created")
        logger.info("")
    except Exception as e:
        logger.warning(f"  Index warning: {e}")
        logger.info("")
    
    # Summary
    logger.info("="*80)
    logger.success("STATS GENERATION COMPLETE")
    logger.info("="*80)
    logger.info("")
    
    high = await stats.count_documents({"diagnosis_code": {"$exists": True}, "record_count": {"$gte": 5}})
    medium = await stats.count_documents({"diagnosis_code": {"$exists": True}, "record_count": {"$gte": 3, "$lt": 5}})
    low = await stats.count_documents({"diagnosis_code": {"$exists": True}, "record_count": {"$lt": 3}})
    
    payers = await stats.distinct("payer", {"diagnosis_code": {"$exists": True}})
    diagnoses = await stats.distinct("diagnosis_code")
    cpts = await stats.distinct("cpt_code", {"diagnosis_code": {"$exists": True}})
    
    with_paid = await stats.count_documents({"diagnosis_code": {"$exists": True}, "paid": {"$gt": 0}})
    
    logger.info("Summary:")
    logger.info(f"  Total documents: {len(docs):,}")
    logger.info(f"  Unique payers: {len(payers)}")
    logger.info(f"  Unique diagnoses: {len(diagnoses)}")
    logger.info(f"  Unique CPT codes: {len(cpts)}")
    logger.info("")
    logger.info("  Quality:")
    logger.info(f"    High (≥5 records): {high:,} ({high/len(docs)*100:.1f}%)")
    logger.info(f"    Medium (3-4): {medium:,} ({medium/len(docs)*100:.1f}%)")
    logger.info(f"    Low (<3): {low:,} ({low/len(docs)*100:.1f}%)")
    logger.info("")
    logger.info(f"  Stats with paid > 0: {with_paid:,} ({with_paid/len(docs)*100:.1f}%)")
    logger.info("")
    
    # Examples
    examples = await stats.find({"diagnosis_code": {"$exists": True}}).limit(3).to_list(3)
    logger.info("Examples:")
    for i, doc in enumerate(examples, 1):
        logger.info(f"  {i}. {doc['payer']} | Dx:{doc['diagnosis_code']} | CPT:{doc['cpt_code']}")
        logger.info(f"     Count:{doc['record_count']} | Billed:${doc.get('billed',0):.2f} | Paid:${doc.get('paid',0):.2f}")
    
    logger.info("")
    logger.info("="*80)
    logger.success("Ready for Additional Charge readiness checks!")
    logger.info("="*80)
    
    client.close()


if __name__ == "__main__":
    try:
        asyncio.run(generate_stats())
    except KeyboardInterrupt:
        logger.warning("\nCancelled by user")
    except Exception as e:
        logger.error(f"\nError: {e}")
