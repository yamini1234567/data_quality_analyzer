import asyncio
from shared.db import init_db, close_db
from ai_core.shared.models import MQuery, QueryType, Priority
from loguru import logger


ADJUSTMENT_QUERIES = [
    {
        "name": "check_negative_adjustments",
        "qtype": QueryType.adjustment,
        "desc": "Claims with negative adjustment amounts",
        "pipeline": [
            {"$match": {"claimAdjAmount": {"$lt": 0}}},
            {"$count": "total"}
        ],
        "priority": Priority.medium,
        "is_active": True
    },
    {
        "name": "check_adjustment_greater_than_claim",
        "qtype": QueryType.adjustment,
        "desc": "Claims where adjustment amount exceeds claim amount",
        "pipeline": [
            {"$match": {"$expr": {"$gt": ["$claimAdjAmount", "$claimAmount"]}}},
            {"$count": "total"}
        ],
        "priority": Priority.high,
        "is_active": True
    },
    {
        "name": "check_excessive_adjustments",
        "qtype": QueryType.adjustment,
        "desc": "Claims where adjustment exceeds 50% of claim amount",
        "pipeline": [
            {
                "$match": {
                    "claimAdjAmount": {"$gt": 0},
                    "$expr": {
                        "$gt": [
                            "$claimAdjAmount",
                            {"$multiply": ["$claimAmount", 0.5]}
                        ]
                    }
                }
            },
            {"$count": "total"}
        ],
        "priority": Priority.medium,
        "is_active": True
    },
    {
        "name": "check_missing_adjustment_details",
        "qtype": QueryType.adjustment,
        "desc": "Claims with adjustment amount but missing adjustment details",
        "pipeline": [
            {
                "$match": {
                    "claimAdjAmount": {"$gt": 0},
                    "$or": [
                        {"claimAdjustments": {"$size": 0}},
                        {"claimAdjustments": {"$exists": False}}
                    ]
                }
            },
            {"$count": "total"}
        ],
        "priority": Priority.high,
        "is_active": True
    },
    {
        "name": "check_claim_adj_sum_mismatch",
        "qtype": QueryType.adjustment,
        "desc": "Claims where sum of adjustment records doesn't match total adjustment",
        "pipeline": [
            {
                "$addFields": {
                    "sumClaimAdjRecords": {"$sum": "$claimAdjustments.adjAmount"}
                }
            },
            {
                "$match": {
                    "$expr": {
                        "$ne": [
                            {"$round": ["$claimAdjAmount", 2]},
                            {"$round": ["$sumClaimAdjRecords", 2]}
                        ]
                    }
                }
            },
            {"$count": "total"}
        ],
        "priority": Priority.high,
        "is_active": True
    },
    {
        "name": "check_charge_negative_adjustments",
        "qtype": QueryType.adjustment,
        "desc": "Charges with negative adjustment amounts",
        "pipeline": [
            {"$unwind": "$charges"},
            {"$match": {"charges.adjustmentAmount": {"$lt": 0}}},
            {"$group": {"_id": "$_id"}},
            {"$count": "total"}
        ],
        "priority": Priority.medium,
        "is_active": True
    },
    {
        "name": "check_charge_adjustment_exceeds_amount",
        "qtype": QueryType.adjustment,
        "desc": "Charges where adjustment exceeds charge amount",
        "pipeline": [
            {"$unwind": "$charges"},
            {
                "$match": {
                    "$expr": {"$gt": ["$charges.adjustmentAmount", "$charges.amount"]}
                }
            },
            {"$group": {"_id": "$_id"}},
            {"$count": "total"}
        ],
        "priority": Priority.high,
        "is_active": True
    },
    {
        "name": "check_charges_missing_adjustment_details",
        "qtype": QueryType.adjustment,
        "desc": "Charges with adjustment amount but missing adjustment details",
        "pipeline": [
            {"$unwind": "$charges"},
            {
                "$match": {
                    "charges.adjustmentAmount": {"$gt": 0},
                    "$or": [
                        {"charges.chargeAdjustments": {"$size": 0}},
                        {"charges.chargeAdjustments": {"$exists": False}}
                    ]
                }
            },
            {"$group": {"_id": "$_id"}},
            {"$count": "total"}
        ],
        "priority": Priority.high,
        "is_active": True
    },
    {
        "name": "check_chargeadjustment_sum_mismatch",
        "qtype": QueryType.adjustment,
        "desc": "Charges where sum of adjustment records doesn't match total adjustment",
        "pipeline": [
            {"$unwind": "$charges"},
            {
                "$addFields": {
                    "sumOfAdjRecords": {"$sum": "$charges.chargeAdjustments.adjAmount"}
                }
            },
            {
                "$match": {
                    "$expr": {
                        "$ne": [
                            {"$round": ["$charges.adjustmentAmount", 2]},
                            {"$round": ["$sumOfAdjRecords", 2]}
                        ]
                    }
                }
            },
            {"$group": {"_id": "$_id"}},
            {"$count": "total"}
        ],
        "priority": Priority.high,
        "is_active": True
    }
]


async def migrate_adjustment_pipelines():
    client, db = await init_db()
    
    inserted = 0
    skipped = 0
    
    for query_data in ADJUSTMENT_QUERIES:
        existing = await MQuery.find_one({"name": query_data["name"]})
        
        if existing:
            skipped += 1
            continue
        
        query = MQuery(**query_data)
        await query.insert()
        inserted += 1
    
    logger.info(f"Inserted: {inserted}, Skipped: {skipped}")
    
    await close_db(client)
    
asyncio.run(migrate_adjustment_pipelines())