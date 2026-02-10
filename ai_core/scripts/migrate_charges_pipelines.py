import asyncio
from datetime import datetime, timedelta
from loguru import logger
from shared.db import init_db, close_db
from ai_core.shared.models import MQuery, QueryType, Priority
 
 
CHARGES_QUERIES = [
    {
        "name": "zero_charges",
        "qtype": QueryType.charge,
        "desc": "Claims with zero charge amounts",
        "pipeline": [
            {"$unwind": "$charges"},
            {"$match": {"charges.amount": 0}},
            {"$group": {"_id": "$_id"}},
            {"$count": "total"}
        ],
        "priority": Priority.medium,
        "is_active": True
    },
    {
        "name": "negative_charges",
        "qtype": QueryType.charge,
        "desc": "Claims with negative charge amounts",
        "pipeline": [
            {"$unwind": "$charges"},
            {"$match": {"charges.amount": {"$lt": 0}}},
            {"$group": {"_id": "$_id"}},
            {"$count": "total"}
        ],
        "priority": Priority.high,
        "is_active": True
    },
    {
        "name": "empty_description",
        "qtype": QueryType.charge,
        "desc": "Charges with missing description",
        "pipeline": [
            {"$unwind": "$charges"},
            {
                "$match": {
                    "$or": [
                        {"charges.description": {"$exists": False}},
                        {"charges.description": ""}
                    ]
                }
            },
            {"$group": {"_id": "$_id"}},
            {"$count": "total"}
        ],
        "priority": Priority.low,
        "is_active": True
    },
    {
        "name": "extreme_units",
        "qtype": QueryType.charge,
        "desc": "Charges with unusually high units (>100)",
        "pipeline": [
            {"$unwind": "$charges"},
            {"$match": {"charges.unit": {"$gt": 100}}},
            {"$group": {"_id": "$_id"}},
            {"$count": "total"}
        ],
        "priority": Priority.medium,
        "is_active": True
    },
    {
        "name": "negative_units",
        "qtype": QueryType.charge,
        "desc": "Charges with negative units",
        "pipeline": [
            {"$unwind": "$charges"},
            {"$match": {"charges.unit": {"$lt": 0}}},
            {"$group": {"_id": "$_id"}},
            {"$count": "total"}
        ],
        "priority": Priority.high,
        "is_active": True
    },
    {
        "name": "zero_units_with_amount",
        "qtype": QueryType.charge,
        "desc": "Charges with zero units but positive amount",
        "pipeline": [
            {"$unwind": "$charges"},
            {
                "$match": {
                    "$or": [
                        {"charges.unit": 0},
                        {"charges.unit": {"$exists": False}},
                        {"charges.unit": None}
                    ],
                    "charges.amount": {"$gt": 0}
                }
            },
            {"$group": {"_id": "$_id"}},
            {"$count": "total"}
        ],
        "priority": Priority.medium,
        "is_active": True
    },
    {
        "name": "negative_unit_price",
        "qtype": QueryType.charge,
        "desc": "Charges with negative unit prices",
        "pipeline": [
            {"$unwind": "$charges"},
            {"$match": {"charges.unitPrice": {"$exists": True, "$lt": 0}}},
            {"$group": {"_id": "$_id"}},
            {"$count": "total"}
        ],
        "priority": Priority.high,
        "is_active": True
    },
    {
        "name": "missing_service_dates",
        "qtype": QueryType.charge,
        "desc": "Charges without service dates",
        "pipeline": [
            {"$unwind": "$charges"},
            {
                "$match": {
                    "$or": [
                        {"charges.serviceDate": {"$exists": False}},
                        {"charges.serviceDate": None},
                        {"charges.serviceDate": ""}
                    ]
                }
            },
            {"$group": {"_id": "$_id"}},
            {"$count": "total"}
        ],
        "priority": Priority.medium,
        "is_active": True
    },
    {
        "name": "future_service_dates",
        "qtype": QueryType.charge,
        "desc": "Charges with service dates in the future",
        "pipeline": [
            {"$unwind": "$charges"},
            {"$match": {"charges.serviceDate": {"$gt": datetime.now().strftime("%Y-%m-%d")}}},
            {"$group": {"_id": "$_id"}},
            {"$count": "total"}
        ],
        "priority": Priority.high,
        "is_active": True
    },
    {
        "name": "very_old_service_dates",
        "qtype": QueryType.charge,
        "desc": "Charges with service dates older than 5 years",
        "pipeline": [
            {"$unwind": "$charges"},
            {"$match": {"charges.serviceDate": {"$lt": (datetime.now() - timedelta(days=1825)).strftime("%Y-%m-%d")}}},
            {"$group": {"_id": "$_id"}},
            {"$count": "total"}
        ],
        "priority": Priority.low,
        "is_active": True
    },
    {
        "name": "missing_unit_prices",
        "qtype": QueryType.charge,
        "desc": "Charges with multiple units but missing unit prices",
        "pipeline": [
            {"$unwind": "$charges"},
            {
                "$match": {
                    "charges.unit": {"$exists": True, "$gt": 1},
                    "$or": [
                        {"charges.unitPrice": {"$exists": False}},
                        {"charges.unitPrice": None},
                        {"charges.unitPrice": 0}
                    ]
                }
            },
            {"$group": {"_id": "$_id"}},
            {"$count": "total"}
        ],
        "priority": Priority.medium,
        "is_active": True
    },
    {
        "name": "charge_remittance_missing",
        "qtype": QueryType.charge,
        "desc": "Charges with amount paid but missing remittance details",
        "pipeline": [
            {"$unwind": "$charges"},
            {
                "$match": {
                    "charges.amountPaid": {"$gt": 0},
                    "$or": [
                        {"charges.chargeRemittances": {"$size": 0}},
                        {"charges.chargeRemittances": {"$exists": False}}
                    ]
                }
            },
            {"$group": {"_id": "$_id"}},
            {"$count": "total"}
        ],
        "priority": Priority.medium,
        "is_active": True
    },
    {
        "name": "paid_exceeds_charge",
        "qtype": QueryType.charge,
        "desc": "Claims where amount paid exceeds charge amount",
        "pipeline": [
            {"$unwind": "$charges"},
            {"$match": {"$expr": {"$gt": ["$charges.amountPaid", "$charges.amount"]}}},
            {"$group": {"_id": "$_id"}},
            {"$count": "total"}
        ],
        "priority": Priority.high,
        "is_active": True
    },
    {
        "name": "paid_plus_adjustment_exceeds_charge",
        "qtype": QueryType.charge,
        "desc": "Claims where paid + adjustment exceeds charge amount",
        "pipeline": [
            {"$unwind": "$charges"},
            {
                "$match": {
                    "$expr": {
                        "$gt": [
                            {"$add": [{"$ifNull": ["$charges.amountPaid", 0]}, {"$ifNull": ["$charges.adjustmentAmount", 0]}]},
                            "$charges.amount"
                        ]
                    }
                }
            },
            {"$group": {"_id": "$_id"}},
            {"$count": "total"}
        ],
        "priority": Priority.high,
        "is_active": True
    },
    {
        "name": "unit_price_mismatch",
        "qtype": QueryType.charge,
        "desc": "Charges where unit * unitPrice doesn't match amount",
        "pipeline": [
            {"$unwind": "$charges"},
            {
                "$match": {
                    "charges.unitPrice": {"$exists": True, "$ne": None, "$gt": 0},
                    "charges.unit": {"$exists": True, "$ne": None, "$gt": 0}
                }
            },
            {
                "$project": {
                    "_id": 1,
                    "actual": "$charges.amount",
                    "expected": {"$multiply": ["$charges.unit", "$charges.unitPrice"]}
                }
            },
            {
                "$project": {
                    "_id": 1,
                    "diff": {"$subtract": [{"$max": ["$actual", "$expected"]}, {"$min": ["$actual", "$expected"]}]}
                }
            },
            {"$match": {"diff": {"$gt": 0.01}}},
            {"$group": {"_id": "$_id"}},
            {"$count": "total"}
        ],
        "priority": Priority.medium,
        "is_active": True
    },
    {
        "name": "duplicate_charges_same_date",
        "qtype": QueryType.charge,
        "desc": "Claims with duplicate charges on the same date",
        "pipeline": [
            {"$unwind": "$charges"},
            {
                "$group": {
                    "_id": {
                        "claim": "$_id",
                        "cpt": "$charges.cptHcpcs",
                        "date": "$charges.serviceDate",
                        "modifier": "$charges.modifier"
                    },
                    "count": {"$sum": 1}
                }
            },
            {"$match": {"count": {"$gt": 1}}},
            {"$group": {"_id": "$_id.claim"}},
            {"$count": "total"}
        ],
        "priority": Priority.medium,
        "is_active": True
    },
    {
        "name": "all_charges_zero",
        "qtype": QueryType.charge,
        "desc": "Claims where all charges have zero amount",
        "pipeline": [
            {"$addFields": {"totalAmount": {"$sum": "$charges.amount"}}},
            {"$match": {"charges": {"$exists": True, "$ne": []}, "totalAmount": 0}},
            {"$count": "total"}
        ],
        "priority": Priority.low,
        "is_active": True
    }
]
 
 
async def migrate():
    logger.info("Starting migration")
   
    client, db = await init_db()
   
    for q in CHARGES_QUERIES:
        exists = await MQuery.find_one({"name": q["name"]})
       
        if exists:
            logger.info(f"Skip: {q['name']}")
            continue
       
        query = MQuery(
            name=q["name"],
            qtype=q["qtype"],
            desc=q["desc"],
            pipeline=q["pipeline"],
            priority=q["priority"],
            is_active=q["is_active"]
        )
       
        await query.insert()
        logger.success(f"Added: {q['name']}")
   
    total = await MQuery.find({"qtype": QueryType.charge}).count()
    logger.success(f"Done! Total: {total}")
   
    await close_db(client)
 
 
asyncio.run(migrate())