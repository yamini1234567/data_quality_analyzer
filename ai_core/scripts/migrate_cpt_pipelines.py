import asyncio
from shared.db import init_db, close_db
from ai_core.shared.models import MQuery, QueryType, Priority
from loguru import logger


CPT_QUERIES = [
    {
        "name": "check_invalid_cpt_format",
        "qtype": QueryType.cpt, 
        "desc": "CPT codes that don't have exactly 5 characters",
        "pipeline": [
            {"$unwind": "$charges"},
            {
                "$match": {
                    "charges.cptHcpcs": {"$exists": True, "$ne": None, "$ne": ""}
                }
            },
            {
                "$project": {
                    "_id": 1,
                    "cptLength": {"$strLenCP": "$charges.cptHcpcs"}
                }
            },
            {"$match": {"cptLength": {"$ne": 5}}},
            {"$group": {"_id": "$_id"}},
            {"$count": "total"}
        ],
        "priority": Priority.high,
        "is_active": True
    },
    {
        "name": "check_invalid_modifier_codes",
        "qtype": QueryType.cpt, 
        "desc": "Charges with invalid CPT modifier codes",
        "pipeline": [
            {"$unwind": "$charges"},
            {
                "$match": {
                    "charges.modifier": {
                        "$exists": True,
                        "$ne": None,
                        "$ne": "",
                        "$nin": [
                            "22", "24", "25", "26", "27", "32", "33", "47",
                            "50", "51", "52", "53", "54", "55", "56", "57", "58", "59",
                            "62", "66", "73", "74", "76", "77", "78", "79",
                            "80", "81", "82", "90", "91", "95", "99",
                            "AA", "GA", "GC", "GT", "GY", "GZ",
                            "JW", "JZ",
                            "LT", "RT", "LC", "LD",
                            "TC", "QW", "QX", "QY", "QZ",
                            "XE", "XP", "XS", "XU"
                        ]
                    }
                }
            },
            {"$group": {"_id": "$_id"}},
            {"$count": "total"}
        ],
        "priority": Priority.medium,
        "is_active": True
    }
]


async def migrate_cpt_pipelines():
    client, db = await init_db()
    
    inserted = 0
    skipped = 0
    
    for query_data in CPT_QUERIES:
        existing = await MQuery.find_one({"name": query_data["name"]})
        
        if existing:
            skipped += 1
            continue
        
        query = MQuery(**query_data)
        await query.insert()
        inserted += 1
    
    logger.info(f"Inserted: {inserted}, Skipped: {skipped}")
    
    await close_db(client)
    
asyncio.run(migrate_cpt_pipelines())