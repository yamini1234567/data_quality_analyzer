import asyncio
from loguru import logger
from shared.db import init_db, close_db
from ai_core.shared.models import MQuery, QueryType, Priority


DIAGNOSIS_QUERIES = [
    {
        "name": "missing_diagnosis",
        "qtype": QueryType.diagnosis,
        "desc": "Claims with no diagnoses or empty diagnosis array",
        "pipeline": [
            {
                "$match": {
                    "$or": [
                        {"diagnoses": {"$size": 0}},
                        {"diagnoses": {"$exists": False}},
                        {"diagnoses": None}
                    ]
                }
            },
            {"$count": "total"}
        ],
        "priority": Priority.high,
        "is_active": True
    },
    {
        "name": "missing_primary_diagnosis",
        "qtype": QueryType.diagnosis,
        "desc": "Claims without a primary diagnosis",
        "pipeline": [
            {"$match": {"$nor": [{"diagnoses.isPrimaryDiagnosis": True}]}},
            {"$count": "total"}
        ],
        "priority": Priority.high,
        "is_active": True
    },
    {
        "name": "missing_diagnosis_description",
        "qtype": QueryType.diagnosis,
        "desc": "Diagnoses with missing or empty description",
        "pipeline": [
            {"$unwind": "$diagnoses"},
            {
                "$match": {
                    "$or": [
                        {"diagnoses.description": {"$exists": False}},
                        {"diagnoses.description": None},
                        {"diagnoses.description": ""}
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
        "name": "missing_diagnosis_code",
        "qtype": QueryType.diagnosis,
        "desc": "Diagnoses with missing or empty code",
        "pipeline": [
            {"$unwind": "$diagnoses"},
            {
                "$match": {
                    "$or": [
                        {"diagnoses.code": {"$exists": False}},
                        {"diagnoses.code": None},
                        {"diagnoses.code": ""}
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
        "name": "multiple_primary_diagnosis",
        "qtype": QueryType.diagnosis,
        "desc": "Claims with more than one primary diagnosis",
        "pipeline": [
            {"$unwind": "$diagnoses"},
            {"$match": {"diagnoses.isPrimaryDiagnosis": True}},
            {"$group": {"_id": "$_id", "count": {"$sum": 1}}},
            {"$match": {"count": {"$gt": 1}}},
            {"$count": "total"}
        ],
        "priority": Priority.high,
        "is_active": True
    },
    {
        "name": "missing_diagnosis_type",
        "qtype": QueryType.diagnosis,
        "desc": "Diagnoses with missing or empty type",
        "pipeline": [
            {"$unwind": "$diagnoses"},
            {
                "$match": {
                    "$or": [
                        {"diagnoses.type": {"$exists": False}},
                        {"diagnoses.type": None},
                        {"diagnoses.type": ""}
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
        "name": "missing_diagnosis_status",
        "qtype": QueryType.diagnosis,
        "desc": "Diagnoses with missing or empty status",
        "pipeline": [
            {"$unwind": "$diagnoses"},
            {
                "$match": {
                    "$or": [
                        {"diagnoses.status": {"$exists": False}},
                        {"diagnoses.status": None},
                        {"diagnoses.status": ""}
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
        "name": "diagnosis_order_mismatch",
        "qtype": QueryType.diagnosis,
        "desc": "Diagnoses with order=1 but not marked as primary",
        "pipeline": [
            {"$unwind": "$diagnoses"},
            {
                "$match": {
                    "$and": [
                        {"$or": [
                            {"diagnoses.order": "1"},
                            {"diagnoses.order": 1}
                        ]},
                        {"$or": [
                            {"diagnoses.isPrimaryDiagnosis": {"$ne": True}},
                            {"diagnoses.isPrimaryDiagnosis": {"$exists": False}}
                        ]}
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
        "name": "missing_diagnosis_order",
        "qtype": QueryType.diagnosis,
        "desc": "Diagnoses with missing or empty order",
        "pipeline": [
            {"$unwind": "$diagnoses"},
            {
                "$match": {
                    "$or": [
                        {"diagnoses.order": {"$exists": False}},
                        {"diagnoses.order": None},
                        {"diagnoses.order": ""}
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
        "name": "duplicate_diagnosis_order",
        "qtype": QueryType.diagnosis,
        "desc": "Claims with duplicate diagnosis order numbers",
        "pipeline": [
            {"$unwind": "$diagnoses"},
            {
                "$match": {
                    "diagnoses.order": {"$exists": True, "$ne": None, "$ne": ""}
                }
            },
            {
                "$group": {
                    "_id": {
                        "claim_id": "$_id",
                        "order": "$diagnoses.order"
                    },
                    "count": {"$sum": 1}
                }
            },
            {"$match": {"count": {"$gt": 1}}},
            {"$group": {"_id": "$_id.claim_id"}},
            {"$count": "total"}
        ],
        "priority": Priority.medium,
        "is_active": True
    },
    {
        "name": "missing_occurrence_date",
        "qtype": QueryType.diagnosis,
        "desc": "Diagnoses with missing occurrence date",
        "pipeline": [
            {"$unwind": "$diagnoses"},
            {
                "$match": {
                    "$or": [
                        {"diagnoses.occurrenceDate": {"$exists": False}},
                        {"diagnoses.occurrenceDate": None},
                        {"diagnoses.occurrenceDate": ""}
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
        "name": "missing_present_on_admission",
        "qtype": QueryType.diagnosis,
        "desc": "Diagnoses with missing present on admission indicator",
        "pipeline": [
            {"$unwind": "$diagnoses"},
            {
                "$match": {
                    "$or": [
                        {"diagnoses.presentOnAdmission": {"$exists": False}},
                        {"diagnoses.presentOnAdmission": None},
                        {"diagnoses.presentOnAdmission": ""}
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
        "name": "invalid_icd10_format",
        "qtype": QueryType.diagnosis,
        "desc": "Diagnoses with invalid ICD-10 code format",
        "pipeline": [
            {"$unwind": "$diagnoses"},
            {
                "$match": {
                    "diagnoses.code": {"$exists": True, "$ne": None, "$ne": ""},
                    "$expr": {
                        "$not": {
                            "$regexMatch": {
                                "input": "$diagnoses.code",
                                "regex": "^[A-Z][0-9A-Z]{2,6}$"
                            }
                        }
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
        "name": "primary_diagnosis_not_order_1",
        "qtype": QueryType.diagnosis,
        "desc": "Primary diagnoses that don't have order=1",
        "pipeline": [
            {"$unwind": "$diagnoses"},
            {
                "$match": {
                    "diagnoses.isPrimaryDiagnosis": True,
                    "$and": [
                        {"diagnoses.order": {"$ne": "1"}},
                        {"diagnoses.order": {"$ne": 1}}
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
        "name": "invalid_diagnosis_status",
        "qtype": QueryType.diagnosis,
        "desc": "Diagnoses with invalid status codes (not A/W/I/R/D)",
        "pipeline": [
            {"$unwind": "$diagnoses"},
            {
                "$match": {
                    "diagnoses.status": {
                        "$exists": True,
                        "$ne": None,
                        "$ne": "",
                        "$nin": ["A", "W", "I", "R", "D"]
                    }
                }
            },
            {"$group": {"_id": "$_id"}},
            {"$count": "total"}
        ],
        "priority": Priority.medium,
        "is_active": True
    },
    {
        "name": "invalid_diagnosis_type",
        "qtype": QueryType.diagnosis,
        "desc": "Diagnoses with invalid type codes",
        "pipeline": [
            {"$unwind": "$diagnoses"},
            {
                "$match": {
                    "diagnoses.type": {
                        "$exists": True,
                        "$ne": None,
                        "$ne": "",
                        "$nin": ["ABK", "ABF", "BK", "BF", "PRIMARY", "SECONDARY"]
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


async def migrate():
    logger.info("Starting diagnosis migration")
    
    client, db = await init_db()
    
    for q in DIAGNOSIS_QUERIES:
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
    
    total = await MQuery.find({"qtype": QueryType.diagnosis}).count()
    logger.success(f"Done! Total: {total}")
    
    await close_db(client)
    
asyncio.run(migrate())