import asyncio
from shared.db import init_db, close_db
from ai_core.shared.models import MQuery, QueryType, Priority
from loguru import logger


CLAIMS_QUERIES = [
    {
        "name": "check_denied_with_payment",
        "qtype": QueryType.claim,
        "desc": "Denied claims that have received payment",
        "pipeline": [
            {"$match": {"claimStatus": "Denied", "claimAmountPaid": {"$gt": 0}}},
            {"$count": "total"}
        ],
        "priority": Priority.high,
        "is_active": True
    },
    {
        "name": "check_denied_without_remittances",
        "qtype": QueryType.claim,
        "desc": "Denied claims without remittance information",
        "pipeline": [
            {
                "$match": {
                    "claimStatus": "Denied",
                    "$or": [
                        {"chargeRemittances": {"$exists": False}},
                        {"chargeRemittances": []},
                        {"chargeRemittances": None}
                    ]
                }
            },
            {"$count": "total"}
        ],
        "priority": Priority.medium,
        "is_active": True
    },
    {
        "name": "check_denied_with_overpayment",
        "qtype": QueryType.claim,
        "desc": "Denied claims where paid amount exceeds claim amount",
        "pipeline": [
            {
                "$match": {
                    "claimStatus": "Denied",
                    "$expr": {"$gt": ["$claimAmountPaid", "$claimAmount"]}
                }
            },
            {"$count": "total"}
        ],
        "priority": Priority.high,
        "is_active": True
    },
    {
        "name": "check_open_with_payment",
        "qtype": QueryType.claim,
        "desc": "Open claims that have received payment",
        "pipeline": [
            {"$match": {"claimStatus": "Open", "claimAmountPaid": {"$gt": 0}}},
            {"$count": "total"}
        ],
        "priority": Priority.medium,
        "is_active": True
    },
    {
        "name": "check_paidamount_greater_than_claimamount",
        "qtype": QueryType.claim,
        "desc": "Claims where paid amount exceeds claim amount",
        "pipeline": [
            {"$match": {"$expr": {"$gt": ["$claimAmountPaid", "$claimAmount"]}}},
            {"$count": "total"}
        ],
        "priority": Priority.high,
        "is_active": True
    },
    {
        "name": "check_adjamount_greater_than_claimamount",
        "qtype": QueryType.claim,
        "desc": "Claims where adjustment amount exceeds claim amount",
        "pipeline": [
            {"$match": {"$expr": {"$gt": ["$claimAdjAmount", "$claimAmount"]}}},
            {"$count": "total"}
        ],
        "priority": Priority.high,
        "is_active": True
    },
    {
        "name": "check_paid_plus_adjustment_exceeds_claim",
        "qtype": QueryType.claim,
        "desc": "Claims where paid + adjustment exceeds claim amount",
        "pipeline": [
            {
                "$match": {
                    "$expr": {
                        "$gt": [
                            {
                                "$add": [
                                    {"$ifNull": ["$claimAmountPaid", 0]},
                                    {"$ifNull": ["$claimAdjAmount", 0]}
                                ]
                            },
                            "$claimAmount"
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
        "name": "check_closed_with_zero_amtpaid_and_adj",
        "qtype": QueryType.claim,
        "desc": "Closed claims with zero paid and zero adjustment amounts",
        "pipeline": [
            {
                "$match": {
                    "claimStatus": "Closed",
                    "claimAmountPaid": 0,
                    "claimAdjAmount": 0
                }
            },
            {"$count": "total"}
        ],
        "priority": Priority.medium,
        "is_active": True
    },
    {
        "name": "check_closed_with_remaining_balanceamt",
        "qtype": QueryType.claim,
        "desc": "Closed claims with remaining balance",
        "pipeline": [
            {
                "$match": {
                    "claimStatus": "Closed",
                    "$expr": {
                        "$gt": [
                            {
                                "$subtract": [
                                    "$claimAmount",
                                    {
                                        "$add": [
                                            {"$ifNull": ["$claimAmountPaid", 0]},
                                            {"$ifNull": ["$claimAdjAmount", 0]}
                                        ]
                                    }
                                ]
                            },
                            0
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
        "name": "check_duplicate_claims",
        "qtype": QueryType.claim,
        "desc": "Duplicate claims with same claimId",
        "pipeline": [
            {"$group": {"_id": "$claimId", "count": {"$sum": 1}}},
            {"$match": {"count": {"$gt": 1}}},
            {"$group": {"_id": None, "total": {"$sum": "$count"}}}
        ],
        "priority": Priority.high,
        "is_active": True
    },
    {
        "name": "check_claim_sum_mismatch",
        "qtype": QueryType.claim,
        "desc": "Claims where sum of charge amounts doesn't match claim amount",
        "pipeline": [
            {"$unwind": "$charges"},
            {
                "$group": {
                    "_id": "$_id",
                    "claimAmount": {"$first": "$claimAmount"},
                    "totalCharges": {"$sum": "$charges.amount"}
                }
            },
            {
                "$match": {
                    "$expr": {
                        "$ne": [
                            {"$round": ["$claimAmount", 2]},
                            {"$round": ["$totalCharges", 2]}
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
        "name": "check_claim_amount_paid_sum_mismatch",
        "qtype": QueryType.claim,
        "desc": "Claims where sum of charge payments doesn't match claim amount paid",
        "pipeline": [
            {"$unwind": "$charges"},
            {
                "$group": {
                    "_id": "$_id",
                    "claimAmountPaid": {"$first": "$claimAmountPaid"},
                    "totalChargesPaid": {"$sum": "$charges.amountPaid"}
                }
            },
            {
                "$match": {
                    "$expr": {
                        "$ne": [
                            {"$round": ["$claimAmountPaid", 2]},
                            {"$round": ["$totalChargesPaid", 2]}
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
        "name": "check_claim_adj_amount_sum_mismatch",
        "qtype": QueryType.claim,
        "desc": "Claims where sum of charge adjustments doesn't match claim adjustment amount",
        "pipeline": [
            {"$unwind": "$charges"},
            {
                "$group": {
                    "_id": "$_id",
                    "claimAdjAmount": {"$first": "$claimAdjAmount"},
                    "totalChargesAdj": {"$sum": "$charges.adjustmentAmount"}
                }
            },
            {
                "$match": {
                    "$expr": {
                        "$ne": [
                            {"$round": ["$claimAdjAmount", 2]},
                            {"$round": ["$totalChargesAdj", 2]}
                        ]
                    }
                }
            },
            {"$count": "total"}
        ],
        "priority": Priority.high,
        "is_active": True
    }
]


async def migrate_claims_pipelines():
    client, db = await init_db()
    
    inserted = 0
    skipped = 0
    
    for query_data in CLAIMS_QUERIES:
        existing = await MQuery.find_one({"name": query_data["name"]})
        
        if existing:
            skipped += 1
            continue
        
        query = MQuery(**query_data)
        await query.insert()
        inserted += 1
    
    logger.info(f"Inserted: {inserted}, Skipped: {skipped}")
    
    await close_db(client)
    
asyncio.run(migrate_claims_pipelines())