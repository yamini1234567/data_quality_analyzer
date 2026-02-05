from loguru import logger
from .base import BaseAnalyzer
from .models import Payer
import asyncio

class PayerAnalyzer(BaseAnalyzer):
   
    def __init__(self, db,filters=None):
        super().__init__(db,filters)
   
    async def get_payer_distribution(self):
        pipeline = [
            {
                "$group": {
                    "_id": "$payerMCO",
                    "total_claims": {"$sum": 1},
                    "total_closed": {
                        "$sum": {
                            "$cond": [
                                {"$in": ["$claimStatus", ["Closed"]]},
                                1,
                                0
                            ]
                        }
                    },
                    "total_denied": {
                        "$sum": {
                            "$cond": [
                                {"$in": ["$claimStatus", ["Denied"]]},
                                1,
                                0
                            ]
                        }
                    },
                    "avg_claim_amount": {"$avg": "$claimAmount"},
                    "avg_paid_amount": {"$avg": "$claimAmountPaid"},
                    "total_denied_amount": {
                        "$sum": {
                            "$cond": [
                                {"$in": ["$claimStatus", ["Denied"]]},  
                                "$claimAmount",
                                0
                            ]
                        }
                    }
                }
            },
            {
                "$addFields": {
                    "avg_denied_amount": {
                        "$cond": [
                            {"$gt": ["$total_denied", 0]},
                            {"$divide": ["$total_denied_amount", "$total_denied"]},
                            0
                        ]
                    }
                }
            },
            {"$sort": {"total_claims": -1}}
        ]
       
        return await self.aggregate(pipeline)
    
    async def get_unique_payers(self):
        pipeline = [
            {"$group": {"_id": "$payerMCO"}},
            {"$project": {"_id": 1}}
        ]
        result = await self.aggregate(pipeline)
        return [item["_id"] for item in result]
   
    async def run_all(self):
        logger.info("Starting Payer Analysis")
        (
            total_claims,
            unique_payers,
            payer_table
        ) = await asyncio.gather(
            self.get_total_claims(),
            self.get_unique_payers(),  
            self.get_payer_distribution()
        )
        
        self.total_claims = total_claims
        unique_payers_count = len(unique_payers)
        logger.info(f"Total claims: {self.total_claims:,}")
        logger.info(f"Unique payers: {unique_payers_count}")
        logger.info(f"Payer distribution calculated for {len(payer_table)} payers")
        
        top10_payers = payer_table[:10]
        least10_payers = payer_table[-10:]
        
        payer_result = Payer(
            total_claims=self.total_claims,
            unique_payers_count=unique_payers_count,
            all_payers=[
                {
                    "payer_name": p["_id"],
                    "total_claims": p["total_claims"],
                    "total_closed": p["total_closed"],
                    "total_denied": p["total_denied"],
                    "avg_claim_amount": p.get("avg_claim_amount", 0),
                    "avg_paid_amount": p.get("avg_paid_amount", 0),
                    "avg_denied_amount": p.get("avg_denied_amount", 0)
                    
                }
                for p in payer_table
            ],
            payer_summary= {
                "total_payers": len(payer_table),
                "top_10_payers": [
                    {
                        "payer_name": p["_id"],
                        "total_claims": p["total_claims"],
                        "total_closed": p["total_closed"],
                        "total_denied": p["total_denied"],
                        "avg_claim_amount": p.get("avg_claim_amount", 0),
                        "avg_paid_amount": p.get("avg_paid_amount", 0),
                        "avg_denied_amount": p.get("avg_denied_amount", 0)
                    }
                    for p in top10_payers
                ],
                "bottom_10_payers": [
                    {
                        "payer_name": p["_id"],
                        "total_claims": p["total_claims"] 
                    }
                    for p in least10_payers
                ]
            }
        )
        logger.info("Payer Analysis complete")
        return payer_result


async def payer_analysis(db, filters=None):
    analyzer = PayerAnalyzer(db, filters)
    return await analyzer.run_all()