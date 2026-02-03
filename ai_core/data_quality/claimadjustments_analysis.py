from loguru import logger
from .models import  Adjustment
from .base import BaseAnalyzer
 
class AdjustmentAnalyzer(BaseAnalyzer):
   
    def __init__(self, db):
        super().__init__(db)
   
    async def check_negative_adjustments(self):
        logger.info("Checking for negative adjustments")
        pipeline = [
            {"$match": {"claimAdjAmount": {"$lt": 0}}},
            {"$count": "total"}
        ]
        return await self.run_pipeline(pipeline)
 
    async def check_adjustment_greater_than_claim(self):
        pipeline = [
            {"$match": {"$expr": {"$gt": ["$claimAdjAmount", "$claimAmount"]}}},
            {"$count": "total"}
        ]
        return await self.run_pipeline(pipeline)
   
    async def check_excessive_adjustments(self):
        logger.info("Checking for excessive adjustments(>50%)")
        pipeline = [
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
        ]
        return await self.run_pipeline(pipeline)
 
    async def check_missing_adjustment_details(self):
        logger.info("Checking for missing adjustment details")
        pipeline = [
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
        ]
        return await self.run_pipeline(pipeline)
 
    async def check_charge_negative_adjustments(self):
        logger.info("Checking for charge negative adjustments")
        pipeline = [
            {"$unwind": "$charges"},
            {"$match": {"charges.adjustmentAmount": {"$lt": 0}}},
            {"$group": {"_id": "$_id"}},
            {"$count": "total"}
        ]
        return await self.run_pipeline(pipeline)
 
    async def check_charge_adjustment_exceeds_amount(self):
        logger.info("Checking for charge adjustment > charge amount")
        pipeline = [
            {"$unwind": "$charges"},
            {
                "$match": {
                    "$expr": {"$gt": ["$charges.adjustmentAmount", "$charges.amount"]}
                }
            },
            {"$group": {"_id": "$_id"}},
            {"$count": "total"}
        ]
        return await self.run_pipeline(pipeline)
 
    async def check_charges_missing_adjustment_details(self):
        logger.info("Checking for charges missing adjustment details")
        pipeline = [
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
        ]
        return await self.run_pipeline(pipeline)
 
    async def run_all(self):
        self.total_claims = await self.get_total_claims()
        claims_with_adjustments = await self.count_documents({"claimAdjAmount": {"$gt": 0}})
        logger.info(f"Total Claims: {self.total_claims:,}")
        logger.info(f"Claims With Adjustments: {claims_with_adjustments:,}")
       
        return Adjustment(
            total_claims=self.total_claims,
            claims_with_adjustments=claims_with_adjustments,
            negative_adjustments=await self.check_negative_adjustments(),
            adjustment_greater_than_claim=await self.check_adjustment_greater_than_claim(),
            adjustment_exceeds_50_percent=await self.check_excessive_adjustments(),
            missing_adjustment_details=await self.check_missing_adjustment_details(),
            charge_negative_adjustments=await self.check_charge_negative_adjustments(),
            charge_adjustment_exceeds_amount=await self.check_charge_adjustment_exceeds_amount(),
            charges_missing_adjustment_details=await self.check_charges_missing_adjustment_details()
        )
 
async def adjustment_analysis(db):
    analyzer = AdjustmentAnalyzer(db)
    return await analyzer.run_all()