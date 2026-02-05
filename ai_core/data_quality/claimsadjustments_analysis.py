from loguru import logger
from ai_core.data_quality.models import Adjustment, AdjustmentValidation, DataCount
from .base import BaseAnalyzer
import asyncio

class AdjustmentAnalyzer(BaseAnalyzer):
   
    def __init__(self, db,filters=None):
        super().__init__(db,filters)
        
   # To Check for Negative Adjustments 
   
    async def check_negative_adjustments(self):
        pipeline = [
            {"$match": {"claimAdjAmount": {"$lt": 0}}},
            {"$count": "total"}
        ]
        return await self.run_pipeline(pipeline)
 
     # To Check for claimAdjAmount Greater than Claim Amount
 
    async def check_adjustment_greater_than_claim(self):
        pipeline = [
            {"$match": {"$expr": {"$gt": ["$claimAdjAmount", "$claimAmount"]}}},
            {"$count": "total"}
        ]
        return await self.run_pipeline(pipeline)
   
        # To Check for claimAdjAmount exceeding 50% of Claim Amount
      
    async def check_excessive_adjustments(self):
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
 
      # To Check for Missing Adjustment Details
 
    async def check_missing_adjustment_details(self):
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
 
     # To Check whether adustmentAmount in charges is negative
 
    async def check_charge_negative_adjustments(self):
        pipeline = [
            {"$unwind": "$charges"},
            {"$match": {"charges.adjustmentAmount": {"$lt": 0}}},
            {"$group": {"_id": "$_id"}},
            {"$count": "total"}
        ]
        return await self.run_pipeline(pipeline)
    
        # To Check whether adjustmentAmount in charges exceeds charge amount
 
    async def check_charge_adjustment_exceeds_amount(self):
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
    
    # To Check for charges with missing adjustment details
 
    async def check_charges_missing_adjustment_details(self):
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
    
    # To Check for mismatch between charge adjustmentAmount and sum of chargeAdjustments.adjAmount
    
    async def check_chargeadjustment_sum_mismatch(self):
        
        pipeline=[
                {"$unwind": "$charges"},
         {
        "$addFields": {
            "sumOfAdjRecords": {
                "$sum": "$charges.chargeAdjustments.adjAmount"
            }
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
    {"$count": "total"}]
        return await self.run_pipeline(pipeline)
    
    # To Check for mismatch between claimAdjAmount and sum of claimAdjustments.adjAmount
    
    async def check_claim_adj_records_sum_mismatch(self):
        
         pipeline = [
        {
            "$addFields": {
                "sumClaimAdjRecords": {
                    "$sum": "$claimAdjustments.adjAmount"
                }
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
    ]
    
         return await self.run_pipeline(pipeline)
     

 
    async def run_all(self):
        self.total_claims = await self.get_total_claims()
        claims_with_adjustments = await self.claims.count_documents(
            self.filter | {"claimAdjAmount": {"$gt": 0}})
        logger.info(f"Total Claims: {self.total_claims:,}")
        logger.info(f"Claims With Adjustments: {claims_with_adjustments:,}")
        (
        negative_adjustments,
        adjustment_greater_than_claim,
        adjustment_exceeds_50_percent,
        missing_adjustment_details,
        charge_negative_adjustments,
        charge_adjustment_exceeds_amount,
        charges_missing_adjustment_details,
        chargeadjustment_sum_mismatch,
        claim_adj_records_sum_mismatch
        ) = await asyncio.gather(
        self.check_negative_adjustments(),
        self.check_adjustment_greater_than_claim(),
        self.check_excessive_adjustments(),
        self.check_missing_adjustment_details(),
        self.check_charge_negative_adjustments(),
        self.check_charge_adjustment_exceeds_amount(),
        self.check_charges_missing_adjustment_details(),
        self.check_chargeadjustment_sum_mismatch(),
        self.check_claim_adj_records_sum_mismatch()
       )
       
        issues = AdjustmentValidation(
            negative_adjustments=negative_adjustments,
            adjustment_greater_than_claim=adjustment_greater_than_claim,
            adjustment_exceeds_50_percent=adjustment_exceeds_50_percent,
            missing_adjustment_details=missing_adjustment_details,
            charge_negative_adjustments=charge_negative_adjustments,
            charge_adjustment_exceeds_amount=charge_adjustment_exceeds_amount,
            charges_missing_adjustment_details=charges_missing_adjustment_details,
            chargeadjustment_sum_mismatch=chargeadjustment_sum_mismatch,
            claim_adj_records_sum_mismatch=claim_adj_records_sum_mismatch
        )
        
        return Adjustment(
            total_claims=self.total_claims,
            claims_with_adjustments=claims_with_adjustments,
            issues=issues
        )
 
async def adjustment_analysis(db, filters=None):
    analyzer = AdjustmentAnalyzer(db, filters)
    return await analyzer.run_all()
 