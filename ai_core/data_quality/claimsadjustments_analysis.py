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
    
    async def run_claim_level_checks_combined(self):
        pipeline = [
            {
                "$facet": {
                    "negative_adjustments": [
                        {"$match": {"claimAdjAmount": {"$lt": 0}}},
                        {"$count": "total"}
                    ],
                    "adjustment_greater_than_claim": [
                        {"$match": {"$expr": {"$gt": ["$claimAdjAmount", "$claimAmount"]}}},
                        {"$count": "total"}
                    ],
                    "excessive_adjustments": [
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
                    "missing_adjustment_details": [
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
                    "claim_adj_sum_mismatch": [
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
                    ]
                }
            }
        ]
        
        results = await self.aggregate(pipeline)
        facet_result = results[0]
        
        return {
            "negative_adjustments": self.facet_to_datacount(facet_result, "negative_adjustments"),
            "adjustment_greater_than_claim": self.facet_to_datacount(facet_result, "adjustment_greater_than_claim"),
            "excessive_adjustments": self.facet_to_datacount(facet_result, "excessive_adjustments"),
            "missing_adjustment_details": self.facet_to_datacount(facet_result, "missing_adjustment_details"),
            "claim_adj_sum_mismatch": self.facet_to_datacount(facet_result, "claim_adj_sum_mismatch")
        }
    
    async def run_charge_level_checks_combined(self):
        pipeline = [
            {"$unwind": "$charges"},
            {
                "$facet": {
                    "charge_negative_adjustments": [
                        {"$match": {"charges.adjustmentAmount": {"$lt": 0}}},
                        {"$group": {"_id": "$_id"}},
                        {"$count": "total"}
                    ],
                    "charge_adjustment_exceeds_amount": [
                        {
                            "$match": {
                                "$expr": {"$gt": ["$charges.adjustmentAmount", "$charges.amount"]}
                            }
                        },
                        {"$group": {"_id": "$_id"}},
                        {"$count": "total"}
                    ],
                    "charges_missing_adjustment_details": [
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
                    "chargeadjustment_sum_mismatch": [
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
                    ]
                }
            }
        ]
        
        results = await self.aggregate(pipeline)
        facet_result = results[0]
        
        return {
            "charge_negative_adjustments": self.facet_to_datacount(facet_result, "charge_negative_adjustments"),
            "charge_adjustment_exceeds_amount": self.facet_to_datacount(facet_result, "charge_adjustment_exceeds_amount"),
            "charges_missing_adjustment_details": self.facet_to_datacount(facet_result, "charges_missing_adjustment_details"),
            "chargeadjustment_sum_mismatch": self.facet_to_datacount(facet_result, "chargeadjustment_sum_mismatch")
        }

    async def run_all(self):
        self.total_claims = await self.get_total_claims()
        claims_with_adjustments = await self.claims.count_documents(self.filter | {"claimAdjAmount": {"$gt": 0}})
        
        (claim_results, charge_results) = await asyncio.gather(
            self.run_claim_level_checks_combined(),
            self.run_charge_level_checks_combined()
        )
        
        issues = AdjustmentValidation(
            negative_adjustments=claim_results["negative_adjustments"],
            adjustment_greater_than_claim=claim_results["adjustment_greater_than_claim"],
            adjustment_exceeds_50_percent=claim_results["excessive_adjustments"],
            missing_adjustment_details=claim_results["missing_adjustment_details"],
            charge_negative_adjustments=charge_results["charge_negative_adjustments"],
            charge_adjustment_exceeds_amount=charge_results["charge_adjustment_exceeds_amount"],
            charges_missing_adjustment_details=charge_results["charges_missing_adjustment_details"],
            chargeadjustment_sum_mismatch=charge_results["chargeadjustment_sum_mismatch"],
            claim_adj_records_sum_mismatch=claim_results["claim_adj_sum_mismatch"]
        )
        
        return Adjustment(
            total_claims=self.total_claims,
            claims_with_adjustments=claims_with_adjustments,
            issues=issues
        )
 
async def adjustment_analysis(db, filters=None):
    analyzer = AdjustmentAnalyzer(db, filters)
    return await analyzer.run_all()
