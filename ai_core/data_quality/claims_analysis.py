from loguru import logger
from .base import BaseAnalyzer
from .models import DataCount, Claims_info, ClaimIssues
import asyncio


class ClaimsAnalyzer(BaseAnalyzer):
    def __init__(self, db,filters=None):
        super().__init__(db,filters)
    
    # To get the count of the different claim status's 
    
    async def get_claim_status_counts(self):
        pipeline = [
        {
            "$facet": {
                "total": [
                    {"$count": "count"}
                ],
                "open": [
                    {"$match": {"claimStatus": "Open"}},
                    {"$count": "count"}
                ],
                "sent_to_payer": [
                    {"$match": {"claimStatus": "Sent to Payor"}},
                    {"$count": "count"}
                ],
                "closed": [
                    {"$match": {"claimStatus": "Closed"}},
                    {"$count": "count"}
                ],
                "denied": [
                    {"$match": {"claimStatus": "Denied"}},
                    {"$count": "count"}
                ]
            }
        }
    ] 
        results = await self.aggregate(pipeline)
        result = results[0]
        self.total_claims = result["total"][0]["count"] if result["total"] else 0
        self.open_count = result["open"][0]["count"] if result["open"] else 0
        self.sent_to_payer_count = result["sent_to_payer"][0]["count"] if result["sent_to_payer"] else 0
        self.closed_count = result["closed"][0]["count"] if result["closed"] else 0
        self.denied_count = result["denied"][0]["count"] if result["denied"] else 0
        
        logger.info(f"Total Claims: {self.total_claims:,} claims")
        logger.info(f"Open Claims: {self.open_count:,} claims")
        logger.info(f"Sent to Payer Claims: {self.sent_to_payer_count:,} claims")
        logger.info(f"Closed Claims: {self.closed_count:,} claims")
        logger.info(f"Denied Claims: {self.denied_count:,} claims")
    
        
    # To get the pending payment information
    
    async def get_pending_payment_info(self):
        logger.info("Pending Payment (Open + Sent to Payor)")
        
        self.pending_count = self.open_count + self.sent_to_payer_count
        
        pipeline = [
            {"$match": {"claimStatus": {"$in": ["Open", "Sent to Payor"]}}},
            {"$group": {"_id": None, "total_amount": {"$sum": "$claimAmount"}}}
        ]  
        result = await self.aggregate(pipeline)
        self.pending_amount = result[0]["total_amount"] if result else 0
        
        logger.info(f"Pending Count: {self.pending_count:,} claims")
        logger.info(f"Pending Amount: ${self.pending_amount:,.2f}")
    
    #To get the denial information 
    
    async def get_denial_info(self):
        logger.info("Denial Rate")
        
        self.denial_rate = (self.denied_count / self.total_claims * 100) if self.total_claims > 0 else 0
        
        pipeline = [
            {"$match": {"claimStatus": "Denied"}},
            {"$group": {"_id": None, "total_amount": {"$sum": "$claimAmount"}}}
        ]
        
        result = await self.aggregate(pipeline)
        self.denied_amount = result[0]["total_amount"] if result else 0
        logger.info(f"Denied Count: {self.denied_count:,} claims")
        logger.info(f"Denial Rate: {self.denial_rate:.2f}%")
        logger.info(f"Denied Amount: ${self.denied_amount:,.2f}")
    
    #  To find the denied claims with payment and also incorrectly paid amount 
    
    async def get_denied_claims_with_payment(self) -> DataCount:
        logger.info("Denied claims with payment")
        
        pipeline = [
            {"$match": {"claimStatus": "Denied", "claimAmountPaid": {"$gt": 0}}},
            {"$count": "total"}
        ]
        
        result = await self.run_pipeline(pipeline)    
        return result
    
    # To find denied claims without remittances 
    
    async def get_denied_claims_without_remittances(self) -> DataCount:
        logger.info("Denied claims without remittance")
        
        pipeline = [
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
        ]
        
        result = await self.run_pipeline(pipeline)    
        return result
    
    # To find denied claims with overpayment
    
    async def get_denied_claims_with_overpayment(self) -> DataCount:
        logger.info("Denied claims with overpayment")
        
        pipeline = [
            {
                "$match": {
                    "claimStatus": "Denied",
                    "$expr": {"$gt": ["$claimAmountPaid", "$claimAmount"]}
                }
            },
            {"$count": "total"}
        ]
        
        result = await self.run_pipeline(pipeline)   
        return result
    
    # To find open claims with payment
    
    async def get_open_claims_with_payment(self) -> DataCount:
        logger.info("Open claims with payment")
        
        pipeline = [
            {"$match": {"claimStatus": "Open", "claimAmountPaid": {"$gt": 0}}},
            {"$count": "total"}
        ]
        
        result = await self.run_pipeline(pipeline)        
        return result
    
    # To find paid amount exceeds claim amount
    
    async def get_paid_amount_exceeds_claim(self) -> DataCount:
        logger.info("Paid amount exceeds claim amount")
        
        pipeline = [
            {"$match": {"$expr": {"$gt": ["$claimAmountPaid", "$claimAmount"]}}},
            {"$count": "total"}
        ]
        
        result = await self.run_pipeline(pipeline)    
        return result
    
    # To find adjustment amount exceeds claim amount
    
    async def get_adjustment_exceeds_claim(self) -> DataCount:
        logger.info("Adjustment amount exceeds claim amount")
        
        pipeline = [
            {"$match": {"$expr": {"$gt": ["$claimAdjAmount", "$claimAmount"]}}},
            {"$count": "total"}
        ]
        
        result = await self.run_pipeline(pipeline)
        return result
    
    # To find claimamountpaid + claimAdjAmount exceeds claim amount
    
    async def get_paid_plus_adjustment_exceeds_claim(self) -> DataCount:
        logger.info("Paid amount plus adjustment exceeds claim amount")
        
        pipeline = [
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
        ]
        
        result = await self.run_pipeline(pipeline) 
        return result
    
     # To find claim amount not equal to sum of charges
    
    async def get_claim_amount_sum_mismatch(self) -> DataCount:
        logger.info("Claim amount does not equal sum of charges")
        
        pipeline = [
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
        ]
        
        result = await self.run_pipeline(pipeline)
        return result
    
# To check whether the claimAmt_paid is not equal to sum of charges amountpaid

    async def get_claim_amount_paid_sum_mismatch(self) -> DataCount:        
        pipeline = [
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
        ]
        
        result = await self.run_pipeline(pipeline)
        return result
    
    #  To check whether  claimAdjAmount is not equal to  sum of charges adjustment amount 
    
    async def get_claim_adj_amount_sum_mismatch(self) -> DataCount:  
        logger.info("Claim adjustment amount does not equal sum of charges adjustment amount")
        
        pipeline = [
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
        ]
        
        result = await self.run_pipeline(pipeline)
        return result
    
    
    async def get_closed_with_zero_amtpaid_and_adj(self) -> DataCount:
        pipeline = [
           {
            "$match": {
                "claimStatus": "Closed",
                "claimAmountPaid": 0,
                "claimAdjAmount": 0
            }
        },
        {"$count": "total"}]
        
        result= await self.run_pipeline(pipeline)
        return result
    
    # To find closed claims with remaining balance amount (claimAmount - (claimAmountPaid + claimAdjAmount) > 0)
    
    async def get_closed_with_remaining_balanceamt(self) -> DataCount:
        pipeline = [
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
    ]
    
        result = await self.run_pipeline(pipeline)
        return result   
           
    #  To find duplicate claim IDs
    
    async def get_duplicate_claim_ids(self) -> DataCount:
        logger.info("Duplicate claim IDs")
        
        pipeline = [
            {"$group": {"_id": "$claimId", "count": {"$sum": 1}}},
            {"$match": {"count": {"$gt": 1}}},
            {"$group": {"_id": None, "total": {"$sum": "$count"}}}
        ]
        
        result = await self.run_pipeline(pipeline)
        return result
    
    async def run_all(self) -> Claims_info:
        await self.get_claim_status_counts()
        await self.get_pending_payment_info()
        await self.get_denial_info()
        (
        denied_with_payment,
        denied_without_remittances,
        denied_with_overpayment,
        open_with_payment,
        paidamount_greater_than_claimamount,
        adjamount_greater_than_claimamount,
        claim_sum_mismatch,
        duplicate_claims,
        paid_plus_adjustment_exceeds_claim,
        closed_with_zero_amtpaid_and_adj,
        claim_amount_paid_sum_mismatch,
        claim_adj_amount_sum_mismatch,
        closed_with_remaining_balanceamt
        ) = await asyncio.gather(
        self.get_denied_claims_with_payment(),
        self.get_denied_claims_without_remittances(),
        self.get_denied_claims_with_overpayment(),
        self.get_open_claims_with_payment(),
        self.get_paid_amount_exceeds_claim(),
        self.get_adjustment_exceeds_claim(),
        self.get_claim_amount_sum_mismatch(),
        self.get_duplicate_claim_ids(),
        self.get_paid_plus_adjustment_exceeds_claim(),
        self.get_closed_with_zero_amtpaid_and_adj(),
        self.get_claim_amount_paid_sum_mismatch(),
        self.get_claim_adj_amount_sum_mismatch(),
        self.get_closed_with_remaining_balanceamt()
        )
        issues = ClaimIssues(
        denied_with_payment=denied_with_payment,
        denied_without_remittances=denied_without_remittances,
        denied_with_overpayment=denied_with_overpayment,
        open_with_payment=open_with_payment,
        paidamount_greater_than_claimamount=paidamount_greater_than_claimamount,
        adjamount_greater_than_claimamount=adjamount_greater_than_claimamount,
        claim_sum_mismatch=claim_sum_mismatch,
        duplicate_claims=duplicate_claims,
        paid_plus_adjustment_exceeds_claim=paid_plus_adjustment_exceeds_claim,
        closed_with_zero_amtpaid_and_adj=closed_with_zero_amtpaid_and_adj,
        claim_amount_paid_sum_mismatch=claim_amount_paid_sum_mismatch,
        claim_adj_amount_sum_mismatch=claim_adj_amount_sum_mismatch,
        closed_with_remaining_balanceamt=closed_with_remaining_balanceamt
    )

        return Claims_info(
            total_claims=self.total_claims,
            open_count=self.open_count,
            sent_to_payer_count=self.sent_to_payer_count,
            closed_count=self.closed_count,
            denied_count=self.denied_count,
            pending_count=self.pending_count,
            pending_amount=self.pending_amount,
            denial_rate=self.denial_rate,
            denied_amount=self.denied_amount,
            issues=issues
        )

async def claims_analysis(db, filters=None) -> Claims_info:
    analyzer = ClaimsAnalyzer(db, filters)
    return await analyzer.run_all()
