from loguru import logger
from .base import BaseAnalyzer
from .models import DataCount, Claims_info, ClaimIssues
import asyncio


class ClaimsAnalyzer(BaseAnalyzer):
    def __init__(self, db):
        super().__init__(db)
 
    
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
                    ],
                    "pending_amount": [
                        {"$match": {"claimStatus": {"$in": ["Open", "Sent to Payor"]}}},
                        {"$group": {"_id": None, "total_amount": {"$sum": "$claimAmount"}}}
                    ],
                    "denied_amount": [
                        {"$match": {"claimStatus": "Denied"}},
                        {"$group": {"_id": None, "total_amount": {"$sum": "$claimAmount"}}}
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
        
        self.pending_count = self.open_count + self.sent_to_payer_count
        self.pending_amount = result["pending_amount"][0]["total_amount"] if result["pending_amount"] else 0
        self.denied_amount = result["denied_amount"][0]["total_amount"] if result["denied_amount"] else 0
        self.denial_rate = (self.denied_count / self.total_claims * 100) if self.total_claims > 0 else 0

    async def run_claim_level_checks_combined(self):
        pipeline = [
            {
                "$facet": {
                    "denied_with_payment": [
                        {"$match": {"claimStatus": "Denied", "claimAmountPaid": {"$gt": 0}}},
                        {"$count": "total"}
                    ],
                    "denied_without_remittances": [
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
                    "denied_with_overpayment": [
                        {
                            "$match": {
                                "claimStatus": "Denied",
                                "$expr": {"$gt": ["$claimAmountPaid", "$claimAmount"]}
                            }
                        },
                        {"$count": "total"}
                    ],
                    "open_with_payment": [
                        {"$match": {"claimStatus": "Open", "claimAmountPaid": {"$gt": 0}}},
                        {"$count": "total"}
                    ],
                    "paidamount_greater_than_claimamount": [
                        {"$match": {"$expr": {"$gt": ["$claimAmountPaid", "$claimAmount"]}}},
                        {"$count": "total"}
                    ],
                    "adjamount_greater_than_claimamount": [
                        {"$match": {"$expr": {"$gt": ["$claimAdjAmount", "$claimAmount"]}}},
                        {"$count": "total"}
                    ],
                    "paid_plus_adjustment_exceeds_claim": [
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
                    "closed_with_zero_amtpaid_and_adj": [
                        {
                            "$match": {
                                "claimStatus": "Closed",
                                "claimAmountPaid": 0,
                                "claimAdjAmount": 0
                            }
                        },
                        {"$count": "total"}
                    ],
                    "closed_with_remaining_balanceamt": [
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
                    "duplicate_claims": [
                        {"$group": {"_id": "$claimId", "count": {"$sum": 1}}},
                        {"$match": {"count": {"$gt": 1}}},
                        {"$group": {"_id": None, "total": {"$sum": "$count"}}}
                    ]
                }
            }
        ]
        
        results = await self.aggregate(pipeline)
        facet_result = results[0]
        
        return {
            "denied_with_payment": self.facet_to_datacount(facet_result, "denied_with_payment"),
            "denied_without_remittances": self.facet_to_datacount(facet_result, "denied_without_remittances"),
            "denied_with_overpayment": self.facet_to_datacount(facet_result, "denied_with_overpayment"),
            "open_with_payment": self.facet_to_datacount(facet_result, "open_with_payment"),
            "paidamount_greater_than_claimamount": self.facet_to_datacount(facet_result, "paidamount_greater_than_claimamount"),
            "adjamount_greater_than_claimamount": self.facet_to_datacount(facet_result, "adjamount_greater_than_claimamount"),
            "paid_plus_adjustment_exceeds_claim": self.facet_to_datacount(facet_result, "paid_plus_adjustment_exceeds_claim"),
            "closed_with_zero_amtpaid_and_adj": self.facet_to_datacount(facet_result, "closed_with_zero_amtpaid_and_adj"),
            "closed_with_remaining_balanceamt": self.facet_to_datacount(facet_result, "closed_with_remaining_balanceamt"),
            "duplicate_claims": self.facet_to_datacount(facet_result, "duplicate_claims")
        }

    async def run_charge_level_checks_combined(self):
        pipeline = [
            {"$unwind": "$charges"},
            {
                "$facet": {
                    "claim_sum_mismatch": [
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
                    "claim_amount_paid_sum_mismatch": [
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
                    "claim_adj_amount_sum_mismatch": [
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
                }
            }
        ]
        
        results = await self.aggregate(pipeline)
        facet_result = results[0]
        
        return {
            "claim_sum_mismatch": self.facet_to_datacount(facet_result, "claim_sum_mismatch"),
            "claim_amount_paid_sum_mismatch": self.facet_to_datacount(facet_result, "claim_amount_paid_sum_mismatch"),
            "claim_adj_amount_sum_mismatch": self.facet_to_datacount(facet_result, "claim_adj_amount_sum_mismatch")
        }
    
    async def run_all(self) -> Claims_info:
        await self.get_claim_status_counts()
        
        (claim_results, charge_results) = await asyncio.gather(
            self.run_claim_level_checks_combined(),
            self.run_charge_level_checks_combined()
        )
        
        issues = ClaimIssues(
            denied_with_payment=claim_results["denied_with_payment"],
            denied_without_remittances=claim_results["denied_without_remittances"],
            denied_with_overpayment=claim_results["denied_with_overpayment"],
            open_with_payment=claim_results["open_with_payment"],
            paidamount_greater_than_claimamount=claim_results["paidamount_greater_than_claimamount"],
            adjamount_greater_than_claimamount=claim_results["adjamount_greater_than_claimamount"],
            claim_sum_mismatch=charge_results["claim_sum_mismatch"],
            duplicate_claims=claim_results["duplicate_claims"],
            paid_plus_adjustment_exceeds_claim=claim_results["paid_plus_adjustment_exceeds_claim"],
            closed_with_zero_amtpaid_and_adj=claim_results["closed_with_zero_amtpaid_and_adj"],
            claim_amount_paid_sum_mismatch=charge_results["claim_amount_paid_sum_mismatch"],
            claim_adj_amount_sum_mismatch=charge_results["claim_adj_amount_sum_mismatch"],
            closed_with_remaining_balanceamt=claim_results["closed_with_remaining_balanceamt"]
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

async def claims_analysis(db) -> Claims_info:
    analyzer = ClaimsAnalyzer(db)
    return await analyzer.run_all()
