 
from loguru import logger
from .models import DataCount, Claims_info, ClaimIssues
 
class ClaimsAnalyzer:  
    def __init__(self, db):
        self.db = db
        self.claims = db["claims"]
        self.total_claims = 0
        self.open_count = 0
        self.sent_to_payer_count = 0
        self.closed_count = 0
        self.denied_count = 0
        self.pending_count = 0
        self.pending_amount = 0
        self.denial_rate = 0
        self.denied_amount = 0
   
  # To get the count of the different claim status's
   
    async def get_claim_status_counts(self):
        logger.info("\nCounting claim statuses")
        self.total_claims = await self.claims.count_documents({})
        self.open_count = await self.claims.count_documents({"claimStatus": "Open"})
        self.sent_to_payer_count = await self.claims.count_documents({"claimStatus": "Sent to Payor"})
        self.closed_count = await self.claims.count_documents({"claimStatus": "Closed"})
        self.denied_count = await self.claims.count_documents({"claimStatus": "Denied"})
        logger.info(f"Total Claims: {self.total_claims:,}")
        logger.info(f"Open: {self.open_count:,}")
        logger.info(f"Sent to Payer: {self.sent_to_payer_count:,}")
        logger.info(f"Closed: {self.closed_count:,}")
        logger.info(f"Denied: {self.denied_count:,}")
   
    # To get the pending payment information
   
    async def get_pending_payment_info(self):
        logger.info("Pending Payment (Open + Sent to Payer)")
       
        self.pending_count = self.open_count + self.sent_to_payer_count
       
        pipeline = [
            {"$match": {"claimStatus": {"$in": ["Open", "Sent to Payor"]}}},
            {"$group": {"_id": None, "total_amount": {"$sum": "$claimAmount"}}}
        ]    
        result = await self.claims.aggregate(pipeline).to_list(1)
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
       
        result = await self.claims.aggregate(pipeline).to_list(1)
        self.denied_amount = result[0]["total_amount"] if result else 0
       
        logger.info(f"Denied Count: {self.denied_count:,} claims")
        logger.info(f"Denial Rate: {self.denial_rate:.2f}%")
        logger.info(f"Denied Amount: ${self.denied_amount:,.2f}")
   
    #  To find the denied claims with payment and also incorrectly paid amount
   
    async def get_denied_claims_with_payment(self) -> DataCount:
        if self.denied_count == 0:
            logger.info("No denied claims")
            return DataCount(count=0, percentage=0.0)
        logger.info("Checking for denied claims with Payment")
        denied_with_payment = await self.claims.find({
            "claimStatus": "Denied",
            "claimAmountPaid": {"$gt": 0}
        }).to_list(length=None)
       
        count = len(denied_with_payment)
        percentage = (count / self.total_claims * 100) if self.total_claims > 0 else 0.0
       
        if count > 0:
            total_incorrect = sum(claim.get("claimAmountPaid", 0) for claim in denied_with_payment)
            logger.error(f"Found: {count} claims")
            logger.error(f"Total Incorrectly Paid: ${total_incorrect:,.2f}")
        else:
            logger.info("No denied claims with payment found")
       
        return DataCount(count=count, percentage=round(percentage, 2))
   
    # To find denied claims without remittances
   
    async def get_denied_claims_without_remittances(self) -> DataCount:
        if self.denied_count == 0:
            return DataCount(count=0, percentage=0.0)
        logger.info("Denied claims without remittance ")
       
        denied_without_remittances = await self.claims.find({
            "claimStatus": "Denied",
            "$or": [
                {"chargeRemittances": {"$exists": False}},
                {"chargeRemittances": []},
                {"chargeRemittances": None}
            ]
        }).to_list(length=None)
       
        count = len(denied_without_remittances)
        percentage = (count / self.total_claims * 100) if self.total_claims > 0 else 0.0
        logger.warning(f"Found: {count} claims")
        return DataCount(count=count, percentage=round(percentage, 2))
   
    # To find denied claims with overpayment
   
    async def get_denied_claims_with_overpayment(self) -> DataCount:
        if self.denied_count == 0:
            return DataCount(count=0, percentage=0.0)
       
        logger.info("Denied claims with overpayment")
       
        denied_with_overpayment = await self.claims.find({
            "claimStatus": "Denied",
            "$expr": {"$gt": ["$claimAmountPaid", "$claimAmount"]}
        }).to_list(length=None)
       
        count = len(denied_with_overpayment)
        percentage = (count / self.total_claims * 100) if self.total_claims > 0 else 0.0
       
        if count > 0:
            total_overpayment = sum(
                claim.get("claimAmountPaid", 0) - claim.get("claimAmount", 0)
                for claim in denied_with_overpayment
            )
            logger.info(f"Found {count} denied claims with overpayment")
            logger.info(f"Claims affected: {count:,}")
            logger.info(f"Total overpaid: ${total_overpayment:,.2f}")
        else:
            logger.info("No denied claims with overpayment found")
       
        return DataCount(count=count, percentage=round(percentage, 2))
   
    # To find open claims with payment
   
    async def get_open_claims_with_payment(self) -> DataCount:
        if self.open_count == 0:
            logger.info("no open claims to check further")
            return DataCount(count=0, percentage=0.0)
       
        logger.info("Checking for open claims with Payment")
       
        open_with_payment = await self.claims.find({
            "claimStatus": "Open",
            "claimAmountPaid": {"$gt": 0}
        }).to_list(length=None)
       
        count = len(open_with_payment)
        percentage = (count / self.total_claims * 100) if self.total_claims > 0 else 0.0
        if count > 0:
            total_incorrect = sum(claim.get("claimAmountPaid", 0) for claim in open_with_payment)
            logger.error(f"Total Incorrectly Paid in Open Claims: ${total_incorrect:,.2f}")
        else:
            logger.info("No open claims with payment found")
       
        return DataCount(count=count, percentage=round(percentage, 2))
   
    # To find paid amount exceeds claim amount
 
    async def get_paid_amount_exceeds_claim(self) -> DataCount:
       
        logger.info("Checking for paid amount > claim amount...")
       
        results = await self.claims.find({
            "$expr": {"$gt": ["$claimAmountPaid", "$claimAmount"]}
        }).to_list(length=None)
       
        count = len(results)
        percentage = (count / self.total_claims * 100) if self.total_claims > 0 else 0.0        
        return DataCount(count=count, percentage=round(percentage, 2))
   
    # To find adjustment amount exceeds claim amount
   
    async def get_adjustment_exceeds_claim(self) -> DataCount:
        logger.info("\nChecking for adjustment amount > claim amount...")
       
        results = await self.claims.find({
            "$expr": {"$gt": ["$claimAdjAmount", "$claimAmount"]}
        }).to_list(length=None)
        count = len(results)
        percentage = (count / self.total_claims * 100) if self.total_claims > 0 else 0.0
        return DataCount(count=count, percentage=round(percentage, 2))
   
    # To find claimAmountPaid + claimAdjAmount exceeds claim amount
   
    async def get_paid_plus_adjustment_exceeds_claim(self) -> DataCount:
        logger.info("Checking for (paid + adjustment) > claim amount")
        results = await self.claims.find({
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
        }).to_list(length=None)
       
        count = len(results)
        percentage = (count / self.total_claims * 100) if self.total_claims > 0 else 0.0
        return DataCount(count=count, percentage=round(percentage, 2))
   
     # To find claim amount not equal to sum of charges
   
    async def get_claim_amount_sum_mismatch(self) -> DataCount:
       
        logger.info("Checking for claim amount ≠ sum of charges")
       
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
            {"$count": "mismatch_count"}
        ]
       
        result = await self.claims.aggregate(pipeline).to_list(1)
        count = result[0]["mismatch_count"] if result else 0
        percentage = (count / self.total_claims * 100) if self.total_claims > 0 else 0.0
        return DataCount(count=count, percentage=round(percentage, 2))
 
    #  To find duplicate claim IDs
   
    async def get_duplicate_claim_ids(self) -> DataCount:
        logger.info("Checking for duplicate claims")
       
        pipeline = [
            {"$group": {"_id": "$claimId", "count": {"$sum": 1}}},
            {"$match": {"count": {"$gt": 1}}},
            {"$group": {"_id": None, "total_duplicate_claims": {"$sum": "$count"}}}
        ]
       
        result = await self.claims.aggregate(pipeline).to_list(1)
        count = result[0]["total_duplicate_claims"] if result else 0
        percentage = (count / self.total_claims * 100) if self.total_claims > 0 else 0.0        
        return DataCount(count=count, percentage=round(percentage, 2))
   
 
    async def run_all(self) -> Claims_info:
        logger.info("Claim Status Analysis")
     
        await self.get_claim_status_counts()
        await self.get_pending_payment_info()
        await self.get_denial_info()
        logger.info("Running validation checks")
       
        issues = ClaimIssues(
            denied_with_payment=await self.get_denied_claims_with_payment(),
            denied_without_remittances=await self.get_denied_claims_without_remittances(),
            denied_with_overpayment=await self.get_denied_claims_with_overpayment(),
            open_with_payment=await self.get_open_claims_with_payment(),
            paidamount_greater_than_claimamount=await self.get_paid_amount_exceeds_claim(),
            adjamount_greater_than_claimamount=await self.get_adjustment_exceeds_claim(),
            claim_sum_mismatch=await self.get_claim_amount_sum_mismatch(),
            duplicate_claims=await self.get_duplicate_claim_ids(),
            paid_plus_adjustment_exceeds_claim=await self.get_paid_plus_adjustment_exceeds_claim()
        )
 
        logger.info("Claims analysis complete")
       
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