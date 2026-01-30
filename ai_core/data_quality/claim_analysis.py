 
from loguru import logger
from .models import DataCount,Claims_info,ClaimIssues
 
async def claims_analysis(db):
 
    logger.info("Claim Status Analysis")
 
    claims = db["claims"]
    total_claims = await claims.count_documents({})
   
    logger.info("Counts of different claim statuses")
   
    open_count = await claims.count_documents({"claimStatus": "Open"})
    sent_to_payer_count = await claims.count_documents({"claimStatus": "Sent to Payor"})
    closed_count = await claims.count_documents({"claimStatus": "Closed"})
    denied_count = await claims.count_documents({"claimStatus": "Denied"})
   
    logger.info(f"\nTotal Claims:{total_claims:8,}")
    logger.info(f"\nOpen:{open_count:8,}")
    logger.info(f"Sent to Payer:{sent_to_payer_count:8,}")
    logger.info(f"Closed:{closed_count:8,}")
    logger.info(f"Denied:{denied_count:8,}")
   
    # Pending Payment
   
    logger.info("Pending Payment (Open + Sent to Payer)")
   
    pending_count = open_count + sent_to_payer_count
    pending_pipeline = [
        {
            "$match": {
                "claimStatus": {"$in": ["Open", "Sent to Payor"]}
            }
        },
        {
            "$group": {
                "_id": None,
                "total_amount": {"$sum": "$claimAmount"}
            }
        }
    ]
   
    pending_result = await claims.aggregate(pending_pipeline).to_list(1)
    pending_amount = pending_result[0]["total_amount"] if pending_result else 0
   
    logger.info(f"\nPending Count: {pending_count:8,} claims")
    logger.info(f"Pending Amount:${pending_amount:,.2f}")
   
    # DENIAL RATE
   
    logger.info("Denial Rate")
   
    denial_rate = (denied_count / total_claims * 100) if total_claims > 0 else 0
    denied_pipeline = [
        {
            "$match": {
                "claimStatus": "Denied"
            }
        },
        {
            "$group": {
                "_id": None,
                "total_amount": {"$sum": "$claimAmount"}
            }
        }
    ]
   
    denied_result = await claims.aggregate(denied_pipeline).to_list(1)
    denied_amount = denied_result[0]["total_amount"] if denied_result else 0
   
    logger.info(f"\nDenied Count:{denied_count:8,} claims")
    logger.info(f"Denial Rate:{denial_rate:8.2f}%")
    logger.info(f"Denied Amount:${denied_amount:,.2f}")
     
      # Checking the denied claims with Payment
   
    if denied_count==0:
        logger.info("No denied claims")
        denied_with_payment_count = 0
        denied_with_payment_percentage = 0
        denied_without_remittances_count = 0
        denied_without_remittances_percentage = 0
        denied_with_overpayment_count = 0
        denied_with_overpayment_percentage = 0
   
    else:
        logger.info("Checking for denied claims with Payment ")
        denied_with_payment = await claims.find({
            "claimStatus": "Denied",
            "claimAmountPaid": {"$gt": 0}
        }).to_list(length=None)
       
        denied_with_payment_count = len(denied_with_payment)
        denied_with_payment_percentage = (denied_with_payment_count / total_claims * 100) if total_claims > 0 else 0.0
       
        # Calculation of  total incorrectly paid amount
       
        if denied_with_payment_count > 0:
            total_incorrect_payment = sum(
                claim.get("claimAmountPaid", 0) for claim in denied_with_payment
            )
            logger.error(f"Found: {denied_with_payment_count} claims")
            logger.error(f"Total Incorrectly Paid: ${total_incorrect_payment:,.2f}\n")
           
        else:
            logger.info("No denied claims with payment found.\n")
           
        # Checking for the denied claims without remittance
           
        logger.info("Denied claims without remittance that is no denial reason")
       
        denied_without_remittances = await claims.find({
            "claimStatus": "Denied",
            "$or": [
                {"chargeRemittances": {"$exists": False}},
                {"chargeRemittances": []},
                {"chargeRemittances": None}
            ]
        }).to_list(length=None)
       
        denied_without_remittances_count = len(denied_without_remittances)
        denied_without_remittances_percentage= (denied_without_remittances_count / total_claims * 100) if total_claims > 0 else 0.0  
   
        logger.warning(f": {denied_without_remittances_count} claims")
       
        # check for Denied claims with Overpayment  
       
        logger.info("Denied claims with Overpayment")
        denied_with_overpayment = await claims.find({
            "claimStatus": "Denied",
            "$expr": {"$gt": ["$claimAmountPaid", "$claimAmount"]}
        }).to_list(length=None)
       
        denied_with_overpayment_count = len(denied_with_overpayment)
        denied_with_overpayment_percentage = (denied_with_overpayment_count / total_claims * 100) if total_claims > 0 else 0.0
       
        if denied_with_overpayment_count > 0:
           
            total_overpayment = sum(
                claim.get("claimAmountPaid", 0) - claim.get("claimAmount", 0)
                for claim in denied_with_overpayment
            )
            logger.info(f": found {denied_with_overpayment_count} denied claims with overpayment")
            logger.info(f"Claims affected:{denied_with_overpayment_count:,}")
            logger.info(f"Total overpaid:${total_overpayment:,.2f}")
        else:
            logger.info("No denied claims with overpayment found")
   
   
    # Checking the Open claims with Payment  
   
    if open_count==0:
        logger.info("No open claims to check further")
        open_with_payment_count = 0
        open_with_payment_percentage = 0
    else:
        logger.info("Checking for open claims with Payment")
        open_with_payment = await claims.find({
            "claimStatus": "Open",
            "claimAmountPaid": {"$gt": 0}
        }).to_list(length=None)
       
        open_with_payment_count = len(open_with_payment)
        open_with_payment_percentage = (open_with_payment_count / total_claims * 100) if total_claims > 0 else 0.0
       
        if open_with_payment_count > 0:
            total_incorrect_open_payment = sum(
                claim.get("claimAmountPaid", 0) for claim in open_with_payment
            )
            logger.error(f"Found: {open_with_payment_count} open claims with payment")
            logger.error(f"Total Incorrectly Paid in Open Claims: ${total_incorrect_open_payment:,.2f}\n")
        else:
            logger.info("No open claims with payment found.\n")
           
   
    # Checking for ClaimAmount paid greater than ClaimAmount
   
    paidamount_greater_than_claimamount= await claims.find({
        "$expr": {"$gt": ["$claimAmountPaid", "$claimAmount"]}
    }).to_list(length=None)
    paidamount_greater_than_claimamount_count = len(paidamount_greater_than_claimamount)
    paidamount_greater_than_claimamount_pct = (paidamount_greater_than_claimamount_count / total_claims * 100) if total_claims > 0 else 0.0
   
    #  ClaimAdjAmount greater than ClaimAmount
   
    adjamount_greater_than_claimamount=  await claims.find({
        "$expr": {"$gt": ["$claimAdjAmount", "$claimAmount"]}
    }).to_list(length=None)
   
    adjamount_greater_than_claimamount_count = len(adjamount_greater_than_claimamount)
    adjamount_greater_than_claimamount_pct = (adjamount_greater_than_claimamount_count / total_claims * 100) if total_claims > 0 else 0.0
   
    # claimamount not equal to sum of charges
   
    claimamount_sum_mismatch=[
       
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
   
    claim_sum_mismatch_result = await claims.aggregate(claimamount_sum_mismatch).to_list(1)
    claim_sum_mismatch_count = claim_sum_mismatch_result[0]["mismatch_count"] if claim_sum_mismatch_result else 0
    claim_sum_mismatch_pct = (claim_sum_mismatch_count / total_claims * 100) if total_claims > 0 else 0.0
   
    # check for duplicate claims
   
    duplicate_claims_pipeline = [
        {
            "$group": {
                "_id": "$claimId",
                "count": {"$sum": 1}
            }
        },
        {
            "$match": {
                "count": {"$gt": 1}
            }
        },
        {
            "$group": {
                "_id": None,
                "total_duplicate_claims": {"$sum": "$count"}
            }
        }
    ]
   
    duplicate_claims_result = await claims.aggregate(duplicate_claims_pipeline).to_list(1)
    duplicate_claims_count = duplicate_claims_result[0]["total_duplicate_claims"] if duplicate_claims_result else 0
    duplicate_claims_pct = (duplicate_claims_count / total_claims * 100) if total_claims > 0 else 0.0
   
   
    # Check for claimAmountPaid + claimAdjAmount > claimAmount
   
    paid_plus_adjustment_exceeds_claim = await claims.find({
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
   
    paid_plus_adjustment_exceeds_claim_count = len(paid_plus_adjustment_exceeds_claim)
    paid_plus_adjustment_exceeds_claim_pct = (paid_plus_adjustment_exceeds_claim_count / total_claims * 100) if total_claims > 0 else 0.0
   
   
    issues = ClaimIssues(
    denied_with_payment=DataCount(count=denied_with_payment_count,percentage=round(denied_with_payment_percentage,2)),
    denied_without_remittances=DataCount(
        count=denied_without_remittances_count,
        percentage=round(denied_without_remittances_percentage, 2)
    ),
    denied_with_overpayment=DataCount(count=denied_with_overpayment_count,
        percentage=round(denied_with_overpayment_percentage, 2)
    ),
    open_with_payment=DataCount(
        count=open_with_payment_count,
        percentage=round(open_with_payment_percentage, 2)
    ),
   
    paidamount_greater_than_claimamount=DataCount(count=paidamount_greater_than_claimamount_count, percentage=round(paidamount_greater_than_claimamount_pct, 2)),
    adjamount_greater_than_claimamount=DataCount(count=adjamount_greater_than_claimamount_count, percentage=round(adjamount_greater_than_claimamount_pct, 2)),
    claim_sum_mismatch=DataCount(count=claim_sum_mismatch_count, percentage=round(claim_sum_mismatch_pct, 2)),
    duplicate_claims=DataCount(count=duplicate_claims_count, percentage=round(duplicate_claims_pct, 2)) ,
    paid_plus_adjustment_exceeds_claim=DataCount(count=paid_plus_adjustment_exceeds_claim_count, percentage=round(paid_plus_adjustment_exceeds_claim_pct, 2))
    )    
   
    return Claims_info(
        total_claims=total_claims,
        open_count=open_count,
        sent_to_payer_count=sent_to_payer_count,
        closed_count=closed_count,
        denied_count=denied_count,
        pending_count=pending_count,
        pending_amount=pending_amount,
        denial_rate=denial_rate,
        denied_amount=denied_amount,
        issues=issues
    )
 