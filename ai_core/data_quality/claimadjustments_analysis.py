from loguru import logger
from .models import DataCount, Adjustment
 
async def adjustment_analysis(db):
    claims = db["claims"]
    total_claims = await claims.count_documents({})
   
    # Checking Claims with Adjustments
   
    claims_with_adjustments = await claims.count_documents({"claimAdjAmount": {"$gt": 0}})
    logger.info(f"\nTotal Claims: {total_claims:,}")
    logger.info(f"Claims With Adjustments: {claims_with_adjustments:,}\n")
   
    #  Checking for Negative Claim Adjustment amount
    logger.info("Checking for negative adjustments")
    negative_adj_count = await claims.count_documents({"claimAdjAmount": {"$lt": 0}})
    negative_adj_pct = (negative_adj_count / total_claims * 100) if total_claims > 0 else 0.0
 
    #  ClaimAdjAmount greater than ClaimAmount
   
    adjamount_greater_than_claimamount=  await claims.find({
        "$expr": {"$gt": ["$claimAdjAmount", "$claimAmount"]}
    }).to_list(length=None)
   
    adjamount_greater_than_claimamount_count = len(adjamount_greater_than_claimamount)
    adjamount_greater_than_claimamount_pct = (adjamount_greater_than_claimamount_count / total_claims * 100) if total_claims > 0 else 0.0
   
   
    #  Excessive Adjustments (> 50% of claim)
   
    logger.info("Checking for excessive adjustments(>50%)")
    excessive_adj_pipeline = [
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
   
    excessive_adj_result = await claims.aggregate(excessive_adj_pipeline).to_list(1)
    excessive_adj_count = excessive_adj_result[0]["total"] if excessive_adj_result else 0
    excessive_adj_pct = (excessive_adj_count / total_claims * 100) if total_claims > 0 else 0.0
 
   
    # Missing Adjustment Details
   
    logger.info("Checking for missing adjustment details")
    missing_details_pipeline = [
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
   
    missing_details_result = await claims.aggregate(missing_details_pipeline).to_list(1)
    missing_details_count = missing_details_result[0]["total"] if missing_details_result else 0
    missing_details_pct = (missing_details_count / total_claims * 100) if total_claims > 0 else 0.0
 
    # Checking for whether the adjustment amount under the charges is negative
   
    logger.info("Checking for charge negative adjustments")
    charge_neg_pipeline = [
        {"$unwind": "$charges"},
        {"$match": {"charges.adjustmentAmount": {"$lt": 0}}},
        {"$group": {"_id": "$_id"}},
        {"$count": "total"}
    ]
   
    charge_neg_result = await claims.aggregate(charge_neg_pipeline).to_list(1)
    charge_neg_count = charge_neg_result[0]["total"] if charge_neg_result else 0
    charge_neg_pct = (charge_neg_count / total_claims * 100) if total_claims > 0 else 0.0
   
   
    # Checks whether charge adjustment amount is greater than charge amount
   
    logger.info("Checking for charge adjustment > charge amount")
    charge_exceeds_pipeline = [
        {"$unwind": "$charges"},
        {
            "$match": {
                "$expr": {"$gt": ["$charges.adjustmentAmount", "$charges.amount"]}
            }
        },
        {"$group": {"_id": "$_id"}},
        {"$count": "total"}
    ]
   
    charge_exceeds_result = await claims.aggregate(charge_exceeds_pipeline).to_list(1)
    charge_exceeds_count = charge_exceeds_result[0]["total"] if charge_exceeds_result else 0
    charge_exceeds_pct = (charge_exceeds_count / total_claims * 100) if total_claims > 0 else 0.0
   
       
    # Charges Missing Adjustment Details
   
    logger.info("Checking for charges missing adjustment details")
    charge_missing_pipeline = [
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
   
    charge_missing_result = await claims.aggregate(charge_missing_pipeline).to_list(1)
    charge_missing_count = charge_missing_result[0]["total"] if charge_missing_result else 0
    charge_missing_pct = (charge_missing_count / total_claims * 100) if total_claims > 0 else 0.0
   
 
   
    return Adjustment(
        total_claims=total_claims,
        claims_with_adjustments=claims_with_adjustments,
        # Claim-level issues
        negative_adjustments=DataCount(
            count=negative_adj_count,
            percentage=round(negative_adj_pct, 2)
        ),
        adjustment_greater_than_claim=DataCount(
            count=adjamount_greater_than_claimamount_count,  
            percentage=round(adjamount_greater_than_claimamount_pct, 2)  
        ),
        adjustment_exceeds_50_percent=DataCount(
            count=excessive_adj_count,
            percentage=round(excessive_adj_pct, 2)
        ),
 
        missing_adjustment_details=DataCount(
            count=missing_details_count,
            percentage=round(missing_details_pct, 2)
        ),
       
        # Charge-level issues
        charge_negative_adjustments=DataCount(
            count=charge_neg_count,
            percentage=round(charge_neg_pct, 2)
        ),
        charge_adjustment_exceeds_amount=DataCount(
            count=charge_exceeds_count,
            percentage=round(charge_exceeds_pct, 2)
        ),
        charges_missing_adjustment_details=DataCount(
            count=charge_missing_count,
            percentage=round(charge_missing_pct, 2)
        )
    )