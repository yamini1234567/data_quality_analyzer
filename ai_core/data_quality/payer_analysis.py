from loguru import logger
 
async def payer_analysis(db):
 
    logger.info("Payer Analysis")
    claims = db["claims"]
    total_claims= await claims.count_documents({})
    logger.info(f"Total no of claims: {total_claims}")
    unique_payers=await claims.distinct("payerMCO")
    unique_payers_count = len(unique_payers)
    logger.info(f"No of Unique payers: {unique_payers_count}")
   
    # Payer distribution table
   
    logger.info("Payer Distribution Table")
   
    payer_pipeline=[
 
      {
        "$group": {
            "_id": "$payerMCO",
            "total_claims": {"$sum": 1},
           
            # Total closed claims
           
            "total_closed": {
                "$sum": {
                    "$cond": [
                        {"$in": ["$claimStatus", ["Closed"]]},
                        1,
                        0
                    ]
                }
            },
           
            # Total denied claims
           
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
           
            # Total denied amount
           
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
   
    payer_table=await claims.aggregate(payer_pipeline).to_list(length=None)
       
    logger.info("\n")
    logger.info("-" * 140)
    logger.info(
        f"{'Payer':35s} "
        f"{'Total claims':>18s} "
        f"{'Totalclosed claims':>18s} "
        f"{'TotalDenied claims':>18s} "
        f"{'Avg Claim Amount':>12s} "
        f"{'Avg Paid Amount':>12s} "
        f"{'Avg Denied Amount':>12s}"
    )
    logger.info("-" * 140)
   
    for payer in payer_table:
        name = payer["_id"]
        total = payer["total_claims"]
        closed = payer["total_closed"]
        denied = payer["total_denied"]
        avg_claim = payer.get("avg_claim_amount") or 0
        avg_paid = payer.get("avg_paid_amount") or 0
        avg_denied = payer.get("avg_denied_amount") or 0
       
        logger.info(
            f"{name:35s} "
            f"{total:18,} "
            f"{closed:18,} "
            f"{denied:18,} "
            f"${avg_claim:11,.2f} "
            f"${avg_paid:11,.2f} "
            f"${avg_denied:11,.2f}"
        )
    logger.info("\n" + "=" * 80)
   
    logger.info("Top 10 Payers with Most claims")
    top10_payers=payer_table[:10]
    for i in range(len(top10_payers)):
        payers=top10_payers[i]
        name = payers["_id"]
        count = payers["total_claims"]
        logger.info(f"{i+1:2d}. {name:40s} {count:8,} claims")
       
    logger.info("\n" + "=" * 80)    
    logger.info("Payers with least claims")
    least10_payers=payer_table[-10:]
    for i in range(len(least10_payers)):
        payers = least10_payers[i]
        name = payers["_id"]
        count = payers["total_claims"]
        logger.info(f"{i+1:2d}. {name:40s} {count:8,} claims")
   
     
     
    payer_results = {
        "total_claims": total_claims,
        "unique_payers_count": unique_payers_count,
        "all_payers": [
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
        "payer_summary": {
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
    }
    return payer_results
 