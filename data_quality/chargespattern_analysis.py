import asyncio
import sys
import logging
from pathlib import Path
from typing import Dict, Any, List

project_root = Path(__file__).parent.parent. parent
sys.path.insert(0, str(project_root))

from shared.db import init_db,close_db

logger = logging.getLogger(__name__)
logger.setLevel(logging. INFO)

if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] %(name)s - %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
class charges_analyzer:
    
    def __init__(self,db):
        self.db=db
        self.claims_collection=self.db['claims']


    async def charges_statitics(self):
        logger.info("Charges Statistics")
        charge_pipeline = [
    
            {"$unwind": "$charges"},  
            
            {
                "$group": {
                    "_id": None,
                    "total_charges": {"$sum": "$charges.amount"},
                    "avg_charge": {"$avg": "$charges.amount"},
                    "min_charge": {"$min": "$charges.amount"},
                    "max_charge": {"$max":  "$charges.amount"},
                    "count": {"$sum":1}
                }
            }
        ]
                
        results= await self.claims_collection.aggregate(charge_pipeline).to_list(1)
        
           # Handling cases where no charges exist
        if not results or not results[0]: 
            logger.warning("No charge data found in claims collection")
            return {
                "total_charges": 0,
                "avg_charge": 0,
                "min_charge": 0,
                "max_charge": 0,
                "count": 0,
                "status": "no_data"
            }
        
        stats = results[0]
        
        # Safely extract values with None handling
        total_charges = stats. get('total_charges') or 0
        avg_charge = stats.get('avg_charge') or 0
        min_charge = stats.get('min_charge') or 0
        max_charge = stats.get('max_charge') or 0
        count = stats.get('count') or 0
        
        # Display statistics
        logger.info(f"Total Charge Amount:   ${total_charges:,.2f}")
        logger.info(f"Average Charge:       ${avg_charge:,.2f}")
        logger.info(f"Minimum Charge:       ${min_charge:,.2f}")
        logger.info(f"Maximum Charge:       ${max_charge:,.2f}")
        logger.info(f"Total Line Items:     {count:,}")
        
        # Warning if all charges are zero
        if total_charges == 0 and count > 0:
            logger.warning("All charges are $0.00 - possible data issue")
        
        return {
            "total_charges":  total_charges,
            "avg_charge": avg_charge,
            "min_charge": min_charge,
            "max_charge": max_charge,
            "count": count,
            "status": "completed"
        }
        
        
    async def charge_ranges(self):
        logger.info("=" * 80)
        logger.info("Charges Ranges")
        
        total_pipeline = [
            {"$unwind": "$charges"},
            {"$count": "total"}
        ]
        total_result = await self.claims_collection. aggregate(total_pipeline).to_list(1)
        total_count = total_result[0]['total'] if total_result else 0
        
        if total_count == 0:
            logger.warning("No charges found for distribution analysis")
            return {"status": "no_data"}
        ranges = [
            ("$0 - $500", 0, 500),
            ("$501 - $1,000", 501, 1000),
            ("$1,001 - $2,000", 1001, 2000),
            ("$2,001 - $5,000", 2001, 5000),
            ("$5,001 - $10,000", 5001, 10000),
            ("$10,000+", 10001, float('inf'))
        ]
        logger.info("Charge Distribution by Range:")
        
        ranges_results=[]
        for range_name, min_val, max_val in ranges: 
            if max_val == float('inf'):
                
                match_query = {"charges.amount": {"$gte": min_val}}
            else:
                match_query = {"charges.amount": {"$gte": min_val, "$lte": max_val}}
            
            # Count charges in this range
            pipeline = [
                {"$unwind": "$charges"},
                {"$match": match_query},
                {"$count": "count"}
            ]
            
            result = await self.claims_collection. aggregate(pipeline).to_list(1)
            count_in_range = result[0]['count'] if result else 0
            
            percentage = (count_in_range / total_count) * 100 if total_count > 0 else 0
            
            logger.info(f"  {range_name:20s} {count_in_range:6,} ({percentage:5.2f}%)")
            
            ranges_results.append({
                "range": range_name,
                "count": count_in_range,
                "percentage": round(percentage, 2)
            })
        
        return {
            "total_charges": total_count,
            "ranges": ranges_results,
            "status": "completed"
        }
    
    
    async def highvalue_charges(self):

        logger.info("HIGH-VALUE CHARGES")
        logger.info("=" * 80)
        
        # Find charges > $10,000 and get top 10
        outlier_pipeline = [
            {"$unwind": "$charges"},
            {"$match": {"charges.amount": {"$gt":10000}}},
            {
                "$project": {
                    "claimId": 1,
                    "payerMCO": 1,
                    "chargeAmount": "$charges.amount",
                    "cptCode": "$charges.cptHcpcs"
                }
            },
            {"$sort": {"chargeAmount": -1}},
            {"$limit": 10}
        ]
        
        outliers = await self.claims_collection. aggregate(outlier_pipeline).to_list(10)
        
        if outliers:
            logger.warning(f"Found {len(outliers)} high-value charges (> $10,000)")
            logger.info("\nTop 10 High-Value Charges:")
            logger.info("-" * 80)
            
            for i, outlier in enumerate(outliers, 1):
                claim_id = outlier. get('claimId')
                payer = outlier.get('payerMCO')
                cpt = outlier.get('cptCode')
                amount = outlier.get('chargeAmount') or 0
                
                logger. info(f"  {i:2d}.  Claim:  {claim_id:20s} Payer: {payer:25s} "
                           f"CPT: {cpt:10s} Amount: ${amount:,.2f}")
        else:
            logger.info("No high-value charges (> $10,000) found")
        
        # Also count total charges > $10,000
        total_outliers_pipeline = [
            {"$unwind": "$charges"},
            {"$match": {"charges.amount":  {"$gt": 10000}}},
            {"$count": "total"}
        ]
        
        total_result = await self.claims_collection. aggregate(total_outliers_pipeline).to_list(1)
        total_outliers = total_result[0]['total'] if total_result else 0
        
        if total_outliers > 10:
            logger.info(f"\n(Showing top 10 of {total_outliers} total high-value charges)")
        
        return {
            "high_value_count": total_outliers,
            "top_10":  [
                {
                    "claim_id": o.get('claimId'),
                    "payer": o.get('payerMCO'),
                    "cpt_code": o.get('cptCode'),
                    "amount":  o.get('chargeAmount')
                }
                for o in outliers
            ],
            "status": "completed"
        }
    
    # ZERO OR NEGATIVE CHARGES
    
    async def zero_negative_charges(self):
        logger.info("ZERO OR NEGATIVE CHARGES")
        logger.info("=" * 80)
        
        # Count zero charges
        zero_pipeline = [
            {"$unwind":  "$charges"},
            {"$match": {"charges.amount": 0}},
            {"$count": "total"}
        ]
        
        zero_result = await self.claims_collection.aggregate(zero_pipeline).to_list(1)
        zero_count = zero_result[0]['total'] if zero_result else 0
        
        # Count negative charges
        negative_pipeline = [
            {"$unwind": "$charges"},
            {"$match": {"charges.amount": {"$lt": 0}}},
            {"$count": "total"}
        ]
        
        negative_result = await self.claims_collection.aggregate(negative_pipeline).to_list(1)
        negative_count = negative_result[0]['total'] if negative_result else 0
        
        # Display results
        if zero_count > 0:
            logger.warning(f"Zero Charges:{zero_count:,} charges with $0.00")
        else:
            logger.info(f"Zero Charges:{zero_count}")
        
        if negative_count > 0:
            logger.error(f"Negative Charges:{negative_count:,} charges with negative amounts")
        else:
            logger.info(f"Negative Charges: {negative_count}")
        
        # Get total for percentages
        total_pipeline = [
            {"$unwind": "$charges"},
            {"$count": "total"}
        ]
        total_result = await self. claims_collection.aggregate(total_pipeline).to_list(1)
        total_charges = total_result[0]['total'] if total_result else 0
        
        zero_percentage = (zero_count / total_charges * 100) if total_charges > 0 else 0
        negative_percentage = (negative_count / total_charges * 100) if total_charges > 0 else 0
        
        if zero_count > 0 or negative_count > 0:
            logger.info(f"\nPercentages:")
            if zero_count > 0:
                logger. info(f"  Zero charges:{zero_percentage:.2f}% of all charges")
            if negative_count > 0:
                logger. info(f"Negative charges: {negative_percentage:.2f}% of all charges")
        
        return {
            "zero_count": zero_count,
            "zero_percentage": round(zero_percentage, 2),
            "negative_count": negative_count,
            "negative_percentage": round(negative_percentage, 2),
            "total_charges": total_charges,
            "status": "completed"
        }
    
     #  LOW-VALUE CHARGES 
    
    async def lowvalue_charges(self):
        
        logger.info("LOW-VALUE CHARGES")
        
        # Count charges < $1
        very_low_pipeline = [
            {"$unwind": "$charges"},
            {"$match": {"charges.amount": {"$gt": 0, "$lt": 1}}},
            {"$count": "total"}
        ]
        
        very_low_result = await self.claims_collection.aggregate(very_low_pipeline).to_list(1)
        very_low_count = very_low_result[0]['total'] if very_low_result else 0
        
        # Count charges < $10 (but >= $1)
        low_pipeline = [
            {"$unwind": "$charges"},
            {"$match": {"charges.amount": {"$gte": 1, "$lt": 10}}},
            {"$count": "total"}
        ]
        
        low_result = await self.claims_collection. aggregate(low_pipeline).to_list(1)
        low_count = low_result[0]['total'] if low_result else 0
        
        # Get total for percentages
        total_pipeline = [
            {"$unwind": "$charges"},
            {"$count": "total"}
        ]
        total_result = await self.claims_collection.aggregate(total_pipeline).to_list(1)
        total_charges = total_result[0]['total'] if total_result else 0
        
        very_low_percentage = (very_low_count / total_charges * 100) if total_charges > 0 else 0
        low_percentage = (low_count / total_charges * 100) if total_charges > 0 else 0
        
        # Display results
        logger.info("Charges < $1.00:")
        if very_low_count > 0:
            logger.warning(f"Found {very_low_count:,} charges < $1.00 ({very_low_percentage:.2f}%)")
        else:
            logger.info(f"No value")
        
        logger. info("\nCharges < $10.00:")
        if low_count > 0:
            logger.warning(f"Found {low_count:,} charges between $1.00-$9.99 ({low_percentage:.2f}%)")

        else:
            logger.info(f"No value")
        
        # Listing a few samples of charges less than $1.00
    
        if very_low_count > 0:
            sample_pipeline = [
                {"$unwind": "$charges"},
                {"$match": {"charges.amount": {"$gt": 0, "$lt": 1}}},
                {
                    "$project": {
                        "claimId": 1,
                        "amount": "$charges.amount",
                        "cptCode": "$charges.cptHcpcs",
                        "payer": "$payerMCO"
                    }
                },
                {"$limit": 5}
            ]
            
            samples = await self.claims_collection. aggregate(sample_pipeline).to_list(5)
            
            logger.info("\n  Sample of charges < $1.00:")
            for sample in samples:
                logger.info(f"    Claim {sample['claimId']}: ${sample['amount']:.2f} "
                           f"(CPT: {sample. get('cptCode')})")
        
        return {
            "very_low_count":  very_low_count,
            "very_low_percentage": round(very_low_percentage, 2),
            "low_count": low_count,
            "low_percentage": round(low_percentage, 2),
            "total_charges": total_charges,
            "status": "completed"
        }
        
    
    
    # RUN ALL ANALYSES
    
    async def run_analysis(self):

        logger.info("CHARGE ANALYSIS")
        
        results = {}
        
        # Run all analyses
        results["statistics"] = await self.charges_statitics()
        results["ranges"] = await self.charge_ranges()
        results["high_value"] = await self.highvalue_charges()
        results["zero_negative"] = await self.zero_negative_charges()
        results["low_value"] = await self.lowvalue_charges()
        return results

# MAIN EXECUTION 

async def main():
    client = None
    try:
        logger.info("Initializing database connection")
        client, db = await init_db()
        logger.info("Starting charges pattern analysis")
        
        # Create analyzer
        analyzer = charges_analyzer(db)
        
        # Run analysis
        final_results = await analyzer.run_analysis()
        
        logger.info("Charges Pattern Analysis")
        
        return final_results
        
    except Exception as e:
        logger.error("ERROR OCCURRED")
        logger.error(f"Error: {e}")
        
        import traceback
        traceback.print_exc()
        
    finally:
        if client: 
            await close_db(client)
            logger.info("Database connection closed.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.warning("Analysis cancelled by user")
        logger.warning("=" * 80)
    except Exception as e:
        logger.error("Unexpected error")
        logger.error("=" * 80)
        logger.error(f"Error: {e}")
        
        import traceback
        traceback.print_exc()