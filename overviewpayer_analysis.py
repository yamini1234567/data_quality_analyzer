import asyncio
import sys
import logging
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent. parent. parent
sys.path.insert(0, str(project_root))

from shared.db import init_db, close_db

# LOGGING SETUP
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] %(name)s - %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # Class to analyze the overview and Payer Distribution 

class OverviewAndPayerAnalyzer:
    def __init__(self, db):
        self.db = db
        self.claims_collection = db["claims"]
        
    async def analyze_overview(self):
        logger.info("DATA OVERVIEW")
        
        # Total claims
        total_claims = await self.claims_collection. count_documents({})
        logger.info(f"Total Claims: {total_claims:,}")
        
        # Unique payers
        unique_payers = len(await self.claims_collection. distinct("payerMCO"))
        logger.info(f"Unique Payers: {unique_payers}")
        
        return {
            "total_claims": total_claims,
            "unique_payers": unique_payers
        }
    async def analyze_payer_distribution(self):     
        logger.info("PAYER DISTRIBUTION ANALYSIS")
        
        # Count claims per payer
        payer_pipeline = [
            {
                "$group": {
                    "_id": "$payerMCO",
                    "claim_count": {"$sum": 1}
                }
            },
            {"$sort": {"claim_count":  -1}}
        ]
        
        payer_results = await self.claims_collection.aggregate(payer_pipeline).to_list(None)
        total_claims = sum(p["claim_count"] for p in payer_results)
        total_payers = len(payer_results)
        
        logger.info(f"Total Payers: {total_payers}")
        
        # Top 10 payers
        logger.info("Top 10 Payers by Claim Volume:")
        logger.info("-" * 80)
        cumulative_percentage = 0
        for i, payer in enumerate(payer_results[:10], 1):
            payer_name = payer["_id"] or "(Unknown)"
            count = payer["claim_count"]
            percentage = (count / total_claims) * 100
            cumulative_percentage += percentage
            
            logger.info(f"{i: 2d}. {payer_name:35s} "
                       f"{count:6,} claims ({percentage:5.2f}%) "
                       f"[Cumulative: {cumulative_percentage:5.2f}%]")
        
        # Top 5 concentration
        top_5_count = sum(p["claim_count"] for p in payer_results[:5])
        top_5_percentage = (top_5_count / total_claims) * 100
        logger.info(f"Top 5 Payers Concentration: {top_5_percentage:.1f}% of all claims")
        
        # Insufficient data payers (< 100 claims)
        insufficient_payers = [p for p in payer_results if p["claim_count"] < 100]
        if insufficient_payers:
            logger.warning(f"Payers with < 100 claims:  {len(insufficient_payers)}")
        
        # Payers (≥ 100 claims)
        sufficient_payers = [p for p in payer_results if p["claim_count"] >= 100]
        if sufficient_payers:   
            logger.info(f"\nPayers with ≥ 100 claims:{len(sufficient_payers)}")
            for payer in sufficient_payers:  
                logger.info(f"{payer['_id']:35s} {payer['claim_count']:6,} claims")
        
        return {
            "total_payers": total_payers,
            "total_claims": total_claims,
            "top_5_concentration": top_5_percentage,
            "insufficient_payers_count": len(insufficient_payers),
            "sufficient_payers_count": len(sufficient_payers)
        }
    async def run_analysis(self):
        logger.info("=" * 80)
        logger.info("OVERVIEW AND PAYER DISTRIBUTION ANALYSIS")
        
        overview_results = await self.analyze_overview()
        payer_results = await self.analyze_payer_distribution()
        
        return {
            "overview": overview_results,
            "payer_distribution": payer_results
        }
async def main():
   
    client = None    
    try:
        logger.info("Initializing database connection...")
        client, db = await init_db()
        logger.info("Starting analysis.. .\n")
        analyzer = OverviewAndPayerAnalyzer(db)
        results = await analyzer.run_analysis()
        logger.info("\n" + "=" * 80)
        logger.info("ANALYSIS COMPLETE")
        logger.info("=" * 80)
        return results
        
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
    except Exception as e:
        logger.error("Unexpected error")
        logger.error(f"Error: {e}")
        
        import traceback
        traceback.print_exc()