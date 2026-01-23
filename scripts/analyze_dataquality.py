"""Data Quality Analysis for Healthcare Claims."""
import asyncio
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from shared.db import init_db, close_db
from loguru import logger


class DataQualityAnalyzer:
    """Analyzes claims data quality and patterns."""
    
    def __init__(self, db):
        self.db = db
        self.claims = db["claims"]
        
    async def analyze_all(self):
        """Run all analyses."""
        logger.info("="*80)
        logger.info("HEALTHCARE CLAIMS DATA QUALITY ANALYSIS")
        logger.info("="*80)
        
        await self.analyze_overview()
        await self.analyze_payer_distribution()
        await self.analyze_charge_patterns()
        await self.analyze_cpt_codes()
        await self.analyze_diagnoses()
        await self.analyze_data_quality_issues()
        await self.generate_insights()
        
    async def analyze_overview(self):
        """Basic overview of data volume."""
        logger.info("="*80)
        logger.info("1. DATA OVERVIEW")
        logger.info("="*80)
        
        total = await self.claims.count_documents({})
        payers = len(await self.claims.distinct("payerMCO"))
        
        logger.info(f"Total Claims: {total:,}")
        logger.info(f"Unique Payers: {payers}")
        
    async def analyze_payer_distribution(self):
        """Analyze payer distribution."""
        logger.info("="*80)
        logger.info("2. PAYER DISTRIBUTION")
        logger.info("="*80)
        
        pipeline = [
            {"$group": {"_id": "$payerMCO", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]
        
        results = await self.claims.aggregate(pipeline).to_list(None)
        total = sum(p["count"] for p in results)
        
        logger.info(f"Total Payers: {len(results)}")
        logger.info("Top 10 Payers by Claim Volume:")
        logger.info("-"*80)
        
        cumulative = 0
        for i, p in enumerate(results[:10], 1):
            name = p["_id"] or "(Unknown)"
            count = p["count"]
            pct = (count/total)*100
            cumulative += pct
            logger.info(f"  {i:2d}. {name:35s} {count:6,} claims ({pct:5.2f}%) [Cumulative: {cumulative:5.2f}%]")
        
        top5 = sum(p["count"] for p in results[:5])
        top5_pct = (top5/total)*100
        logger.info(f"Top 5 Payers: {top5_pct:.1f}% of all claims")
        
        insufficient = [p for p in results if p["count"] < 100]
        if insufficient:
            logger.warning(f"Payers with < 100 claims: {len(insufficient)}")
        
    async def analyze_charge_patterns(self):
        """Analyze charge amounts."""
        logger.info("="*80)
        logger.info("3. CHARGE PATTERNS")
        logger.info("="*80)
        
        pipeline = [
            {"$unwind": "$charges"},
            {"$group": {
                "_id": None,
                "total": {"$sum": "$charges.amount"},
                "avg": {"$avg": "$charges.amount"},
                "min": {"$min": "$charges.amount"},
                "max": {"$max": "$charges.amount"},
                "count": {"$sum": 1}
            }}
        ]
        
        result = await self.claims.aggregate(pipeline).to_list(1)
        
        if not result:
            logger.warning("No charge data found")
            return
        
        stats = result[0]
        total = stats.get('total') or 0
        avg = stats.get('avg') or 0
        min_val = stats.get('min') or 0
        max_val = stats.get('max') or 0
        count = stats.get('count') or 0
        
        logger.info(f"Total Charge Amount: ${total:,.2f}")
        logger.info(f"Average Charge: ${avg:,.2f}")
        logger.info(f"Min/Max: ${min_val:,.2f} / ${max_val:,.2f}")
        logger.info(f"Total Line Items: {count:,}")
        
        if total == 0 and count > 0:
            logger.warning("All charges are $0.00 - possible data issue")
            return
        
        logger.info("Charge Distribution:")
        logger.info("-"*80)
        
        ranges = [
            ("$0 - $500", 0, 500),
            ("$501 - $1,000", 501, 1000),
            ("$1,001 - $2,000", 1001, 2000),
            ("$2,001 - $5,000", 2001, 5000),
            ("$5,001 - $10,000", 5001, 10000),
            ("$10,000+", 10001, float('inf'))
        ]
        
        for name, min_amt, max_amt in ranges:
            if max_amt == float('inf'):
                cnt = await self.claims.count_documents({"charges.amount": {"$gte": min_amt}})
            else:
                cnt = await self.claims.count_documents({"charges.amount": {"$gte": min_amt, "$lte": max_amt}})
            
            pct = (cnt/count)*100 if count > 0 else 0
            logger.info(f"  {name:20s} {cnt:6,} ({pct:5.2f}%)")
        
        outliers_pipeline = [
            {"$unwind": "$charges"},
            {"$match": {"charges.amount": {"$gt": 10000}}},
            {"$project": {
                "claimId": 1,
                "payerMCO": 1,
                "amount": "$charges.amount",
                "cpt": "$charges.cptHcpcs"
            }},
            {"$sort": {"amount": -1}},
            {"$limit": 10}
        ]
        
        outliers = await self.claims.aggregate(outliers_pipeline).to_list(10)
        
        if outliers:
            logger.warning(f"High-Value Charges (> $10,000): {len(outliers)} found")
            logger.info("Top 10:")
            for i, o in enumerate(outliers, 1):
                claim = o.get('claimId', 'N/A')
                payer = o.get('payerMCO', 'N/A')
                cpt = o.get('cpt', 'N/A')
                amount = o.get('amount', 0) or 0
                logger.info(f"  {i:2d}. Claim: {claim:20s} Payer: {payer:20s} CPT: {cpt:10s} ${amount:,.2f}")
        
        zero_charges = await self.claims.count_documents({"charges.amount": {"$lte": 0}})
        if zero_charges > 0:
            logger.warning(f"Claims with $0 or negative charges: {zero_charges:,}")
        
    async def analyze_cpt_codes(self):
        """Analyze CPT codes."""
        logger.info("="*80)
        logger.info("4. CPT CODE ANALYSIS")
        logger.info("="*80)
        
        pipeline = [
            {"$unwind": "$charges"},
            {"$group": {"_id": "$charges.cptHcpcs", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]
        
        results = await self.claims.aggregate(pipeline).to_list(None)
        
        total_codes = len(results)
        total_usage = sum(r["count"] for r in results)
        total_claims = await self.claims.count_documents({})
        
        logger.info(f"Unique CPT Codes: {total_codes}")
        logger.info(f"Total Uses: {total_usage:,}")
        logger.info(f"Average per Claim: {total_usage/total_claims:.2f}")
        
        logger.info("Top 10 Most Frequent:")
        logger.info("-"*80)
        
        top10_total = 0
        for i, r in enumerate(results[:10], 1):
            code = r["_id"] or "(Unknown)"
            count = r["count"]
            pct = (count/total_usage)*100
            top10_total += count
            logger.info(f"  {i:2d}. {code:10s} {count:6,} uses ({pct:5.2f}%)")
        
        top10_pct = (top10_total/total_usage)*100
        logger.info(f"Top 10 Coverage: {top10_pct:.1f}%")
        
        rare = [r for r in results if r["count"] < 10]
        if rare:
            logger.info(f"Rare CPT codes (< 10 uses): {len(rare)}")
            for r in rare[:10]:
                logger.info(f"  - {r['_id']:10s} {r['count']:2d} uses")
            if len(rare) > 10:
                logger.info(f"  ... and {len(rare)-10} more")
    
    async def analyze_diagnoses(self):
        """Analyze diagnosis patterns."""
        logger.info("="*80)
        logger.info("5. DIAGNOSIS ANALYSIS")
        logger.info("="*80)
        
        with_dx = await self.claims.count_documents({
            "diagnoses": {"$exists": True, "$ne": [], "$ne": None}
        })
        
        total = await self.claims.count_documents({})
        pct = (with_dx/total)*100 if total > 0 else 0
        
        logger.info(f"Claims with Diagnoses: {with_dx:,} ({pct:.2f}%)")
        
        try:
            all_dx = await self.claims.distinct("diagnoses.code")
            unique = len([d for d in all_dx if d])
            logger.info(f"Unique Diagnosis Codes: {unique:,}")
        except Exception:
            logger.warning("Unable to calculate unique diagnosis codes")
        
        pipeline = [
            {"$match": {"diagnoses": {"$exists": True, "$ne": []}}},
            {"$project": {"count": {"$size": "$diagnoses"}}},
            {"$group": {"_id": None, "avg": {"$avg": "$count"}}}
        ]
        
        try:
            result = await self.claims.aggregate(pipeline).to_list(1)
            if result:
                avg = result[0]['avg']
                logger.info(f"Average Diagnoses/Claim: {avg:.2f}")
        except Exception:
            logger.warning("Unable to calculate average diagnoses per claim")
        
    async def analyze_data_quality_issues(self):
        """Identify data quality problems."""
        logger.info("="*80)
        logger.info("6. DATA QUALITY ISSUES")
        logger.info("="*80)
        
        total = await self.claims.count_documents({})
        issues = []
        
        missing_charges = await self.claims.count_documents({
            "$or": [
                {"charges": {"$exists": False}},
                {"charges": []},
                {"charges": None}
            ]
        })
        
        if missing_charges > 0:
            pct = (missing_charges/total)*100
            logger.warning(f"Missing Charges: {missing_charges:,} ({pct:.2f}%)")
            issues.append(missing_charges)
        else:
            logger.info(f"Missing Charges: 0 (0.00%)")
        
        missing_dx = await self.claims.count_documents({
            "$or": [
                {"diagnoses": {"$exists": False}},
                {"diagnoses": []},
                {"diagnoses": None}
            ]
        })
        
        if missing_dx > 0:
            pct = (missing_dx/total)*100
            logger.warning(f"Missing Diagnoses: {missing_dx:,} ({pct:.2f}%)")
            issues.append(missing_dx)
        else:
            logger.info(f"Missing Diagnoses: 0 (0.00%)")
        
        zero_charges = await self.claims.count_documents({"charges.amount": 0})
        if zero_charges > 0:
            pct = (zero_charges/total)*100
            logger.warning(f"Zero Charges: {zero_charges:,} ({pct:.2f}%)")
            issues.append(zero_charges)
        else:
            logger.info(f"Zero Charges: 0 (0.00%)")
        
        negative_charges = await self.claims.count_documents({"charges.amount": {"$lt": 0}})
        if negative_charges > 0:
            logger.warning(f"Negative Charges: {negative_charges:,}")
            issues.append(negative_charges)
        else:
            logger.info(f"Negative Charges: 0")
        
        missing_payer = await self.claims.count_documents({
            "$or": [
                {"payerMCO": {"$exists": False}},
                {"payerMCO": None},
                {"payerMCO": ""}
            ]
        })
        
        if missing_payer > 0:
            pct = (missing_payer/total)*100
            logger.warning(f"Missing Payer: {missing_payer:,} ({pct:.2f}%)")
            issues.append(missing_payer)
        else:
            logger.info(f"Missing Payer: 0 (0.00%)")
        
        issue_count = sum(issues)
        completeness = ((total*5 - issue_count)/(total*5))*100 if total > 0 else 0
        logger.info(f"Overall Data Completeness: {completeness:.2f}%")
        
        if not issues:
            logger.info("No major data quality issues found")
        
    async def generate_insights(self):
        """Generate insights and recommendations."""
        logger.info("="*80)
        logger.info("7. INSIGHTS & RECOMMENDATIONS")
        logger.info("="*80)
        
        total = await self.claims.count_documents({})
        unique_payers = len(await self.claims.distinct("payerMCO"))
        
        pipeline = [
            {"$group": {"_id": "$payerMCO", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]
        
        payers = await self.claims.aggregate(pipeline).to_list(None)
        sufficient = [p for p in payers if p["count"] >= 100]
        
        top5 = sum(p["count"] for p in payers[:5])
        top5_pct = (top5/total)*100 if total > 0 else 0
        
        logger.info("STRENGTHS:")
        logger.info(f"  - Total volume: {total:,} claims")
        logger.info(f"  - Payer diversity: {unique_payers} payers")
        logger.info(f"  - Top 5 concentration: {top5_pct:.1f}%")
        
        if sufficient:
            logger.info(f"  - {len(sufficient)} payers with >= 100 claims")


async def main():
    """Main entry point."""
    client = None
    
    try:
        logger.info("Initializing database connection...")
        client, db = await init_db()
        
        logger.info("Starting analysis...")
        analyzer = DataQualityAnalyzer(db)
        await analyzer.analyze_all()
        
        logger.info("="*80)
        logger.info("ANALYSIS COMPLETE")
        logger.info("="*80)
        
    except Exception as e:
        logger.error("="*80)
        logger.error("ERROR OCCURRED")
        logger.error("="*80)
        logger.error(f"Error: {e}")
        
    finally:
        if client:
            await close_db(client)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.warning("Analysis cancelled by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
