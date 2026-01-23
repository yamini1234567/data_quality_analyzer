import asyncio
import sys
import logging
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
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


# Class to analyze CPT Codes
class CPTCodeAnalyzer:
    def __init__(self, db):
        """Initialize with database connection"""
        self.db = db
        self.claims_collection = db["claims"]
    
    async def analyze_cpt_codes(self):
        """Main function to run all CPT code analysis"""
        logger.info("CPT CODE ANALYSIS")

        
        # Step 1: Get unique and total CPT codes
        cpt_data = await self.get_unique_and_total_cpt_codes()
        
        # Step 2: Calculate average CPT codes per claim
        average_data = await self.calculate_average_cpt_per_claim(cpt_data["total_cpt_uses"])
        
        # Step 3: Get top 10 frequent CPT codes
        top_cpt_data = await self.get_top_frequent_cpt_codes(cpt_data["cpt_details"], top_n=10)
        
        # Step 4: Get rare CPT codes
        rare_cpt_data = await self.get_rare_cpt_codes(cpt_data["cpt_details"], threshold=5)
    
        # Step 5: Analyze modifiers
        modifier_data = await self.analyze_modifiers()
    
    
        # Step 6: Financial analysis
        financial_data = await self.analyze_cpt_financial()

        #step 7: CPT code categories
        category_data = await self.analyze_cpt_categories()
        
        # Step 8: Units analysis
        units_data = await self.analyze_cpt_units()
        
        # Step 9: Check missing CPT codes
        missing_data = await self.check_missing_cpt_codes()
        
        return {
            "cpt_overview": cpt_data,
            "average_cpt_per_claim": average_data,
            "top_frequent_cpt_codes": top_cpt_data,
            "rare_cpt_codes": rare_cpt_data,
            "modifier_analysis": modifier_data,
            "financial_analysis": financial_data,
            "cpt_code_categories": category_data,
            "units_analysis": units_data,
            "missing_cpt_codes": missing_data
        }
        
    async def get_unique_and_total_cpt_codes(self):
        """Get unique CPT codes and total usage count"""
        logger.info("\n1. UNIQUE CPT CODES AND TOTAL USES")
        
        
        pipeline = [
            # Step 1: Unwind the charges array
            {"$unwind": "$charges"},
            
            # Step 2: Filter only valid CPT codes 
            {"$match": {
                "charges.cptHcpcs": {
                    "$exists": True,
                    "$ne": None,
                    "$ne": ""
                }
            }},
            
            # Step 3: Group by CPT code and count how many times each appears
            {"$group": {
                "_id": "$charges.cptHcpcs", 
                "count": {"$sum": 1}
            }},
            
            # Step 4: Sort by count
            {"$sort": {"count": -1}}
        ]
        
        # Execute the aggregation
        cpt_results = await self.claims_collection.aggregate(pipeline).to_list(None)
        
        # Calculate metrics
        unique_cpt_codes = len(cpt_results)
        total_cpt_uses = sum(item["count"] for item in cpt_results)
        
        logger.info(f"Unique CPT Codes:     {unique_cpt_codes:,}")
        logger.info(f"Total CPT Code Uses:  {total_cpt_uses:,}")
        
        return {
            "unique_cpt_codes": unique_cpt_codes,
            "total_cpt_uses": total_cpt_uses,
            "cpt_details": cpt_results
        }
        
        
    async def calculate_average_cpt_per_claim(self, total_cpt_uses):
        """Calculate average CPT codes per claim"""
        logger.info("\n2. AVERAGE CPT CODES PER CLAIM")

        
        # Get total number of claims
        total_claims = await self.claims_collection.count_documents({})
        
        # Calculate average
        average_cpt_per_claim = total_cpt_uses / total_claims if total_claims > 0 else 0
        
        logger.info(f"Total Claims:                {total_claims:,}")
        logger.info(f"Total CPT Code Uses:         {total_cpt_uses:,}")
        logger.info(f"Average CPT Codes per Claim: {average_cpt_per_claim:.2f}")
        
        return {
            "total_claims": total_claims,
            "average_cpt_per_claim": round(average_cpt_per_claim, 2)
        }   
        
        
    async def get_top_frequent_cpt_codes(self, cpt_details, top_n=10):
        """Get top N most frequently used CPT codes"""
        logger.info(f"\n3. TOP {top_n} MOST FREQUENT CPT CODES")
        
        # cpt_details is already sorted by count
        top_cpt_codes = cpt_details[:top_n]
        
        logger.info(f"{'Rank':<6} {'CPT Code':<12} {'Usage Count':<15} {'Percentage':<12}")
        
        total_uses = sum(item["count"] for item in cpt_details)
        
        for idx, item in enumerate(top_cpt_codes, 1):
            cpt_code = item["_id"]
            count = item["count"]
            percentage = (count / total_uses) * 100
            
            logger.info(f"{idx:<6} {cpt_code:<12} {count:<15,} {percentage:<12.2f}%")
        
        return {
            "top_cpt_codes": [
                {
                    "rank": idx,
                    "cpt_code": item["_id"],
                    "count": item["count"],
                    "percentage": round((item["count"] / total_uses) * 100, 2)
                }
                for idx, item in enumerate(top_cpt_codes, 1)
            ]
        } 
        
    async def get_rare_cpt_codes(self, cpt_details, threshold=5):
        """Get CPT codes that are used rarely (threshold or fewer times)"""
        logger.info(f"\n4. RARE CPT CODES (Used {threshold} times or less)")
        
        # Filter CPT codes with count <= threshold
        rare_codes = [item for item in cpt_details if item["count"] <= threshold]
        
        rare_count = len(rare_codes)
        total_unique = len(cpt_details)
        rare_percentage = (rare_count / total_unique) * 100 if total_unique > 0 else 0
        
        logger.info(f"Total Rare CPT Codes:        {rare_count:,}")
        logger.info(f"Percentage of Unique Codes:  {rare_percentage:.2f}%")
        
        
        logger.info(f"\nFirst 10 examples of rare CPT codes:")
        logger.info(f"{'CPT Code':<12} {'Usage Count':<15}")
        logger.info("-" * 30)
        
        for item in rare_codes[:10]:
            logger.info(f"{item['_id']:<12} {item['count']:<15}")
        
        return {
            "rare_cpt_count": rare_count,
            "rare_percentage": round(rare_percentage, 2),
            "rare_codes": rare_codes
        }
        
        
    async def analyze_modifiers(self):
        """Analyze CPT code modifiers usage"""
        logger.info(f"\n5. MODIFIER ANALYSIS")
        
        pipeline = [
            {"$unwind": "$charges"},
            {"$match": {
                "charges.cptHcpcs": {"$exists": True, "$ne": None, "$ne": ""}
            }},
            {"$group": {
                "_id": None,
                "total_charges": {"$sum": 1},
                "with_modifiers": {
                    "$sum": {
                        "$cond": [
                            {
                                "$and": [
                                    {"$ne": ["$charges.modifier", None]},
                                    {"$ne": ["$charges.modifier", ""]}
                                ]
                            },
                            1,
                            0
                        ]
                    }
                }
            }}
        ]
        
        result = await self.claims_collection.aggregate(pipeline).to_list(None)
        
        if result:
            total = result[0]["total_charges"]
            with_mods = result[0]["with_modifiers"]
            without_mods = total - with_mods
            
            with_percentage = (with_mods / total) * 100 if total > 0 else 0
            without_percentage = (without_mods / total) * 100 if total > 0 else 0
            
            logger.info(f"Total Charges:              {total:,}")
            logger.info(f"Charges with Modifiers:     {with_mods:,} ({with_percentage:.2f}%)")
            logger.info(f"Charges without Modifiers:  {without_mods:,} ({without_percentage:.2f}%)")
            
            # Get most common modifiers
            modifier_pipeline = [
                {"$unwind": "$charges"},
                {"$match": {
                    "charges.modifier": {"$exists": True, "$ne": None, "$ne": ""}
                }},
                {"$group": {
                    "_id": "$charges.modifier",
                    "count": {"$sum": 1}
                }},
                {"$sort": {"count": -1}},
                {"$limit": 10}
            ]
            
            top_modifiers = await self.claims_collection.aggregate(modifier_pipeline).to_list(None)
            
            if top_modifiers:
                logger.info(f"\nTop 10 Most Common Modifiers:")
                logger.info(f"{'Modifier':<12} {'Usage Count':<15}")
                logger.info("-" * 30)
                
                for item in top_modifiers:
                    modifier_value = item['_id'] if item['_id'] is not None else "(empty)"
                    logger.info(f"{modifier_value:<12} {item['count']:<15,}")
            
            return {
                "total_charges": total,
                "with_modifiers": with_mods,
                "without_modifiers": without_mods,
                "with_modifiers_percentage": round(with_percentage, 2),
                "top_modifiers": top_modifiers
            }
        
        return {}
    
    
    async def analyze_cpt_financial(self):
        """Analyze financial metrics by CPT code"""
        logger.info(f"\n6. FINANCIAL ANALYSIS BY CPT CODE")
        
        # Get financial metrics per CPT code
        pipeline = [
            {"$unwind": "$charges"},
            {"$match": {
                "charges.cptHcpcs": {"$exists": True, "$ne": None, "$ne": ""}
            }},
            {"$group": {
                "_id": "$charges.cptHcpcs",
                "total_revenue": {"$sum": "$charges.amount"},
                "count": {"$sum": 1},
                "avg_amount": {"$avg": "$charges.amount"}
            }},
            {"$sort": {"total_revenue": -1}}
        ]
        
        financial_results = await self.claims_collection.aggregate(pipeline).to_list(None)
        
        # Calculate totals
        total_revenue = sum(item["total_revenue"] for item in financial_results)
        
        # Top 10 by revenue
        top_revenue = financial_results[:10]
        
        logger.info(f"Total Revenue Across All CPT Codes: ${total_revenue:,.2f}")
        logger.info(f"\nTop 10 CPT Codes by Total Revenue:")
        logger.info(f"{'Rank':<6} {'CPT Code':<12} {'Total Revenue':<18} {'Count':<10} {'Avg Amount':<15}")
        logger.info("-" * 70)
        
        for idx, item in enumerate(top_revenue, 1):
            cpt_code = item["_id"]
            revenue = item["total_revenue"]
            count = item["count"]
            avg = item["avg_amount"]
            
            logger.info(f"{idx:<6} {cpt_code:<12} ${revenue:<17,.2f} {count:<10,} ${avg:<14,.2f}")
        
        # Highest and lowest average amounts
        sorted_by_avg = sorted(financial_results, key=lambda x: x["avg_amount"], reverse=True)
        
        logger.info(f"\nTop 5 Highest Average Charge Amounts:")
        logger.info(f"{'CPT Code':<12} {'Avg Amount':<15} {'Count':<10}")
        logger.info("-" * 40)
        
        for item in sorted_by_avg[:5]:
            logger.info(f"{item['_id']:<12} ${item['avg_amount']:<14,.2f} {item['count']:<10,}")
        
        logger.info(f"\nTop 5 Lowest Average Charge Amounts:")
        logger.info(f"{'CPT Code':<12} {'Avg Amount':<15} {'Count':<10}")
        logger.info("-" * 40)
        
        for item in sorted_by_avg[-5:]:
            logger.info(f"{item['_id']:<12} ${item['avg_amount']:<14,.2f} {item['count']:<10,}")
        
        return {
            "total_revenue": round(total_revenue, 2),
            "top_revenue_cpt_codes": top_revenue,
            "highest_avg_amount": sorted_by_avg[:5],
            "lowest_avg_amount": sorted_by_avg[-5:]
        }
        
    async def analyze_cpt_categories(self):
        """Analyze CPT codes by category (Procedure vs Drug vs E&M)"""
        logger.info(f"\n7. CPT CODE CATEGORIES")
        
        pipeline = [
            {"$unwind": "$charges"},
            {"$match": {
                "charges.cptHcpcs": {"$exists": True, "$ne": None, "$ne": ""}
            }},
            {"$project": {
                "cptHcpcs": "$charges.cptHcpcs",
                "amount": "$charges.amount",
                "category": {
                    "$switch": {
                        "branches": [
                            {"case": {"$regexMatch": {"input": "$charges.cptHcpcs", "regex": "^[J]"}}, "then": "Drug Codes (J)"},
                            {"case": {"$regexMatch": {"input": "$charges.cptHcpcs", "regex": "^(99)"}}, "then": "E&M Codes (99xxx)"},
                            {"case": {"$regexMatch": {"input": "$charges.cptHcpcs", "regex": "^(96)"}}, "then": "Procedure Codes (96xxx)"},
                            {"case": {"$regexMatch": {"input": "$charges.cptHcpcs", "regex": "^[Q]"}}, "then": "HCPCS Q Codes"},
                            {"case": {"$regexMatch": {"input": "$charges.cptHcpcs", "regex": "^(9[0-5])"}}, "then": "Other Procedure Codes"}
                        ],
                        "default": "Other Codes"
                    }
                }
            }},
            {"$group": {
                "_id": "$category",
                "count": {"$sum": 1},
                "total_revenue": {"$sum": "$amount"},
                "avg_amount": {"$avg": "$amount"}
            }},
            {"$sort": {"count": -1}}
        ]
        
        category_results = await self.claims_collection.aggregate(pipeline).to_list(None)
        
        total_count = sum(item["count"] for item in category_results)
        total_revenue = sum(item["total_revenue"] for item in category_results)
        
        logger.info(f"{'Category':<30} {'Count':<12} {'%':<10} {'Revenue':<18} {'Avg Amount':<15}")
        logger.info("-" * 90)
        
        for item in category_results:
            category = item["_id"]
            count = item["count"]
            percentage = (count / total_count) * 100
            revenue = item["total_revenue"]
            avg = item["avg_amount"]
            
            logger.info(f"{category:<30} {count:<12,} {percentage:<10.2f} ${revenue:<17,.2f} ${avg:<14,.2f}")
        
        logger.info(f"\n{'TOTAL':<30} {total_count:<12,} {100.0:<10.2f} ${total_revenue:<17,.2f}")
        
        return {
            "categories": category_results,
            "total_count": total_count,
            "total_revenue": round(total_revenue, 2)
        }
        
        
    async def analyze_cpt_units(self):
        """Analyze units per CPT code"""
        logger.info(f"\n8. UNITS ANALYSIS")
        
        pipeline = [
            {"$unwind": "$charges"},
            {"$match": {
                "charges.cptHcpcs": {"$exists": True, "$ne": None, "$ne": ""},
                "charges.unit": {"$exists": True, "$ne": None}  # Changed from "units" to "unit"
            }},
            {"$group": {
                "_id": "$charges.cptHcpcs",
                "avg_units": {"$avg": "$charges.unit"},  # Changed from "units" to "unit"
                "min_units": {"$min": "$charges.unit"},  # Changed from "units" to "unit"
                "max_units": {"$max": "$charges.unit"},  # Changed from "units" to "unit"
                "count": {"$sum": 1}
            }},
            {"$sort": {"avg_units": -1}},
            {"$limit": 10}
        ]
        
        results = await self.claims_collection.aggregate(pipeline).to_list(None)
        
        if results:
            logger.info(f"Top 10 CPT Codes by Average Units:")
            logger.info(f"{'CPT Code':<12} {'Avg Units':<12} {'Min':<8} {'Max':<8} {'Count':<10}")
            logger.info("-" * 60)
            
            for item in results:
                logger.info(f"{item['_id']:<12} {item['avg_units']:<12.2f} {item['min_units']:<8} {item['max_units']:<8} {item['count']:<10,}")
        else:
            logger.info("No unit data found in charges")
        
        return {"top_units_cpt": results if results else []}


    async def check_missing_cpt_codes(self):
        """Check for charges with missing or invalid CPT codes"""
        logger.info(f"\n9. DATA QUALITY - MISSING CPT CODES")
        
        pipeline = [
            {"$unwind": "$charges"},
            {"$group": {
                "_id": None,
                "total_charges": {"$sum": 1},
                "missing_cpt": {
                    "$sum": {
                        "$cond": [
                            {"$or": [
                                {"$eq": ["$charges.cptHcpcs", None]},
                                {"$eq": ["$charges.cptHcpcs", ""]},
                                {"$eq": [{"$ifNull": ["$charges.cptHcpcs", None]}, None]}
                            ]},
                            1,
                            0
                        ]
                    }
                }
            }}
        ]
        
        result = await self.claims_collection.aggregate(pipeline).to_list(None)
        
        if result:
            total = result[0]["total_charges"]
            missing = result[0]["missing_cpt"]
            valid = total - missing
            percentage = (missing / total) * 100 if total > 0 else 0
            
            logger.info(f"Total Charges:           {total:,}")
            logger.info(f"Valid CPT Codes:         {valid:,} ({100-percentage:.2f}%)")
            logger.info(f"Missing CPT Codes:       {missing:,} ({percentage:.2f}%)")
            
            if percentage > 5:
                logger.warning(f"\n  WARNING: {percentage:.1f}% of charges are missing CPT codes!")
                logger.warning(f"   This could result in unbillable charges and revenue loss.")
            elif percentage > 0:
                logger.info(f"\n✓ Good: Only {percentage:.2f}% missing CPT codes")
            else:
                logger.info(f"\n✓ Excellent: All charges have valid CPT codes")
            
            return {
                "total_charges": total,
                "valid_cpt_codes": valid,
                "missing_cpt_codes": missing,
                "missing_percentage": round(percentage, 2)
            }
        
        return {}  





async def main():
    client = None
    
    try:
        logger.info("Initializing database connection...")
        client, db = await init_db()
        
        logger.info("Starting CPT Code analysis...\n")
        analyzer = CPTCodeAnalyzer(db)
        results = await analyzer.analyze_cpt_codes()
        

        logger.info("ANALYSIS COMPLETE")
        
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