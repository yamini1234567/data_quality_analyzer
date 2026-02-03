 
from loguru import logger
from .base import BaseAnalyzer
from .models import DataCount, Charges, ChargeValidation
from datetime import datetime, timedelta
 
class ChargesAnalyzer(BaseAnalyzer):
   
    def __init__(self, db):
        super().__init__(db)
   
    # To get the total count of charges
   
    async def get_total_charges_count(self)->DataCount:
        logger.info("\nCounting total charges")
       
        pipeline = [
            {"$unwind": "$charges"},
            {"$count": "total"}
        ]
       
        result = await self.aggregate(pipeline)
        count = result[0]["total"] if result else 0
       
        logger.info(f"Total Charges: {count:,}")
        return count
    # To get the charge statistics
    
    async def get_charge_statistics(self):
        logger.info("\nCalculating charge statistics")
       
        pipeline = [
            {"$unwind": "$charges"},
            {
                "$group": {
                    "_id": None,
                    "total_charges": {"$sum": "$charges.amount"},
                    "avg_charge": {"$avg": "$charges.amount"},
                    "min_charge": {"$min": "$charges.amount"},
                    "max_charge": {"$max": "$charges.amount"},
                    "count": {"$sum": 1}
                }
            }
        ]
       
        results = await self.aggregate(pipeline)
       
        if not results:
            return None
       
        stats = results[0]
       
        logger.info(f"Total Amount: ${stats.get('total_charges', 0):,.2f}")
        logger.info(f"Average Charge: ${stats.get('avg_charge', 0):,.2f}")
        logger.info(f"Min Charge: ${stats.get('min_charge', 0):,.2f}")
        logger.info(f"Max Charge: ${stats.get('max_charge', 0):,.2f}")
       
        return {
            "total_charges": stats.get("total_charges", 0),
            "avg_charge": stats.get("avg_charge", 0),
            "min_charge": stats.get("min_charge", 0),
            "max_charge": stats.get("max_charge", 0),
            "count": stats.get("count", 0)
        }
   
    # To get the charge ranges distribution
   
    async def get_charge_ranges(self):
        logger.info("\nAnalyzing charge ranges")
       
        if self.total_charges == 0:
            return []
       
        ranges = [
            ("$0 - $500", 0, 500),
            ("$501 - $1,000", 501, 1000),
            ("$1,001 - $2,000", 1001, 2000),
            ("$2,001 - $5,000", 2001, 5000),
            ("$5,001 - $10,000", 5001, 10000),
            ("$10,000+", 10001, float('inf'))
        ]
       
        results = []
        for range_name, min_val, max_val in ranges:
            if max_val == float('inf'):
                match_query = {"charges.amount": {"$gte": min_val}}
            else:
                match_query = {"charges.amount": {"$gte": min_val, "$lte": max_val}}
           
            pipeline = [
                {"$unwind": "$charges"},
                {"$match": match_query},
                {"$count": "count"}
            ]
           
            result = await self.aggregate(pipeline)
            count = result[0]["count"] if result else 0
            percentage = (count / self.total_charges * 100) if self.total_charges > 0 else 0
           
            results.append({
                "range": range_name,
                "count": count,
                "percentage": round(percentage, 2)
            })
       
        return results
   
    # To get high value charges
   
    async def get_highvalue_charges(self):
        logger.info("Analyzing high value charges (>$10,000)")
       
        count_pipeline = [
            {"$unwind": "$charges"},
            {"$match": {"charges.amount": {"$gt": 10000}}},
            {"$count": "total"}
        ]
       
        count_result = await self.aggregate(count_pipeline)
        total_count = count_result[0]["total"] if count_result else 0
       
        logger.info(f"High value charges: {total_count:,}")
       
        top_10_pipeline = [
            {"$unwind": "$charges"},
            {"$match": {"charges.amount": {"$gt": 10000}}},
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
       
        high_charges = await self.aggregate(top_10_pipeline)
       
        return {
            "count": total_count,
            "top_10": [
                {
                    "claim_id": c.get("claimId"),
                    "payer": c.get("payerMCO"),
                    "cpt_code": c.get("cptCode"),
                    "amount": c.get("chargeAmount")
                }
                for c in high_charges
            ]
        }
   
        # To get low value charges
   
    async def get_lowvalue_charges(self)->DataCount:
        logger.info("Analyzing low value charges")
       
        very_low_pipeline = [
            {"$unwind": "$charges"},
            {"$match": {"charges.amount": {"$gt": 0, "$lt": 1}}},
            {"$count": "total"}
        ]
       
        very_low_result = await self.run_pipeline(very_low_pipeline, base_count=self.total_charges)
       
        low_pipeline = [
            {"$unwind": "$charges"},
            {"$match": {"charges.amount": {"$gte": 1, "$lt": 10}}},
            {"$count": "total"}
        ]
       
        low_result = await self.run_pipeline(low_pipeline, base_count=self.total_charges)
       
        logger.info(f"Very low (<$1): {very_low_result.count:,} charges")
        logger.info(f"Low ($1-$10): {low_result.count:,} charges")
       
        return {
            "very_low_count": very_low_result.count,
            "very_low_percentage": very_low_result.percentage,
            "low_count": low_result.count,
            "low_percentage": low_result.percentage
        }
   
    # To find charges where paid amount exceeds charge amount
   
    async def check_paid_greater_than_charge(self):
        logger.info("Checking for paid > charge amount")
       
        pipeline = [
            {"$unwind": "$charges"},
            {"$match": {
                "$expr": {"$gt": ["$charges.amountPaid", "$charges.amount"]}
            }},
            {"$group": {"_id": "$_id"}},
            {"$count": "total"}
        ]
       
        result = await self.run_pipeline(pipeline, base_count=self.total_charges)
        return result
   
    # To find charges where paid + adjustment exceeds charge amount
   
    async def check_paid_plus_adj_greater_than_charge(self)->DataCount:
        logger.info("Checking for (paid + adjustment) > charge amount")
       
        pipeline = [
            {"$unwind": "$charges"},
            {"$match": {
                "$expr": {
                    "$gt": [
                        {"$add": [
                            {"$ifNull": ["$charges.amountPaid", 0]},
                            {"$ifNull": ["$charges.adjustmentAmount", 0]}
                        ]},
                        "$charges.amount"
                    ]
                }
            }},
            {"$group": {"_id": "$_id"}},
            {"$count": "total"}
        ]
       
        result = await self.run_pipeline(pipeline, base_count=self.total_charges)
       
        if result.count > 0:
            logger.warning(f"Found {result.count} charges")
        else:
            logger.info("No issues found")
       
        return result
   
    # To find charges with zero amount
   
    async def check_zero_amount(self):
        logger.info("\nChecking for zero amount charges")
       
        pipeline = [
            {"$unwind": "$charges"},
            {"$match": {"charges.amount": 0}},
            {"$group": {"_id": "$_id"}},
            {"$count": "total"}
        ]
       
        result = await self.run_pipeline(pipeline, base_count=self.total_charges)
        return result
   
    # To find charges with negative amount
   
    async def check_negative_amount(self):
        logger.info("\nChecking for negative amount charges")
       
        pipeline = [
            {"$unwind": "$charges"},
            {"$match": {"charges.amount": {"$lt": 0}}},
            {"$group": {"_id": "$_id"}},
            {"$count": "total"}
        ]
       
        result = await self.run_pipeline(pipeline, base_count=self.total_charges)
        return result
   
    # To find charges with missing unit prices
   
    async def check_missing_unit_prices(self)->DataCount:
        logger.info("Checking for missing unit prices")
       
        pipeline = [
            {"$unwind": "$charges"},
            {"$match": {
                "charges.unit": {"$exists": True, "$gt": 1},
                "$or": [
                    {"charges.unitPrice": {"$exists": False}},
                    {"charges.unitPrice": None},
                    {"charges.unitPrice": 0}
                ]
            }},
            {"$group": {"_id": "$_id"}},
            {"$count": "total"}
        ]
       
        result = await self.run_pipeline(pipeline, base_count=self.total_charges)
        return result
   
    # To find charges with payment but missing remittance details
   
    async def check_charge_remittance_details_missing(self):
        logger.info("\nChecking for missing remittance details")
       
        pipeline = [
            {"$unwind": "$charges"},
            {"$match": {
                "charges.amountPaid": {"$gt": 0},
                "$or": [
                    {"charges.chargeRemittances": {"$size": 0}},
                    {"charges.chargeRemittances": {"$exists": False}}
                ]
            }},
            {"$group": {"_id": "$_id"}},
            {"$count": "total"}
        ]
       
        result = await self.run_pipeline(pipeline, base_count=self.total_charges)    
        return result
   
    # To find charges with extreme unit counts
   
    async def check_extreme_units(self):
        logger.info("\nChecking for extreme unit counts (>100)")
       
        pipeline = [
            {"$unwind": "$charges"},
            {"$match": {
                "charges.unit": {"$gt": 100}
            }},
            {"$group": {"_id": "$_id"}},
            {"$count": "total"}
        ]
       
        result = await self.run_pipeline(pipeline, base_count=self.total_charges)
       
        if result.count > 0:
            logger.warning(f"Found {result.count} charges")
        else:
            logger.info("No issues found")
       
        return result
   
    # To find charges with empty description
   
    async def check_empty_description(self):
        logger.info("\nChecking for empty descriptions")
       
        pipeline = [
            {"$unwind": "$charges"},
            {"$match": {
                "$or": [
                    {"charges.description": {"$exists": False}},
                    {"charges.description": ""}
                ]
            }},
            {"$group": {"_id": "$_id"}},
            {"$count": "total"}
        ]
       
        result = await self.run_pipeline(pipeline, base_count=self.total_charges)  
        return result
   
    async def check_unit_price_calculation_mismatch(self) -> DataCount:
        logger.info("Checking for unit price calculation mismatch")
        
        pipeline = [
            {"$unwind": "$charges"},
            {"$match": {
                "charges.unitPrice": {"$exists": True, "$ne": None, "$gt": 0},
                "charges.unit": {"$exists": True, "$ne": None, "$gt": 0}
            }},
            {"$project": {
                "_id": 1,
                "actualAmount": "$charges.amount",
                "expectedAmount": {"$multiply": ["$charges.unit", "$charges.unitPrice"]}
            }},
            {"$project": {
                "_id": 1,
                "difference": {
                    "$subtract": [
                        {"$max": ["$actualAmount", "$expectedAmount"]},
                        {"$min": ["$actualAmount", "$expectedAmount"]}
                    ]
                }
            }},
            {"$match": {
                "difference": {"$gt": 0.01}
            }},
            {"$group": {"_id": "$_id"}},
            {"$count": "total"}
        ]
        
        return await self.run_pipeline(pipeline)

    async def check_negative_units(self) -> DataCount:
        logger.info("Checking for negative unit counts")
        
        pipeline = [
            {"$unwind": "$charges"},
            {"$match": {
                "charges.unit": {"$lt": 0}
            }},
            {"$group": {"_id": "$_id"}},
            {"$count": "total"}
        ]
        
        return await self.run_pipeline(pipeline)

    async def check_zero_units_with_amount(self) -> DataCount:
        logger.info("Checking for zero units but non-zero amount")
        
        pipeline = [
            {"$unwind": "$charges"},
            {"$match": {
                "$or": [
                    {"charges.unit": 0},
                    {"charges.unit": {"$exists": False}},
                    {"charges.unit": None}
                ],
                "charges.amount": {"$gt": 0}
            }},
            {"$group": {"_id": "$_id"}},
            {"$count": "total"}
        ]
        
        return await self.run_pipeline(pipeline)

    async def check_negative_unit_price(self) -> DataCount:
        logger.info("Checking for negative unit prices")
        
        pipeline = [
            {"$unwind": "$charges"},
            {"$match": {
                "charges.unitPrice": {"$exists": True, "$lt": 0}
            }},
            {"$group": {"_id": "$_id"}},
            {"$count": "total"}
        ]
        
        return await self.run_pipeline(pipeline)

    async def check_missing_service_dates(self) -> DataCount:
        logger.info("Checking for missing service dates")
        
        pipeline = [
            {"$unwind": "$charges"},
            {"$match": {
                "$or": [
                    {"charges.serviceDate": {"$exists": False}},
                    {"charges.serviceDate": None},
                    {"charges.serviceDate": ""}
                ]
            }},
            {"$group": {"_id": "$_id"}},
            {"$count": "total"}
        ]
        
        return await self.run_pipeline(pipeline)
    
    async def check_future_service_dates(self) -> DataCount:
        logger.info("Checking for future service dates")        
        current_date = datetime.now().strftime("%Y-%m-%d")
        
        pipeline = [
            {"$unwind": "$charges"},
            {"$match": {
                "charges.serviceDate": {"$gt": current_date}
            }},
            {"$group": {"_id": "$_id"}},
            {"$count": "total"}
        ]
        
        return await self.run_pipeline(pipeline)

    async def check_very_old_service_dates(self) -> DataCount:
        logger.info("Checking for service dates older than 5 years")
        
        five_years_ago = (datetime.now() - timedelta(days=5*365)).strftime("%Y-%m-%d")
        
        pipeline = [
            {"$unwind": "$charges"},
            {"$match": {
                "charges.serviceDate": {"$lt": five_years_ago}
            }},
            {"$group": {"_id": "$_id"}},
            {"$count": "total"}
        ]
        
        return await self.run_pipeline(pipeline)

    async def check_duplicate_charges_same_date(self) -> DataCount:
        logger.info("Checking for duplicate CPT codes on same service date")        
        pipeline = [
            {"$unwind": "$charges"},
            {"$group": {
                "_id": {
                    "claim": "$_id",
                    "cpt": "$charges.cptHcpcs",
                    "date": "$charges.serviceDate",
                    "modifier": "$charges.modifier"
                },
                "count": {"$sum": 1}
            }},
            {"$match": {"count": {"$gt": 1}}},
            {"$group": {"_id": "$_id.claim"}},
            {"$count": "total"}
        ]
        
        return await self.run_pipeline(pipeline)

    async def check_charges_with_all_zero_amounts(self) -> DataCount:
        logger.info("Checking for claims where all charges are $0")
        
        pipeline = [
            {"$addFields": {
                "totalAmount": {"$sum": "$charges.amount"}
            }},
            {"$match": {
                "charges": {"$exists": True, "$ne": []},
                "totalAmount": 0
            }},
            {"$count": "total"}
        ]
        
        return await self.run_pipeline(pipeline)
    
    
    async def run_all(self):
        logger.info("Starting charges analysis")
        self.total_claims = await self.get_total_claims()
        self.total_charges = await self.get_total_charges_count()
        statistics = await self.get_charge_statistics()
        ranges = await self.get_charge_ranges()
        high_value = await self.get_highvalue_charges()
        low_value = await self.get_lowvalue_charges()
        logger.info("Running validation checks")    
        issues = ChargeValidation(
            paid_greater_than_charge=await self.check_paid_greater_than_charge(),
            paid_plus_adjustment_greater_than_charge=await self.check_paid_plus_adj_greater_than_charge(),
            zero_charges=await self.check_zero_amount(),
            negative_charges=await self.check_negative_amount(),
            missing_unit_prices=await self.check_missing_unit_prices(),
            charge_remittance_details_missing=await self.check_charge_remittance_details_missing(),
            charges_with_extreme_units=await self.check_extreme_units(),
            charges_with_empty_description=await self.check_empty_description(),
            unit_price_calculation_mismatch=await self.check_unit_price_calculation_mismatch(),
            negative_units=await self.check_negative_units(),
            zero_units_with_amount=await self.check_zero_units_with_amount(),
            negative_unit_price=await self.check_negative_unit_price(),
            missing_service_dates=await self.check_missing_service_dates(),
            future_service_dates=await self.check_future_service_dates(),
            very_old_service_dates=await self.check_very_old_service_dates(),
            duplicate_charges_same_date=await self.check_duplicate_charges_same_date(),
            charges_with_all_zero_amounts=await self.check_charges_with_all_zero_amounts()
        )
 
        logger.info("Charges analysis complete")
       
        return Charges(
            statistics=statistics,
            ranges=ranges,
            high_value=high_value,
            low_value=low_value,
            issues=issues
        )
 
 
async def charges_analysis(db):
    analyzer = ChargesAnalyzer(db)
    return await analyzer.run_all()
 