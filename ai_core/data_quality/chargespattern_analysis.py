 
from loguru import logger
from .base import BaseAnalyzer
from .models import DataCount, Charges, ChargeValidation
from datetime import datetime, timedelta
import asyncio
 
class ChargesAnalyzer(BaseAnalyzer):

    def __init__(self, db):
        super().__init__(db)
   

    # To get the charge statistics
    
    async def get_charge_statistics(self):
        logger.info("Calculating charge statistics")
       
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
        
        if self.total_charges == 0:
            return [] 
        
        pipeline = [
        {"$unwind": "$charges"},
        {
            "$bucketAuto": {
                "groupBy": "$charges.amount",
                "buckets": 6,
                "output": {"count": {"$sum": 1}}
            }
        }
    ]
        results = await self.aggregate(pipeline)
        formatted = []
        for bucket in results:
           min_val = bucket["_id"]["min"]
           max_val = bucket["_id"]["max"]
           count = bucket["count"]
           percentage = (count / self.total_charges * 100) if self.total_charges > 0 else 0
           formatted.append({ "range": f"${min_val:,.2f} - ${max_val:,.2f}",
            "count": count,
            "percentage": round(percentage, 2) })
           
        return formatted
        
    # To get high value charges
   
    async def get_highvalue_charges(self):
        
        logger.info("Analyzing high value charges (>$10,000)")
        pipeline = [
        {"$unwind": "$charges"},
        {"$match": {"charges.amount": {"$gt": 10000}}},
        {
            "$facet": { 
                "count": [
                    {"$count": "total"}
                ],
                "top_10": [
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
            }
        }
    ]
        results = await self.aggregate(pipeline)  
        if not results:
          return {"count": 0, "top_10": []}
        result = results[0]
        total_count = result["count"][0]["total"] if result["count"] else 0
        top_10_list = result["top_10"]
    
        return {
        "count": total_count,
        "top_10": [
            {
                "claim_id": c.get("claimId"),
                "payer": c.get("payerMCO"),
                "cpt_code": c.get("cptCode"),
                "amount": c.get("chargeAmount")
            }
            for c in top_10_list
        ]
    }
        
        # To get low value charges
   
    async def get_lowvalue_charges(self)->DataCount:
        logger.info("Analyzing low value charges")
        
        pipeline = [
        {"$unwind": "$charges"},
        {
            "$facet": {
                "very_low": [
                    {"$match": {"charges.amount": {"$gt": 0, "$lt": 1}}},
                    {"$count": "total"}
                ],
                "low": [
                    {"$match": {"charges.amount": {"$gte": 1, "$lt": 10}}},
                    {"$count": "total"}
                ]
            }
        }
    ]
        results = await self.aggregate(pipeline)
        
        result = results[0]
        very_low_count = result["very_low"][0]["total"] if result["very_low"] else 0
        low_count = result["low"][0]["total"] if result["low"] else 0
        very_low_percentage = round((very_low_count / self.total_charges * 100), 4) if self.total_charges > 0 else 0.0
        low_percentage = round((low_count / self.total_charges * 100), 4) if self.total_charges > 0 else 0.0
    
       
        return {
            "very_low_count": very_low_count,
            "very_low_percentage": very_low_percentage,
            "low_count": low_count,
            "low_percentage": low_percentage
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
        return result
   
    # To find charges with zero amount
   
    async def check_zero_amount(self):
        logger.info("Checking for zero amount charges")
       
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
        logger.info("Checking for negative amount charges")
       
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
        logger.info("Checking for missing remittance details")
       
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
        logger.info("Checking for extreme unit counts (>100)")
       
        pipeline = [
            {"$unwind": "$charges"},
            {"$match": {
                "charges.unit": {"$gt": 100}
            }},
            {"$group": {"_id": "$_id"}},
            {"$count": "total"}
        ]
       
        result = await self.run_pipeline(pipeline, base_count=self.total_charges)
        return result
   
    # To find charges with empty description
   
    async def check_empty_description(self):
        logger.info("Checking for empty descriptions")
       
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
        statistics = await self.get_charge_statistics()
        self.total_charges = statistics["count"] if statistics else 0 
        self.total_claims = await self.get_total_claims()
        (
        ranges,
        high_value,
        low_value
        ) = await asyncio.gather(
        self.get_charge_ranges(),
        self.get_highvalue_charges(),
        self.get_lowvalue_charges()
        )
        logger.info("Running validation checks")
        (
        paid_greater_than_charge,
        paid_plus_adjustment_greater_than_charge,
        zero_charges,
        negative_charges,
        missing_unit_prices,
        charge_remittance_details_missing,
        charges_with_extreme_units,
        charges_with_empty_description,
        unit_price_calculation_mismatch,
        negative_units,
        zero_units_with_amount,
        negative_unit_price,
        missing_service_dates,
        future_service_dates,
        very_old_service_dates,
        duplicate_charges_same_date,
        charges_with_all_zero_amounts
         ) = await asyncio.gather(
        self.check_paid_greater_than_charge(),
        self.check_paid_plus_adj_greater_than_charge(),
        self.check_zero_amount(),
        self.check_negative_amount(),
        self.check_missing_unit_prices(),
        self.check_charge_remittance_details_missing(),
        self.check_extreme_units(),
        self.check_empty_description(),
        self.check_unit_price_calculation_mismatch(),
        self.check_negative_units(),
        self.check_zero_units_with_amount(),
        self.check_negative_unit_price(),
        self.check_missing_service_dates(),
        self.check_future_service_dates(),
        self.check_very_old_service_dates(),
        self.check_duplicate_charges_same_date(),
        self.check_charges_with_all_zero_amounts()
        )

        issues = ChargeValidation( 
            paid_greater_than_charge=paid_greater_than_charge,
            paid_plus_adjustment_greater_than_charge=paid_plus_adjustment_greater_than_charge,
            zero_charges=zero_charges,
            negative_charges=negative_charges,
            missing_unit_prices=missing_unit_prices,
            charge_remittance_details_missing=charge_remittance_details_missing,
            charges_with_extreme_units=charges_with_extreme_units,
            charges_with_empty_description=charges_with_empty_description,
            unit_price_calculation_mismatch=unit_price_calculation_mismatch,
            negative_units=negative_units,
            zero_units_with_amount=zero_units_with_amount,
            negative_unit_price=negative_unit_price,
            missing_service_dates=missing_service_dates,
            future_service_dates=future_service_dates,
            very_old_service_dates=very_old_service_dates,
            duplicate_charges_same_date=duplicate_charges_same_date,
            charges_with_all_zero_amounts=charges_with_all_zero_amounts
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
 
