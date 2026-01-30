from .models import DataCount, Charges, ChargeValidation
from loguru import logger
 
class ChargesAnalyzer:
   
    def __init__(self, db):
        self.db = db
        self.claims = db["claims"]
        self.total_claims = 0
        self.total_charges = 0
   
    async def run_pipeline(self, pipeline):
        result = await self.claims.aggregate(pipeline).to_list(None)
        count = result[0]["total"] if result else 0
       
        if self.total_charges > 0:
            percentage = round((count / self.total_charges * 100), 4)
        else:
            percentage = 0.0
       
        return DataCount(count=count, percentage=percentage)
 
    async def get_total_charges_count(self):
        pipeline = [
            {"$unwind": "$charges"},
            {"$count": "total"}
        ]
        result = await self.claims.aggregate(pipeline).to_list(1)
        return result[0]["total"] if result else 0
   
    async def get_charge_statistics(self):
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
       
        results = await self.claims.aggregate(pipeline).to_list(1)
       
        if not results:
            return None
       
        stats = results[0]
        return {
            "total_charges": stats.get("total_charges", 0),
            "avg_charge": stats.get("avg_charge", 0),
            "min_charge": stats.get("min_charge", 0),
            "max_charge": stats.get("max_charge", 0),
            "count": stats.get("count", 0)
        }
   
    async def get_charge_ranges(self):
        total_count = self.total_charges
        if total_count == 0:
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
           
            result = await self.claims.aggregate(pipeline).to_list(1)
            count = result[0]["count"] if result else 0
            percentage = (count / total_count * 100) if total_count > 0 else 0
           
            results.append({
                "range": range_name,
                "count": count,
                "percentage": round(percentage, 2)
            })
       
        return results
   
    async def get_highvalue_charges(self):
        count_pipeline = [
            {"$unwind": "$charges"},
            {"$match": {"charges.amount": {"$gt": 10000}}},
            {"$count": "total"}
        ]
        count_result = await self.claims.aggregate(count_pipeline).to_list(1)
        total_count = count_result[0]["total"] if count_result else 0
       
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
        high_charges = await self.claims.aggregate(top_10_pipeline).to_list(10)
       
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
   
    async def get_lowvalue_charges(self):
        very_low_pipeline = [
            {"$unwind": "$charges"},
            {"$match": {"charges.amount": {"$gt": 0, "$lt": 1}}},
            {"$count": "total"}
        ]
        very_low_result = await self.claims.aggregate(very_low_pipeline).to_list(1)
        very_low_count = very_low_result[0]["total"] if very_low_result else 0
       
        low_pipeline = [
            {"$unwind": "$charges"},
            {"$match": {"charges.amount": {"$gte": 1, "$lt": 10}}},
            {"$count": "total"}
        ]
        low_result = await self.claims.aggregate(low_pipeline).to_list(1)
        low_count = low_result[0]["total"] if low_result else 0
       
        very_low_pct = (very_low_count / self.total_charges * 100) if self.total_charges > 0 else 0
        low_pct = (low_count / self.total_charges * 100) if self.total_charges > 0 else 0
       
        return {
            "very_low_count": very_low_count,
            "very_low_percentage": round(very_low_pct, 2),
            "low_count": low_count,
            "low_percentage": round(low_pct, 2)
        }
   
    async def check_paid_greater_than_charge(self):
        pipeline = [
            {"$unwind": "$charges"},
            {"$match": {
                "$expr": {"$gt": ["$charges.amountPaid", "$charges.amount"]}
            }},
            {"$group": {"_id": "$_id"}},
            {"$count": "total"}
        ]
        return await self.run_pipeline(pipeline)
   
    async def check_paid_plus_adj_greater_than_charge(self):
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
        return await self.run_pipeline(pipeline)
   
    async def check_missing_unit_prices(self):
        pipeline = [
            {"$unwind": "$charges"},
            {"$match": {
                "charges.unit": {"$exists": True, "$gt": 1},
                "$or": [
                    {"charges.unitPrice": {"$exists": False}},
                    {"charges.unitPrice": None}
                ]
            }},
            {"$group": {"_id": "$_id"}},
            {"$count": "total"}
        ]
        return await self.run_pipeline(pipeline)
   
    async def check_charge_remittance_details_missing(self):
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
        return await self.run_pipeline(pipeline)
   
    # Check for unit charges which are very high > 100
           
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
        return await self.run_pipeline(pipeline)
   
    # Check whether the description feild under charges is empty o
   
    async def check_empty_description(self):
        logger.info("Checking for empty descriptions...")
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
        return await self.run_pipeline(pipeline)
   
    async def check_zero_amount(self):
        pipeline = [
            {"$unwind": "$charges"},
            {"$match": {"charges.amount": 0}},
            {"$group": {"_id": "$_id"}},
            {"$count": "total"}
        ]
        return await self.run_pipeline(pipeline)
   
    async def check_negative_amount(self):
        pipeline = [
            {"$unwind": "$charges"},
            {"$match": {"charges.amount": {"$lt": 0}}},
            {"$group": {"_id": "$_id"}},
            {"$count": "total"}
        ]
        return await self.run_pipeline(pipeline)
   
   
    async def run_all(self):
        logger.info("Starting charges analysis...")
       
        self.total_claims = await self.claims.count_documents({})
        self.total_charges = await self.get_total_charges_count()
       
        statistics = await self.get_charge_statistics()
        ranges = await self.get_charge_ranges()
        high_value = await self.get_highvalue_charges()
        low_value = await self.get_lowvalue_charges()
       
        issues = ChargeValidation(
            paid_greater_than_charge=await self.check_paid_greater_than_charge(),
            paid_plus_adjustment_greater_than_charge=await self.check_paid_plus_adj_greater_than_charge(),
            zero_charges=await self.check_zero_amount(),
            negative_charges=await self.check_negative_amount(),
            missing_unit_prices=await self.check_missing_unit_prices(),
            charge_remittance_details_missing=await self.check_charge_remittance_details_missing(),
            charges_with_extreme_units=await self.check_extreme_units(),
            charges_with_empty_description=await self.check_empty_description()
           
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
 