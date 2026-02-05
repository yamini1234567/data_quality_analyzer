from loguru import logger
from .base import BaseAnalyzer
from .models import DataCount, Charges, ChargeValidation
from datetime import datetime, timedelta
import asyncio
 
class ChargesAnalyzer(BaseAnalyzer):

    def __init__(self, db):
        super().__init__(db)

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
       
        results = await self.aggregate(pipeline)
       
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
           formatted.append({
               "range": f"${min_val:,.2f} - ${max_val:,.2f}",
               "count": count,
               "percentage": round(percentage, 2)
           })
           
        return formatted

    async def get_highvalue_charges(self):
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

    async def get_lowvalue_charges(self):
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

    async def run_simple_validation_checks_combined(self):
        pipeline = [
            {"$unwind": "$charges"},
            {
                "$facet": {
                    "zero_charges": [
                        {"$match": {"charges.amount": 0}},
                        {"$group": {"_id": "$_id"}},
                        {"$count": "total"}
                    ],
                    "negative_charges": [
                        {"$match": {"charges.amount": {"$lt": 0}}},
                        {"$group": {"_id": "$_id"}},
                        {"$count": "total"}
                    ],
                    "empty_description": [
                        {
                            "$match": {
                                "$or": [
                                    {"charges.description": {"$exists": False}},
                                    {"charges.description": ""}
                                ]
                            }
                        },
                        {"$group": {"_id": "$_id"}},
                        {"$count": "total"}
                    ],
                    "extreme_units": [
                        {"$match": {"charges.unit": {"$gt": 100}}},
                        {"$group": {"_id": "$_id"}},
                        {"$count": "total"}
                    ],
                    "negative_units": [
                        {"$match": {"charges.unit": {"$lt": 0}}},
                        {"$group": {"_id": "$_id"}},
                        {"$count": "total"}
                    ],
                    "zero_units_with_amount": [
                        {
                            "$match": {
                                "$or": [
                                    {"charges.unit": 0},
                                    {"charges.unit": {"$exists": False}},
                                    {"charges.unit": None}
                                ],
                                "charges.amount": {"$gt": 0}
                            }
                        },
                        {"$group": {"_id": "$_id"}},
                        {"$count": "total"}
                    ],
                    "negative_unit_price": [
                        {
                            "$match": {
                                "charges.unitPrice": {"$exists": True, "$lt": 0}
                            }
                        },
                        {"$group": {"_id": "$_id"}},
                        {"$count": "total"}
                    ],
                    "missing_service_dates": [
                        {
                            "$match": {
                                "$or": [
                                    {"charges.serviceDate": {"$exists": False}},
                                    {"charges.serviceDate": None},
                                    {"charges.serviceDate": ""}
                                ]
                            }
                        },
                        {"$group": {"_id": "$_id"}},
                        {"$count": "total"}
                    ],
                    "future_service_dates": [
                        {
                            "$match": {
                                "charges.serviceDate": {"$gt": datetime.now().strftime("%Y-%m-%d")}
                            }
                        },
                        {"$group": {"_id": "$_id"}},
                        {"$count": "total"}
                    ],
                    "very_old_service_dates": [
                        {
                            "$match": {
                                "charges.serviceDate": {"$lt": (datetime.now() - timedelta(days=5*365)).strftime("%Y-%m-%d")}
                            }
                        },
                        {"$group": {"_id": "$_id"}},
                        {"$count": "total"}
                    ],
                    "missing_unit_prices": [
                        {
                            "$match": {
                                "charges.unit": {"$exists": True, "$gt": 1},
                                "$or": [
                                    {"charges.unitPrice": {"$exists": False}},
                                    {"charges.unitPrice": None},
                                    {"charges.unitPrice": 0}
                                ]
                            }
                        },
                        {"$group": {"_id": "$_id"}},
                        {"$count": "total"}
                    ],
                    "charge_remittance_details_missing": [
                        {
                            "$match": {
                                "charges.amountPaid": {"$gt": 0},
                                "$or": [
                                    {"charges.chargeRemittances": {"$size": 0}},
                                    {"charges.chargeRemittances": {"$exists": False}}
                                ]
                            }
                        },
                        {"$group": {"_id": "$_id"}},
                        {"$count": "total"}
                    ],
                    "paid_greater_than_charge": [
                        {
                            "$match": {
                                "$expr": {"$gt": ["$charges.amountPaid", "$charges.amount"]}
                            }
                        },
                        {"$group": {"_id": "$_id"}},
                        {"$count": "total"}
                    ],
                    "paid_plus_adjustment_greater_than_charge": [
                        {
                            "$match": {
                                "$expr": {
                                    "$gt": [
                                        {
                                            "$add": [
                                                {"$ifNull": ["$charges.amountPaid", 0]},
                                                {"$ifNull": ["$charges.adjustmentAmount", 0]}
                                            ]
                                        },
                                        "$charges.amount"
                                    ]
                                }
                            }
                        },
                        {"$group": {"_id": "$_id"}},
                        {"$count": "total"}
                    ],
                    "unit_price_calculation_mismatch": [
                        {
                            "$match": {
                                "charges.unitPrice": {"$exists": True, "$ne": None, "$gt": 0},
                                "charges.unit": {"$exists": True, "$ne": None, "$gt": 0}
                            }
                        },
                        {
                            "$project": {
                                "_id": 1,
                                "actualAmount": "$charges.amount",
                                "expectedAmount": {"$multiply": ["$charges.unit", "$charges.unitPrice"]}
                            }
                        },
                        {
                            "$project": {
                                "_id": 1,
                                "difference": {
                                    "$subtract": [
                                        {"$max": ["$actualAmount", "$expectedAmount"]},
                                        {"$min": ["$actualAmount", "$expectedAmount"]}
                                    ]
                                }
                            }
                        },
                        {"$match": {"difference": {"$gt": 0.01}}},
                        {"$group": {"_id": "$_id"}},
                        {"$count": "total"}
                    ],
                    "duplicate_charges_same_date": [
                        {
                            "$group": {
                                "_id": {
                                    "claim": "$_id",
                                    "cpt": "$charges.cptHcpcs",
                                    "date": "$charges.serviceDate",
                                    "modifier": "$charges.modifier"
                                },
                                "count": {"$sum": 1}
                            }
                        },
                        {"$match": {"count": {"$gt": 1}}},
                        {"$group": {"_id": "$_id.claim"}},
                        {"$count": "total"}
                    ]
                }
            }
        ]
        
        results = await self.aggregate(pipeline)
        facet_result = results[0]
        
        return {
            "zero_charges": self.facet_to_datacount(facet_result, "zero_charges"),
            "negative_charges": self.facet_to_datacount(facet_result, "negative_charges"),
            "empty_description": self.facet_to_datacount(facet_result, "empty_description"),
            "extreme_units": self.facet_to_datacount(facet_result, "extreme_units"),
            "negative_units": self.facet_to_datacount(facet_result, "negative_units"),
            "zero_units_with_amount": self.facet_to_datacount(facet_result, "zero_units_with_amount"),
            "negative_unit_price": self.facet_to_datacount(facet_result, "negative_unit_price"),
            "missing_service_dates": self.facet_to_datacount(facet_result, "missing_service_dates"),
            "future_service_dates": self.facet_to_datacount(facet_result, "future_service_dates"),
            "very_old_service_dates": self.facet_to_datacount(facet_result, "very_old_service_dates"),
            "missing_unit_prices": self.facet_to_datacount(facet_result, "missing_unit_prices"),
            "charge_remittance_details_missing": self.facet_to_datacount(facet_result, "charge_remittance_details_missing"),
            "paid_greater_than_charge": self.facet_to_datacount(facet_result, "paid_greater_than_charge"),
            "paid_plus_adjustment_greater_than_charge": self.facet_to_datacount(facet_result, "paid_plus_adjustment_greater_than_charge"),
            "unit_price_calculation_mismatch": self.facet_to_datacount(facet_result, "unit_price_calculation_mismatch"),
            "duplicate_charges_same_date": self.facet_to_datacount(facet_result, "duplicate_charges_same_date")
        }

    async def check_charges_with_all_zero_amounts(self):
        pipeline = [
            {
                "$addFields": {
                    "totalAmount": {"$sum": "$charges.amount"}
                }
            },
            {
                "$match": {
                    "charges": {"$exists": True, "$ne": []},
                    "totalAmount": 0
                }
            },
            {"$count": "total"}
        ]
        
        return await self.run_pipeline(pipeline)
  
    
    async def run_all(self):
        statistics = await self.get_charge_statistics()
        self.total_charges = statistics["count"] if statistics else 0 
        self.total_claims = await self.get_total_claims()
        
        (ranges, high_value, low_value, combined_results, charges_with_all_zero_amounts) = await asyncio.gather(
            self.get_charge_ranges(),
            self.get_highvalue_charges(),
            self.get_lowvalue_charges(),
            self.run_simple_validation_checks_combined(),
            self.check_charges_with_all_zero_amounts()
        )
        
        # Extract all results from combined check
        zero_charges = combined_results["zero_charges"]
        negative_charges = combined_results["negative_charges"]
        charges_with_empty_description = combined_results["empty_description"]
        charges_with_extreme_units = combined_results["extreme_units"]
        negative_units = combined_results["negative_units"]
        zero_units_with_amount = combined_results["zero_units_with_amount"]
        negative_unit_price = combined_results["negative_unit_price"]
        missing_service_dates = combined_results["missing_service_dates"]
        future_service_dates = combined_results["future_service_dates"]
        very_old_service_dates = combined_results["very_old_service_dates"]
        missing_unit_prices = combined_results["missing_unit_prices"]
        charge_remittance_details_missing = combined_results["charge_remittance_details_missing"]
        paid_greater_than_charge = combined_results["paid_greater_than_charge"]
        paid_plus_adjustment_greater_than_charge = combined_results["paid_plus_adjustment_greater_than_charge"]
        unit_price_calculation_mismatch = combined_results["unit_price_calculation_mismatch"]
        duplicate_charges_same_date = combined_results["duplicate_charges_same_date"]

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

