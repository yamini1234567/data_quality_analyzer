from loguru import logger


class CPTCodeAnalyzer:
    
    def __init__(self, db):
        self.db = db
        self.claims = db["claims"]
   
   
    async def get_cpt_overview(self):
        pipeline = [
            {"$unwind": "$charges"},
            {"$match": {"charges.cptHcpcs": {"$exists": True, "$ne": None, "$ne": ""}}},
            {"$group": {"_id": "$charges.cptHcpcs", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]
        
        cpt_results = await self.claims.aggregate(pipeline).to_list(None)
        unique_cpt_codes = len(cpt_results)
        total_cpt_uses = sum(item["count"] for item in cpt_results)
        
        total_claims = await self.claims.count_documents({})
        average_cpt_per_claim = round(total_cpt_uses / total_claims, 2) if total_claims > 0 else 0
        
        return {
            "unique_cpt_codes": unique_cpt_codes,
            "total_cpt_uses": total_cpt_uses,
            "total_claims": total_claims,
            "average_cpt_per_claim": average_cpt_per_claim,
            "cpt_details": cpt_results
        }
   
    async def get_top_cpt_codes(self, cpt_details, top_n=10):
        top_cpt_codes = cpt_details[:top_n]
        total_uses = sum(item["count"] for item in cpt_details)
        
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
        rare_codes = [item for item in cpt_details if item["count"] <= threshold]
        rare_count = len(rare_codes)
        total_unique = len(cpt_details)
        rare_percentage = round((rare_count / total_unique) * 100, 2) if total_unique > 0 else 0
        
        return {
            "rare_cpt_count": rare_count,
            "rare_percentage": rare_percentage,
            "rare_codes": rare_codes[:20]
        }
   
    async def analyze_modifier_usage(self):
        pipeline = [
            {"$unwind": "$charges"},
            {"$match": {"charges.cptHcpcs": {"$exists": True, "$ne": None, "$ne": ""}}},
            {"$group": {
                "_id": None,
                "total_charges": {"$sum": 1},
                "with_modifiers": {
                    "$sum": {
                        "$cond": [
                            {"$and": [
                                {"$ne": ["$charges.modifier", None]},
                                {"$ne": ["$charges.modifier", ""]}
                            ]},
                            1,
                            0
                        ]
                    }
                }
            }}
        ]
        
        result = await self.claims.aggregate(pipeline).to_list(None)
        
        if result:
            total = result[0]["total_charges"]
            with_mods = result[0]["with_modifiers"]
            with_percentage = round((with_mods / total) * 100, 2) if total > 0 else 0
            
            return {
                "total_charges": total,
                "with_modifiers": with_mods,
                "without_modifiers": total - with_mods,
                "with_modifiers_percentage": with_percentage
            }
        
        return {}
   
    async def analyze_cpt_financial(self):
        pipeline = [
            {"$unwind": "$charges"},
            {"$match": {"charges.cptHcpcs": {"$exists": True, "$ne": None, "$ne": ""}}},
            {"$group": {
                "_id": "$charges.cptHcpcs",
                "total_revenue": {"$sum": "$charges.amount"},
                "count": {"$sum": 1},
                "avg_amount": {"$avg": "$charges.amount"}
            }},
            {"$sort": {"total_revenue": -1}},
            {"$limit": 10}
        ]
        
        financial_results = await self.claims.aggregate(pipeline).to_list(None)
        total_revenue = sum(item["total_revenue"] for item in financial_results)
        
        return {
            "total_revenue": round(total_revenue, 2),
            "top_revenue_cpt_codes": [
                {
                    "cpt_code": item["_id"],
                    "total_revenue": round(item["total_revenue"], 2),
                    "count": item["count"],
                    "avg_amount": round(item["avg_amount"], 2)
                }
                for item in financial_results
            ]
        }
 
    async def check_missing_cpt_codes(self):
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
        
        result = await self.claims.aggregate(pipeline).to_list(None)
        
        if result:
            total = result[0]["total_charges"]
            missing = result[0]["missing_cpt"]
            percentage = round((missing / total) * 100, 2) if total > 0 else 0
            
            return {
                "total_charges": total,
                "valid_cpt_codes": total - missing,
                "missing_cpt_codes": missing,
                "missing_percentage": percentage
            }
        
        return {}

    async def analyze(self):
            logger.info("Starting CPT code analysis...")
            
            cpt_overview = await self.get_cpt_overview()
            top_cpt_codes = await self.get_top_cpt_codes(cpt_overview["cpt_details"])
            rare_cpt_codes = await self.get_rare_cpt_codes(cpt_overview["cpt_details"])
            modifier_usage = await self.analyze_modifier_usage()
            financial_analysis = await self.analyze_cpt_financial()
            missing_cpt = await self.check_missing_cpt_codes()
            
            logger.info("CPT code analysis complete")
            
            return {
                "cpt_overview": cpt_overview,
                "top_cpt_codes": top_cpt_codes,
                "rare_cpt_codes": rare_cpt_codes,
                "modifier_usage": modifier_usage,
                "financial_analysis": financial_analysis,
                "missing_cpt": missing_cpt
            }
async def cpt_analysis(db):
    analyzer = CPTCodeAnalyzer(db)
    return await analyzer.analyze()