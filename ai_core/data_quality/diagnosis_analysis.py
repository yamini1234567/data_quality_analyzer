from loguru import logger
from ai_core.data_quality.models import DataCount, Diagnosis, DiagnosisValidation


class DiagnosisAnalyzer:
    
    def __init__(self, db):
        self.db = db
        self.claims = db["claims"]
        self.total_claims = 0
    
    async def run_pipeline(self, pipeline):
        result = await self.claims.aggregate(pipeline).to_list(None)
        count = result[0]["total"] if result else 0
        percentage = round((count / self.total_claims * 100), 4) if self.total_claims > 0 else 0.0
        return DataCount(count=count, percentage=percentage)
     
    async def check_missing_diagnosis(self):
        pipeline = [
            {"$match": {
                "$or": [
                    {"diagnoses": {"$size": 0}},
                    {"diagnoses": {"$exists": False}},
                    {"diagnoses": None}
                ]
            }},
            {"$count": "total"}
        ]
        return await self.run_pipeline(pipeline)
    
    async def check_missing_primary_diagnosis(self):
        pipeline = [
            {"$match": {"$nor": [{"diagnoses.isPrimaryDiagnosis": True}]}},
            {"$count": "total"}
        ]
        return await self.run_pipeline(pipeline)
    
    async def check_missing_description(self):
        pipeline = [
            {"$unwind": "$diagnoses"},
            {"$match": {
                "$or": [
                    {"diagnoses.description": {"$exists": False}},
                    {"diagnoses.description": None},
                    {"diagnoses.description": ""}
                ]
            }},
            {"$group": {"_id": "$_id"}},
            {"$count": "total"}
        ]
        return await self.run_pipeline(pipeline)
    
    async def check_missing_code(self):
        pipeline = [
            {"$unwind": "$diagnoses"},
            {"$match": {
                "$or": [
                    {"diagnoses.code": {"$exists": False}},
                    {"diagnoses.code": None},
                    {"diagnoses.code": ""}
                ]
            }},
            {"$group": {"_id": "$_id"}},
            {"$count": "total"}
        ]
        return await self.run_pipeline(pipeline)
    
    async def check_multiple_primary_diagnosis(self):
        pipeline = [
            {"$unwind": "$diagnoses"},
            {"$match": {"diagnoses.isPrimaryDiagnosis": True}},
            {"$group": {"_id": "$_id", "count": {"$sum": 1}}},
            {"$match": {"count": {"$gt": 1}}},
            {"$count": "total"}
        ]
        return await self.run_pipeline(pipeline)
    
    async def check_missing_type(self):
        pipeline = [
            {"$unwind": "$diagnoses"},
            {"$match": {
                "$or": [
                    {"diagnoses.type": {"$exists": False}},
                    {"diagnoses.type": None},
                    {"diagnoses.type": ""}
                ]
            }},
            {"$group": {"_id": "$_id"}},
            {"$count": "total"}
        ]
        return await self.run_pipeline(pipeline)
    
    async def check_missing_status(self):
        pipeline = [
            {"$unwind": "$diagnoses"},
            {"$match": {
                "$or": [
                    {"diagnoses.status": {"$exists": False}},
                    {"diagnoses.status": None},
                    {"diagnoses.status": ""}
                ]
            }},
            {"$group": {"_id": "$_id"}},
            {"$count": "total"}
        ]
        return await self.run_pipeline(pipeline)
    
    async def check_order_1_not_primary(self):
        pipeline = [
            {"$unwind": "$diagnoses"},
            {"$match": {
                "$and": [
                    {"$or": [
                        {"diagnoses.order": "1"},
                        {"diagnoses.order": 1}
                    ]},
                    {"$or": [
                        {"diagnoses.isPrimaryDiagnosis": {"$ne": True}},
                        {"diagnoses.isPrimaryDiagnosis": {"$exists": False}}
                    ]}
                ]
            }},
            {"$group": {"_id": "$_id"}},
            {"$count": "total"}
        ]
        return await self.run_pipeline(pipeline)

    async def check_missing_order(self):
        pipeline = [
            {"$unwind": "$diagnoses"},
            {"$match": {
                "$or": [
                    {"diagnoses.order": {"$exists": False}},
                    {"diagnoses.order": None},
                    {"diagnoses.order": ""}
                ]
            }},
            {"$group": {"_id": "$_id"}},
            {"$count": "total"}
        ]
        return await self.run_pipeline(pipeline)
    
    async def check_duplicate_order(self):
        pipeline = [
            {"$unwind": "$diagnoses"},
            {"$match": {
                "diagnoses.order": {"$exists": True, "$ne": None, "$ne": ""}
            }},
            {"$group": {
                "_id": {
                    "claim_id": "$_id",
                    "order": "$diagnoses.order"
                },
                "count": {"$sum": 1}
            }},
            {"$match": {"count": {"$gt": 1}}},
            {"$group": {"_id": "$_id.claim_id"}},
            {"$count": "total"}
        ]
        return await self.run_pipeline(pipeline)
 
    async def check_missing_occurrence_date(self):
        pipeline = [
            {"$unwind": "$diagnoses"},
            {"$match": {
                "$or": [
                    {"diagnoses.occurrenceDate": {"$exists": False}},
                    {"diagnoses.occurrenceDate": None},
                    {"diagnoses.occurrenceDate": ""}
                ]
            }},
            {"$group": {"_id": "$_id"}},
            {"$count": "total"}
        ]
        return await self.run_pipeline(pipeline)
    
    async def check_missing_present_on_admission(self):
        pipeline = [
            {"$unwind": "$diagnoses"},
            {"$match": {
                "$or": [
                    {"diagnoses.presentOnAdmission": {"$exists": False}},
                    {"diagnoses.presentOnAdmission": None},
                    {"diagnoses.presentOnAdmission": ""}
                ]
            }},
            {"$group": {"_id": "$_id"}},
            {"$count": "total"}
        ]
        return await self.run_pipeline(pipeline)
    
    async def analyze(self):
        logger.info("Starting diagnosis analysis")
        
        self.total_claims = await self.claims.count_documents({})
        
        unique_icd10_pipeline = [
            {"$unwind": "$diagnoses"},
            {"$match": {"diagnoses.code": {"$exists": True, "$ne": None, "$ne": ""}}},
            {"$group": {"_id": "$diagnoses.code"}},
            {"$count": "total"}
        ]
        icd10_result = await self.claims.aggregate(unique_icd10_pipeline).to_list(None)
        unique_icd_10_codes = icd10_result[0]["total"] if icd10_result else 0
        
        unique_icd10_primary_pipeline = [
            {"$unwind": "$diagnoses"},
            {"$match": {"diagnoses.isPrimaryDiagnosis": True}},
            {"$group": {"_id": "$diagnoses.code"}},
            {"$count": "total"}
        ]
        icd10_primary_result = await self.claims.aggregate(unique_icd10_primary_pipeline).to_list(None)
        unique_icd_10_primary_codes = icd10_primary_result[0]["total"] if icd10_primary_result else 0
        
        Issues = DiagnosisValidation(
            missing_diagnosis=await self.check_missing_diagnosis(),
            missing_primary_diagnosis=await self.check_missing_primary_diagnosis(),
            missing_description=await self.check_missing_description(),
            missing_code=await self.check_missing_code(),
            multiple_primary=await self.check_multiple_primary_diagnosis(),
            missing_type=await self.check_missing_type(),
            missing_status=await self.check_missing_status(),
            order_mismatch=await self.check_order_1_not_primary(),
            missing_order=await self.check_missing_order(),
            duplicate_order=await self.check_duplicate_order(),
            missing_occurrence_date=await self.check_missing_occurrence_date(),     
            missing_present_on_admission=await self.check_missing_present_on_admission()    
        )
        
        diagnosis_result = Diagnosis(
            unique_icd_10_codes=unique_icd_10_codes,
            unique_icd_10_primary_codes=unique_icd_10_primary_codes,
            Issues=Issues
        )
        
        logger.info("Diagnosis analysis complete")
        
        return diagnosis_result   
    
async def diagnosis_analysis(db):
    analyzer = DiagnosisAnalyzer(db)
    return await analyzer.analyze()