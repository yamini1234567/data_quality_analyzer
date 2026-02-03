from loguru import logger
from ai_core.data_quality.models import DataCount, Diagnosis, DiagnosisValidation
from ai_core.data_quality.base import BaseAnalyzer
 
 
class DiagnosisAnalyzer(BaseAnalyzer):
   
    def __init__(self, db):
        super().__init__(db)
       
    async def check_missing_diagnosis(self)->DataCount:
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
   
    async def check_missing_primary_diagnosis(self)->DataCount:
        pipeline = [
            {"$match": {"$nor": [{"diagnoses.isPrimaryDiagnosis": True}]}},
            {"$count": "total"}
        ]
        return await self.run_pipeline(pipeline)
   
    async def check_missing_description(self)->DataCount:
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
   
    async def check_missing_code(self)->DataCount:
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
   
    async def check_multiple_primary_diagnosis(self)->DataCount:
        pipeline = [
            {"$unwind": "$diagnoses"},
            {"$match": {"diagnoses.isPrimaryDiagnosis": True}},
            {"$group": {"_id": "$_id", "count": {"$sum": 1}}},
            {"$match": {"count": {"$gt": 1}}},
            {"$count": "total"}
        ]
        return await self.run_pipeline(pipeline)
   
    async def check_missing_type(self)->DataCount:
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
   
    async def check_missing_status(self)->DataCount:
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
   
    async def check_order_1_not_primary(self)->DataCount:
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
 
    async def check_missing_order(self)->DataCount:
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
   
    async def check_duplicate_order(self)->DataCount:
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
 
    async def check_missing_occurrence_date(self)->DataCount:
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
   
    async def check_missing_present_on_admission(self)->DataCount:
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
   
    async def check_invalid_icd10_format(self) -> DataCount:
        logger.info("Checking for invalid ICD-10 format")
        
        pipeline = [
            {"$unwind": "$diagnoses"},
            {"$match": {
                "diagnoses.code": {"$exists": True, "$ne": None, "$ne": ""},
                "$expr": {
                    "$not": {
                        "$regexMatch": {
                            "input": "$diagnoses.code",
                            "regex": "^[A-Z][0-9A-Z]{2,6}$"
                        }
                    }
                }
            }},
            {"$group": {"_id": "$_id"}},
            {"$count": "total"}
        ]
        
        return await self.run_pipeline(pipeline)

    async def check_primary_diagnosis_not_order_1(self) -> DataCount:
        pipeline = [
            {"$unwind": "$diagnoses"},
            {"$match": {
                "diagnoses.isPrimaryDiagnosis": True,
                "$and": [
                    {"diagnoses.order": {"$ne": "1"}},
                    {"diagnoses.order": {"$ne": 1}}
                ]
            }},
            {"$group": {"_id": "$_id"}},
            {"$count": "total"}
        ]
        
        return await self.run_pipeline(pipeline)

    async def check_invalid_diagnosis_status(self) -> DataCount:
        valid_statuses = ["A", "W", "I", "R", "D"]
        
        pipeline = [
            {"$unwind": "$diagnoses"},
            {"$match": {
                "diagnoses.status": {
                    "$exists": True, 
                    "$ne": None, 
                    "$ne": "",
                    "$nin": valid_statuses
                }
            }},
            {"$group": {"_id": "$_id"}},
            {"$count": "total"}
        ]
        
        return await self.run_pipeline(pipeline)


    async def check_invalid_diagnosis_type(self) -> DataCount:
    
        valid_types = ["ABK", "ABF", "BK", "BF", "PRIMARY", "SECONDARY"]
        
        pipeline = [
            {"$unwind": "$diagnoses"},
            {"$match": {
                "diagnoses.type": {
                    "$exists": True,
                    "$ne": None,
                    "$ne": "",
                    "$nin": valid_types
                }
            }},
            {"$group": {"_id": "$_id"}},
            {"$count": "total"}
        ]
        
        return await self.run_pipeline(pipeline)

    async def run_all(self):
        logger.info("Starting diagnosis analysis")
       
        self.total_claims = await self.get_total_claims()
       
        unique_icd10_pipeline = [
            {"$unwind": "$diagnoses"},
            {"$match": {"diagnoses.code": {"$exists": True, "$ne": None, "$ne": ""}}},
            {"$group": {"_id": "$diagnoses.code"}},
            {"$count": "total"}
        ]
        icd10_result = await self.aggregate(unique_icd10_pipeline)
        unique_icd_10_codes = icd10_result[0]["total"] if icd10_result else 0
       
        unique_icd10_primary_pipeline = [
            {"$unwind": "$diagnoses"},
            {"$match": {"diagnoses.isPrimaryDiagnosis": True}},
            {"$group": {"_id": "$diagnoses.code"}},
            {"$count": "total"}
        ]
        icd10_primary_result = await self.aggregate(unique_icd10_primary_pipeline)
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
            missing_present_on_admission=await self.check_missing_present_on_admission(),
            invalid_icd10_format=await self.check_invalid_icd10_format(),
            primary_diagnosis_not_order_1=await self.check_primary_diagnosis_not_order_1(),
            invalid_diagnosis_status=await self.check_invalid_diagnosis_status(),
            invalid_diagnosis_type=await self.check_invalid_diagnosis_type()
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
    return await analyzer.run_all()
 
 