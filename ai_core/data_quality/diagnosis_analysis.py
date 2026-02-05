from loguru import logger
from ai_core.data_quality.models import DataCount, Diagnosis, DiagnosisValidation
from ai_core.data_quality.base import BaseAnalyzer
import asyncio

class DiagnosisAnalyzer(BaseAnalyzer):
   
    def __init__(self, db,filters=None):
        super().__init__(db,filters)
    
    async def run_claim_level_checks_combined(self):
        pipeline = [
            {
                "$facet": {
                    "missing_diagnosis": [
                        {
                            "$match": {
                                "$or": [
                                    {"diagnoses": {"$size": 0}},
                                    {"diagnoses": {"$exists": False}},
                                    {"diagnoses": None}
                                ]
                            }
                        },
                        {"$count": "total"}
                    ],
                    "missing_primary_diagnosis": [
                        {"$match": {"$nor": [{"diagnoses.isPrimaryDiagnosis": True}]}},
                        {"$count": "total"}
                    ]
                }
            }
        ]
        
        results = await self.aggregate(pipeline)
        facet_result = results[0]
        
        return {
            "missing_diagnosis": self.facet_to_datacount(facet_result, "missing_diagnosis"),
            "missing_primary_diagnosis": self.facet_to_datacount(facet_result, "missing_primary_diagnosis")
        }
    
    async def run_diagnosis_level_checks_combined(self):
        valid_statuses = ["A", "W", "I", "R", "D"]
        valid_types = ["ABK", "ABF", "BK", "BF", "PRIMARY", "SECONDARY"]
        
        pipeline = [
            {"$unwind": "$diagnoses"},
            {
                "$facet": {
                    "missing_description": [
                        {
                            "$match": {
                                "$or": [
                                    {"diagnoses.description": {"$exists": False}},
                                    {"diagnoses.description": None},
                                    {"diagnoses.description": ""}
                                ]
                            }
                        },
                        {"$group": {"_id": "$_id"}},
                        {"$count": "total"}
                    ],
                    "missing_code": [
                        {
                            "$match": {
                                "$or": [
                                    {"diagnoses.code": {"$exists": False}},
                                    {"diagnoses.code": None},
                                    {"diagnoses.code": ""}
                                ]
                            }
                        },
                        {"$group": {"_id": "$_id"}},
                        {"$count": "total"}
                    ],
                    "multiple_primary": [
                        {"$match": {"diagnoses.isPrimaryDiagnosis": True}},
                        {"$group": {"_id": "$_id", "count": {"$sum": 1}}},
                        {"$match": {"count": {"$gt": 1}}},
                        {"$count": "total"}
                    ],
                    "missing_type": [
                        {
                            "$match": {
                                "$or": [
                                    {"diagnoses.type": {"$exists": False}},
                                    {"diagnoses.type": None},
                                    {"diagnoses.type": ""}
                                ]
                            }
                        },
                        {"$group": {"_id": "$_id"}},
                        {"$count": "total"}
                    ],
                    "missing_status": [
                        {
                            "$match": {
                                "$or": [
                                    {"diagnoses.status": {"$exists": False}},
                                    {"diagnoses.status": None},
                                    {"diagnoses.status": ""}
                                ]
                            }
                        },
                        {"$group": {"_id": "$_id"}},
                        {"$count": "total"}
                    ],
                    "order_mismatch": [
                        {
                            "$match": {
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
                            }
                        },
                        {"$group": {"_id": "$_id"}},
                        {"$count": "total"}
                    ],
                    "missing_order": [
                        {
                            "$match": {
                                "$or": [
                                    {"diagnoses.order": {"$exists": False}},
                                    {"diagnoses.order": None},
                                    {"diagnoses.order": ""}
                                ]
                            }
                        },
                        {"$group": {"_id": "$_id"}},
                        {"$count": "total"}
                    ],
                    "duplicate_order": [
                        {
                            "$match": {
                                "diagnoses.order": {"$exists": True, "$ne": None, "$ne": ""}
                            }
                        },
                        {
                            "$group": {
                                "_id": {
                                    "claim_id": "$_id",
                                    "order": "$diagnoses.order"
                                },
                                "count": {"$sum": 1}
                            }
                        },
                        {"$match": {"count": {"$gt": 1}}},
                        {"$group": {"_id": "$_id.claim_id"}},
                        {"$count": "total"}
                    ],
                    "missing_occurrence_date": [
                        {
                            "$match": {
                                "$or": [
                                    {"diagnoses.occurrenceDate": {"$exists": False}},
                                    {"diagnoses.occurrenceDate": None},
                                    {"diagnoses.occurrenceDate": ""}
                                ]
                            }
                        },
                        {"$group": {"_id": "$_id"}},
                        {"$count": "total"}
                    ],
                    "missing_present_on_admission": [
                        {
                            "$match": {
                                "$or": [
                                    {"diagnoses.presentOnAdmission": {"$exists": False}},
                                    {"diagnoses.presentOnAdmission": None},
                                    {"diagnoses.presentOnAdmission": ""}
                                ]
                            }
                        },
                        {"$group": {"_id": "$_id"}},
                        {"$count": "total"}
                    ],
                    "invalid_icd10_format": [
                        {
                            "$match": {
                                "diagnoses.code": {"$exists": True, "$ne": None, "$ne": ""},
                                "$expr": {
                                    "$not": {
                                        "$regexMatch": {
                                            "input": "$diagnoses.code",
                                            "regex": "^[A-Z][0-9A-Z]{2,6}$"
                                        }
                                    }
                                }
                            }
                        },
                        {"$group": {"_id": "$_id"}},
                        {"$count": "total"}
                    ],
                    "primary_diagnosis_not_order_1": [
                        {
                            "$match": {
                                "diagnoses.isPrimaryDiagnosis": True,
                                "$and": [
                                    {"diagnoses.order": {"$ne": "1"}},
                                    {"diagnoses.order": {"$ne": 1}}
                                ]
                            }
                        },
                        {"$group": {"_id": "$_id"}},
                        {"$count": "total"}
                    ],
                    "invalid_diagnosis_status": [
                        {
                            "$match": {
                                "diagnoses.status": {
                                    "$exists": True,
                                    "$ne": None,
                                    "$ne": "",
                                    "$nin": valid_statuses
                                }
                            }
                        },
                        {"$group": {"_id": "$_id"}},
                        {"$count": "total"}
                    ],
                    "invalid_diagnosis_type": [
                        {
                            "$match": {
                                "diagnoses.type": {
                                    "$exists": True,
                                    "$ne": None,
                                    "$ne": "",
                                    "$nin": valid_types
                                }
                            }
                        },
                        {"$group": {"_id": "$_id"}},
                        {"$count": "total"}
                    ]
                }
            }
        ]
        
        results = await self.aggregate(pipeline)
        facet_result = results[0]
        
        return {
            "missing_description": self.facet_to_datacount(facet_result, "missing_description"),
            "missing_code": self.facet_to_datacount(facet_result, "missing_code"),
            "multiple_primary": self.facet_to_datacount(facet_result, "multiple_primary"),
            "missing_type": self.facet_to_datacount(facet_result, "missing_type"),
            "missing_status": self.facet_to_datacount(facet_result, "missing_status"),
            "order_mismatch": self.facet_to_datacount(facet_result, "order_mismatch"),
            "missing_order": self.facet_to_datacount(facet_result, "missing_order"),
            "duplicate_order": self.facet_to_datacount(facet_result, "duplicate_order"),
            "missing_occurrence_date": self.facet_to_datacount(facet_result, "missing_occurrence_date"),
            "missing_present_on_admission": self.facet_to_datacount(facet_result, "missing_present_on_admission"),
            "invalid_icd10_format": self.facet_to_datacount(facet_result, "invalid_icd10_format"),
            "primary_diagnosis_not_order_1": self.facet_to_datacount(facet_result, "primary_diagnosis_not_order_1"),
            "invalid_diagnosis_status": self.facet_to_datacount(facet_result, "invalid_diagnosis_status"),
            "invalid_diagnosis_type": self.facet_to_datacount(facet_result, "invalid_diagnosis_type")
        }

    async def run_all(self):
        
        self.total_claims = await self.get_total_claims()
        
        icd10_counts_pipeline = [
            {"$unwind": "$diagnoses"},
            {
                "$facet": {
                    "all_codes": [
                        {"$match": {"diagnoses.code": {"$exists": True, "$ne": None, "$ne": ""}}},
                        {"$group": {"_id": "$diagnoses.code"}},
                        {"$count": "total"}
                    ],
                    "primary_codes": [
                        {"$match": {"diagnoses.isPrimaryDiagnosis": True}},
                        {"$group": {"_id": "$diagnoses.code"}},
                        {"$count": "total"}
                    ]
                }
            }
        ]
        
        (icd10_counts, claim_results, diagnosis_results) = await asyncio.gather(
            self.aggregate(icd10_counts_pipeline),
            self.run_claim_level_checks_combined(),
            self.run_diagnosis_level_checks_combined()
        )
        
        result = icd10_counts[0] if icd10_counts else {}
        all_codes = result.get("all_codes", [])
        unique_icd_10_codes = all_codes[0].get("total", 0) if all_codes else 0
        primary_codes = result.get("primary_codes", [])
        unique_icd_10_primary_codes = primary_codes[0].get("total", 0) if primary_codes else 0
        
        Issues = DiagnosisValidation(
            missing_diagnosis=claim_results["missing_diagnosis"],
            missing_primary_diagnosis=claim_results["missing_primary_diagnosis"],
            missing_description=diagnosis_results["missing_description"],
            missing_code=diagnosis_results["missing_code"],
            multiple_primary=diagnosis_results["multiple_primary"],
            missing_type=diagnosis_results["missing_type"],
            missing_status=diagnosis_results["missing_status"],
            order_mismatch=diagnosis_results["order_mismatch"],
            missing_order=diagnosis_results["missing_order"],
            duplicate_order=diagnosis_results["duplicate_order"],
            missing_occurrence_date=diagnosis_results["missing_occurrence_date"],
            missing_present_on_admission=diagnosis_results["missing_present_on_admission"],
            invalid_icd10_format=diagnosis_results["invalid_icd10_format"],
            primary_diagnosis_not_order_1=diagnosis_results["primary_diagnosis_not_order_1"],
            invalid_diagnosis_status=diagnosis_results["invalid_diagnosis_status"],
            invalid_diagnosis_type=diagnosis_results["invalid_diagnosis_type"]
        )
        
        return Diagnosis(
            unique_icd_10_codes=unique_icd_10_codes,
            unique_icd_10_primary_codes=unique_icd_10_primary_codes,
            Issues=Issues
       )
    
   
async def diagnosis_analysis(db, filters=None):
    analyzer = DiagnosisAnalyzer(db, filters)
    return await analyzer.run_all()
