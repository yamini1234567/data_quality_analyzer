"""
Query pipelines for fetching example claims for each data quality issue
"""

def get_example_pipeline(issue_name: str, limit: int = 10):
    """
    Returns aggregation pipeline for fetching examples of specific issues
    Returns None if no pipeline defined for this issue
    """
    
    pipelines = {
        # ============= CLAIMS MODULE =============
        "denied_with_payment": [
            {"$match": {"claimStatus": "Denied", "claimAmountPaid": {"$gt": 0}}},
            {"$limit": limit}
        ],
        
        "denied_without_remittances": [
            {"$match": {"claimStatus": "Denied"}},
            {"$unwind": "$charges"},
            {
                "$match": {
                    "$or": [
                        {"charges.chargeRemittances": {"$exists": False}},
                        {"charges.chargeRemittances": []},
                        {"charges.chargeRemittances": None}
                    ]
                }
            },
            {"$group": {"_id": "$_id", "doc": {"$first": "$$ROOT"}}},
            {"$replaceRoot": {"newRoot": "$doc"}},
            {"$limit": limit}
        ],
        
        "denied_with_overpayment": [
            {"$match": {"claimStatus": "Denied", "$expr": {"$gt": ["$claimAmountPaid", "$claimAmount"]}}},
            {"$limit": limit}
        ],
        
        "open_with_payment": [
            {"$match": {"claimStatus": "Open", "claimAmountPaid": {"$gt": 0}}},
            {"$limit": limit}
        ],
        
        "paidamount_greater_than_claimamount": [
            {"$match": {"$expr": {"$gt": ["$claimAmountPaid", "$claimAmount"]}}},
            {"$limit": limit}
        ],
        
        "adjamount_greater_than_claimamount": [
            {"$match": {"$expr": {"$gt": ["$claimAdjAmount", "$claimAmount"]}}},
            {"$limit": limit}
        ],
        
        "paid_plus_adjustment_exceeds_claim": [
            {
                "$match": {
                    "$expr": {
                        "$gt": [
                            {"$add": [{"$ifNull": ["$claimAmountPaid", 0]}, {"$ifNull": ["$claimAdjAmount", 0]}]},
                            "$claimAmount"
                        ]
                    }
                }
            },
            {"$limit": limit}
        ],
        
        "claim_sum_mismatch": [
            {"$unwind": "$charges"},
            {
                "$group": {
                    "_id": "$_id",
                    "doc": {"$first": "$$ROOT"},
                    "claimAmount": {"$first": "$claimAmount"},
                    "totalCharges": {"$sum": "$charges.amount"}
                }
            },
            {
                "$match": {
                    "$expr": {
                        "$ne": [
                            {"$round": ["$claimAmount", 2]},
                            {"$round": ["$totalCharges", 2]}
                        ]
                    }
                }
            },
            {"$replaceRoot": {"newRoot": "$doc"}},
            {"$limit": limit}
        ],
        
        "duplicate_claims": [
            {"$group": {"_id": "$claimId", "docs": {"$push": "$$ROOT"}, "count": {"$sum": 1}}},
            {"$match": {"count": {"$gt": 1}}},
            {"$unwind": "$docs"},
            {"$replaceRoot": {"newRoot": "$docs"}},
            {"$limit": limit}
        ],
        
        "closed_with_zero_amtpaid_and_adj": [
            {"$match": {"claimStatus": "Closed", "claimAmountPaid": 0, "claimAdjAmount": 0}},
            {"$limit": limit}
        ],
        
        "claim_amount_paid_sum_mismatch": [
            {"$unwind": "$charges"},
            {
                "$group": {
                    "_id": "$_id",
                    "doc": {"$first": "$$ROOT"},
                    "claimAmountPaid": {"$first": "$claimAmountPaid"},
                    "totalChargesPaid": {"$sum": "$charges.amountPaid"}
                }
            },
            {
                "$match": {
                    "$expr": {
                        "$ne": [
                            {"$round": ["$claimAmountPaid", 2]},
                            {"$round": ["$totalChargesPaid", 2]}
                        ]
                    }
                }
            },
            {"$replaceRoot": {"newRoot": "$doc"}},
            {"$limit": limit}
        ],
        
        "claim_adj_amount_sum_mismatch": [
            {"$unwind": "$charges"},
            {
                "$group": {
                    "_id": "$_id",
                    "doc": {"$first": "$$ROOT"},
                    "claimAdjAmount": {"$first": "$claimAdjAmount"},
                    "totalChargesAdj": {"$sum": "$charges.adjustmentAmount"}
                }
            },
            {
                "$match": {
                    "$expr": {
                        "$ne": [
                            {"$round": ["$claimAdjAmount", 2]},
                            {"$round": ["$totalChargesAdj", 2]}
                        ]
                    }
                }
            },
            {"$replaceRoot": {"newRoot": "$doc"}},
            {"$limit": limit}
        ],
        
        "closed_with_remaining_balanceamt": [
            {
                "$match": {
                    "claimStatus": "Closed",
                    "$expr": {
                        "$gt": [
                            {
                                "$subtract": [
                                    "$claimAmount",
                                    {"$add": [{"$ifNull": ["$claimAmountPaid", 0]}, {"$ifNull": ["$claimAdjAmount", 0]}]}
                                ]
                            },
                            0
                        ]
                    }
                }
            },
            {"$limit": limit}
        ],
        
        # ============= CHARGES MODULE =============
        "missing_service_dates": [
            {"$unwind": "$charges"},
            {"$match": {"charges.serviceDate": {"$in": [None, ""]}}},
            {"$group": {"_id": "$_id", "doc": {"$first": "$$ROOT"}}},
            {"$replaceRoot": {"newRoot": "$doc"}},
            {"$limit": limit}
        ],
        
        "paid_greater_than_charge": [
            {"$unwind": "$charges"},
            {"$match": {"$expr": {"$gt": ["$charges.amountPaid", "$charges.amount"]}}},
            {"$group": {"_id": "$_id", "doc": {"$first": "$$ROOT"}}},
            {"$replaceRoot": {"newRoot": "$doc"}},
            {"$limit": limit}
        ],
        
                "paid_plus_adjustment_greater_than_charge": [
            {"$unwind": "$charges"},
            {
                "$match": {
                    "$expr": {
                        "$gt": [
                            {"$add": [
                                {"$ifNull": ["$charges.amountPaid", 0]},
                                {"$ifNull": ["$charges.adjustmentAmount", 0]}
                            ]},
                            "$charges.amount"
                        ]
                    }
                }
            },
            {"$group": {"_id": "$_id", "doc": {"$first": "$$ROOT"}}},
            {"$replaceRoot": {"newRoot": "$doc"}},
            {"$limit": limit}
        ],
        
        "zero_charges": [
            {"$unwind": "$charges"},
            {"$match": {"charges.amount": 0}},
            {"$group": {"_id": "$_id", "doc": {"$first": "$$ROOT"}}},
            {"$replaceRoot": {"newRoot": "$doc"}},
            {"$limit": limit}
        ],
        
        "negative_charges": [
            {"$unwind": "$charges"},
            {"$match": {"charges.amount": {"$lt": 0}}},
            {"$group": {"_id": "$_id", "doc": {"$first": "$$ROOT"}}},
            {"$replaceRoot": {"newRoot": "$doc"}},
            {"$limit": limit}
        ],
        
        "missing_unit_prices": [
            {"$unwind": "$charges"},
            {"$match": {
                "charges.unit": {"$exists": True, "$gt": 1},
                "$or": [
                    {"charges.unitPrice": {"$exists": False}},
                    {"charges.unitPrice": None},
                    {"charges.unitPrice": 0}
                ]
            }},
            {"$group": {"_id": "$_id", "doc": {"$first": "$$ROOT"}}},
            {"$replaceRoot": {"newRoot": "$doc"}},
            {"$limit": limit}
        ],
        
        "charge_remittance_details_missing": [
            {"$unwind": "$charges"},
            {"$match": {
                "charges.amountPaid": {"$gt": 0},
                "$or": [
                    {"charges.chargeRemittances": {"$size": 0}},
                    {"charges.chargeRemittances": {"$exists": False}}
                ]
            }},
            {"$group": {"_id": "$_id", "doc": {"$first": "$$ROOT"}}},
            {"$replaceRoot": {"newRoot": "$doc"}},
            {"$limit": limit}
        ],
        
        "charges_with_extreme_units": [
            {"$unwind": "$charges"},
            {"$match": {"charges.unit": {"$gt": 100}}},
            {"$group": {"_id": "$_id", "doc": {"$first": "$$ROOT"}}},
            {"$replaceRoot": {"newRoot": "$doc"}},
            {"$limit": limit}
        ],
        
        "charges_with_empty_description": [
            {"$unwind": "$charges"},
            {"$match": {
                "$or": [
                    {"charges.description": {"$exists": False}},
                    {"charges.description": ""}
                ]
            }},
            {"$group": {"_id": "$_id", "doc": {"$first": "$$ROOT"}}},
            {"$replaceRoot": {"newRoot": "$doc"}},
            {"$limit": limit}
        ],
        
        "unit_price_calculation_mismatch": [
            {"$unwind": "$charges"},
            {"$match": {
                "charges.unitPrice": {"$exists": True, "$ne": None, "$gt": 0},
                "charges.unit": {"$exists": True, "$ne": None, "$gt": 0}
            }},
            {"$addFields": {
                "charges.expectedAmount": {"$multiply": ["$charges.unit", "$charges.unitPrice"]},
                "charges.difference": {
                    "$abs": {"$subtract": [
                        "$charges.amount",
                        {"$multiply": ["$charges.unit", "$charges.unitPrice"]}
                    ]}
                }
            }},
            {"$match": {"charges.difference": {"$gt": 0.01}}},
            {"$group": {"_id": "$_id", "doc": {"$first": "$$ROOT"}}},
            {"$replaceRoot": {"newRoot": "$doc"}},
            {"$limit": limit}
        ],
        
        "negative_units": [
            {"$unwind": "$charges"},
            {"$match": {"charges.unit": {"$lt": 0}}},
            {"$group": {"_id": "$_id", "doc": {"$first": "$$ROOT"}}},
            {"$replaceRoot": {"newRoot": "$doc"}},
            {"$limit": limit}
        ],
        
        "zero_units_with_amount": [
            {"$unwind": "$charges"},
            {"$match": {
                "$or": [
                    {"charges.unit": 0},
                    {"charges.unit": {"$exists": False}},
                    {"charges.unit": None}
                ],
                "charges.amount": {"$gt": 0}
            }},
            {"$group": {"_id": "$_id", "doc": {"$first": "$$ROOT"}}},
            {"$replaceRoot": {"newRoot": "$doc"}},
            {"$limit": limit}
        ],
        
        "negative_unit_price": [
            {"$unwind": "$charges"},
            {"$match": {"charges.unitPrice": {"$exists": True, "$lt": 0}}},
            {"$group": {"_id": "$_id", "doc": {"$first": "$$ROOT"}}},
            {"$replaceRoot": {"newRoot": "$doc"}},
            {"$limit": limit}
        ],
        
        "future_service_dates": [
            {"$unwind": "$charges"},
            {"$match": {"charges.serviceDate": {"$gt": "2026-02-03"}}},
            {"$group": {"_id": "$_id", "doc": {"$first": "$$ROOT"}}},
            {"$replaceRoot": {"newRoot": "$doc"}},
            {"$limit": limit}
        ],
        
        "very_old_service_dates": [
            {"$unwind": "$charges"},
            {"$match": {"charges.serviceDate": {"$lt": "2021-02-03"}}},
            {"$group": {"_id": "$_id", "doc": {"$first": "$$ROOT"}}},
            {"$replaceRoot": {"newRoot": "$doc"}},
            {"$limit": limit}
        ],
        # ============= DIAGNOSIS MODULE =============
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
            {"$limit": limit}
        ],
        
        "missing_primary_diagnosis": [
            {"$match": {"$nor": [{"diagnoses.isPrimaryDiagnosis": True}]}},
            {"$limit": limit}
        ],
        
        "missing_description": [
            {"$unwind": "$diagnoses"},
            {"$match": {
                "$or": [
                    {"diagnoses.description": {"$exists": False}},
                    {"diagnoses.description": None},
                    {"diagnoses.description": ""}
                ]
            }},
            {"$group": {"_id": "$_id", "doc": {"$first": "$$ROOT"}}},
            {"$replaceRoot": {"newRoot": "$doc"}},
            {"$limit": limit}
        ],
        
        "missing_code": [
            {"$unwind": "$diagnoses"},
            {"$match": {
                "$or": [
                    {"diagnoses.code": {"$exists": False}},
                    {"diagnoses.code": None},
                    {"diagnoses.code": ""}
                ]
            }},
            {"$group": {"_id": "$_id", "doc": {"$first": "$$ROOT"}}},
            {"$replaceRoot": {"newRoot": "$doc"}},
            {"$limit": limit}
        ],
        
        "multiple_primary": [
            {"$unwind": "$diagnoses"},
            {"$match": {"diagnoses.isPrimaryDiagnosis": True}},
            {"$group": {"_id": "$_id", "doc": {"$first": "$$ROOT"}, "count": {"$sum": 1}}},
            {"$match": {"count": {"$gt": 1}}},
            {"$replaceRoot": {"newRoot": "$doc"}},
            {"$limit": limit}
        ],
        
        "missing_type": [
            {"$unwind": "$diagnoses"},
            {"$match": {
                "$or": [
                    {"diagnoses.type": {"$exists": False}},
                    {"diagnoses.type": None},
                    {"diagnoses.type": ""}
                ]
            }},
            {"$group": {"_id": "$_id", "doc": {"$first": "$$ROOT"}}},
            {"$replaceRoot": {"newRoot": "$doc"}},
            {"$limit": limit}
        ],
        
        "missing_status": [
            {"$unwind": "$diagnoses"},
            {"$match": {
                "$or": [
                    {"diagnoses.status": {"$exists": False}},
                    {"diagnoses.status": None},
                    {"diagnoses.status": ""}
                ]
            }},
            {"$group": {"_id": "$_id", "doc": {"$first": "$$ROOT"}}},
            {"$replaceRoot": {"newRoot": "$doc"}},
            {"$limit": limit}
        ],
        
        "order_mismatch": [
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
            {"$group": {"_id": "$_id", "doc": {"$first": "$$ROOT"}}},
            {"$replaceRoot": {"newRoot": "$doc"}},
            {"$limit": limit}
        ],
        
        "missing_order": [
            {"$unwind": "$diagnoses"},
            {"$match": {
                "$or": [
                    {"diagnoses.order": {"$exists": False}},
                    {"diagnoses.order": None},
                    {"diagnoses.order": ""}
                ]
            }},
            {"$group": {"_id": "$_id", "doc": {"$first": "$$ROOT"}}},
            {"$replaceRoot": {"newRoot": "$doc"}},
            {"$limit": limit}
        ],
        
        "duplicate_order": [
            {"$unwind": "$diagnoses"},
            {"$match": {
                "diagnoses.order": {"$exists": True, "$ne": None, "$ne": ""}
            }},
            {"$group": {
                "_id": {
                    "claim_id": "$_id",
                    "order": "$diagnoses.order"
                },
                "doc": {"$first": "$$ROOT"},
                "count": {"$sum": 1}
            }},
            {"$match": {"count": {"$gt": 1}}},
            {"$replaceRoot": {"newRoot": "$doc"}},
            {"$limit": limit}
        ],
        
        "missing_occurrence_date": [
            {"$unwind": "$diagnoses"},
            {"$match": {
                "$or": [
                    {"diagnoses.occurrenceDate": {"$exists": False}},
                    {"diagnoses.occurrenceDate": None},
                    {"diagnoses.occurrenceDate": ""}
                ]
            }},
            {"$group": {"_id": "$_id", "doc": {"$first": "$$ROOT"}}},
            {"$replaceRoot": {"newRoot": "$doc"}},
            {"$limit": limit}
        ],
        
        "missing_present_on_admission": [
            {"$unwind": "$diagnoses"},
            {"$match": {
                "$or": [
                    {"diagnoses.presentOnAdmission": {"$exists": False}},
                    {"diagnoses.presentOnAdmission": None},
                    {"diagnoses.presentOnAdmission": ""}
                ]
            }},
            {"$group": {"_id": "$_id", "doc": {"$first": "$$ROOT"}}},
            {"$replaceRoot": {"newRoot": "$doc"}},
            {"$limit": limit}
        ],
        
        "invalid_icd10_format": [
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
            {"$group": {"_id": "$_id", "doc": {"$first": "$$ROOT"}}},
            {"$replaceRoot": {"newRoot": "$doc"}},
            {"$limit": limit}
        ],
        
        "primary_diagnosis_not_order_1": [
            {"$unwind": "$diagnoses"},
            {"$match": {
                "diagnoses.isPrimaryDiagnosis": True,
                "$and": [
                    {"diagnoses.order": {"$ne": "1"}},
                    {"diagnoses.order": {"$ne": 1}}
                ]
            }},
            {"$group": {"_id": "$_id", "doc": {"$first": "$$ROOT"}}},
            {"$replaceRoot": {"newRoot": "$doc"}},
            {"$limit": limit}
        ],
        
        "invalid_diagnosis_status": [
            {"$unwind": "$diagnoses"},
            {"$match": {
                "diagnoses.status": {
                    "$exists": True, 
                    "$ne": None, 
                    "$ne": "",
                    "$nin": ["A", "W", "I", "R", "D"]
                }
            }},
            {"$group": {"_id": "$_id", "doc": {"$first": "$$ROOT"}}},
            {"$replaceRoot": {"newRoot": "$doc"}},
            {"$limit": limit}
        ],
        
        "invalid_diagnosis_type": [
            {"$unwind": "$diagnoses"},
            {"$match": {
                "diagnoses.type": {
                    "$exists": True,
                    "$ne": None,
                    "$ne": "",
                    "$nin": ["ABK", "ABF", "BK", "BF", "PRIMARY", "SECONDARY"]
                }
            }},
            {"$group": {"_id": "$_id", "doc": {"$first": "$$ROOT"}}},
            {"$replaceRoot": {"newRoot": "$doc"}},
            {"$limit": limit}
        ],
        
        
                # ============= CPT MODULE =============
        "invalid_cpt_format": [
            {"$unwind": "$charges"},
            {"$match": {
                "charges.cptHcpcs": {"$exists": True, "$ne": None, "$ne": ""}
            }},
            {"$addFields": {
                "charges.cptLength": {"$strLenCP": "$charges.cptHcpcs"}
            }},
            {"$match": {
                "charges.cptLength": {"$ne": 5}
            }},
            {"$group": {"_id": "$_id", "doc": {"$first": "$$ROOT"}}},
            {"$replaceRoot": {"newRoot": "$doc"}},
            {"$limit": limit}
        ],
        
        "invalid_modifier_codes": [
            {"$unwind": "$charges"},
            {"$match": {
                "charges.modifier": {
                    "$exists": True,
                    "$ne": None,
                    "$ne": "",
                    "$nin": [
                        "22", "25", "26", "50", "51", "52", "53", "59",
                        "76", "77", "78", "79", "80", "81", "82",
                        "AA", "GA", "GC", "GY", "GZ",
                        "JW", "JZ",
                        "LT", "RT", "LC", "LD",
                        "TC", "QW", "QX", "QY", "QZ"
                    ]
                }
            }},
            {"$group": {"_id": "$_id", "doc": {"$first": "$$ROOT"}}},
            {"$replaceRoot": {"newRoot": "$doc"}},
            {"$limit": limit}
        ],
        
        # ============= ADJUSTMENT MODULE =============
        "negative_adjustments": [
            {"$match": {"claimAdjAmount": {"$lt": 0}}},
            {"$limit": limit}
        ],
        
        "adjustment_greater_than_claim": [
            {"$match": {"$expr": {"$gt": ["$claimAdjAmount", "$claimAmount"]}}},
            {"$limit": limit}
        ],
                "adjustment_exceeds_50_percent": [
            {
                "$match": {
                    "claimAdjAmount": {"$gt": 0},
                    "$expr": {
                        "$gt": [
                            "$claimAdjAmount",
                            {"$multiply": ["$claimAmount", 0.5]}
                        ]
                    }
                }
            },
            {"$limit": limit}
        ],
        
        "missing_adjustment_details": [
            {
                "$match": {
                    "claimAdjAmount": {"$gt": 0},
                    "$or": [
                        {"claimAdjustments": {"$size": 0}},
                        {"claimAdjustments": {"$exists": False}}
                    ]
                }
            },
            {"$limit": limit}
        ],
        
        "charge_negative_adjustments": [
            {"$unwind": "$charges"},
            {"$match": {"charges.adjustmentAmount": {"$lt": 0}}},
            {"$group": {"_id": "$_id", "doc": {"$first": "$$ROOT"}}},
            {"$replaceRoot": {"newRoot": "$doc"}},
            {"$limit": limit}
        ],
        
        "charge_adjustment_exceeds_amount": [
            {"$unwind": "$charges"},
            {
                "$match": {
                    "$expr": {"$gt": ["$charges.adjustmentAmount", "$charges.amount"]}
                }
            },
            {"$group": {"_id": "$_id", "doc": {"$first": "$$ROOT"}}},
            {"$replaceRoot": {"newRoot": "$doc"}},
            {"$limit": limit}
        ],
        
        "charges_missing_adjustment_details": [
            {"$unwind": "$charges"},
            {
                "$match": {
                    "charges.adjustmentAmount": {"$gt": 0},
                    "$or": [
                        {"charges.chargeAdjustments": {"$size": 0}},
                        {"charges.chargeAdjustments": {"$exists": False}}
                    ]
                }
            },
            {"$group": {"_id": "$_id", "doc": {"$first": "$$ROOT"}}},
            {"$replaceRoot": {"newRoot": "$doc"}},
            {"$limit": limit}
        ],
        
        "chargeadjustment_sum_mismatch": [
            {"$unwind": "$charges"},
            {
                "$addFields": {
                    "sumOfAdjRecords": {
                        "$sum": "$charges.chargeAdjustments.adjAmount"
                    }
                }
            },
            {
                "$match": {
                    "$expr": {
                        "$ne": [
                            {"$round": ["$charges.adjustmentAmount", 2]},
                            {"$round": ["$sumOfAdjRecords", 2]}
                        ]
                    }
                }
            },
            {"$group": {"_id": "$_id", "doc": {"$first": "$$ROOT"}}},
            {"$replaceRoot": {"newRoot": "$doc"}},
            {"$limit": limit}
        ],
        
        "claim_adj_records_sum_mismatch": [
            {
                "$addFields": {
                    "sumClaimAdjRecords": {
                        "$sum": "$claimAdjustments.adjAmount"
                    }
                }
            },
            {
                "$match": {
                    "$expr": {
                        "$ne": [
                            {"$round": ["$claimAdjAmount", 2]},
                            {"$round": ["$sumClaimAdjRecords", 2]}
                        ]
                    }
                }
            },
            {"$limit": limit}
        ],
    }
    
    return pipelines.get(issue_name)