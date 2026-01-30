from pydantic import BaseModel
from typing import Optional
from datetime import datetime
 
 
class DataCount(BaseModel):
    count: int
    percentage: float  
 
class Overview(BaseModel):
    total_claims: int
    unique_payers: int
    unique_cpt_codes: int
    
    
class DiagnosisValidation(BaseModel):
    missing_diagnosis: DataCount
    missing_primary_diagnosis: DataCount
    missing_description: DataCount
    missing_code: DataCount
    multiple_primary: DataCount
    missing_type: DataCount
    missing_status: DataCount
    order_mismatch: DataCount
    missing_order: DataCount
    duplicate_order: DataCount
    missing_occurrence_date: DataCount     
    missing_present_on_admission: DataCount
    
class Diagnosis(BaseModel):
    unique_icd_10_codes: int
    unique_icd_10_primary_codes: int
    Issues: DiagnosisValidation
    
    
class ChargeValidation(BaseModel):
    paid_greater_than_charge: DataCount
    paid_plus_adjustment_greater_than_charge: DataCount
    zero_charges: DataCount                  
    negative_charges: DataCount                  
    missing_unit_prices: DataCount
    charge_remittance_details_missing: DataCount
    charges_with_extreme_units: DataCount
    charges_with_empty_description: DataCount
 
class Charges(BaseModel):
    statistics: dict        
    ranges: list[dict]        
    high_value: dict            
    low_value: dict
    issues: ChargeValidation
    
class CPT(BaseModel):
    cpt_overview: dict            
    top_cpt_codes: dict              
    rare_cpt_codes: dict          
    modifier_usage: dict                 
    financial_analysis: dict 
    missing_cpt: dict 
    
    
class Payer(BaseModel):
    total_claims: int
    unique_payers_count: int
    all_payers: list[dict]
    payer_summary: dict
 
class ClaimIssues(BaseModel):
    denied_with_payment: DataCount
    denied_without_remittances: DataCount  
    denied_with_overpayment: DataCount  
    open_with_payment: DataCount
    paidamount_greater_than_claimamount: DataCount
    adjamount_greater_than_claimamount: DataCount
    claim_sum_mismatch: DataCount
    duplicate_claims: DataCount
    paid_plus_adjustment_exceeds_claim:DataCount
   
 
class Claims_info(BaseModel):
    total_claims: int
    open_count: int
    sent_to_payer_count: int
    closed_count: int
    denied_count: int
    pending_count: int
    pending_amount: float
    denial_rate: float
    denied_amount: float
    issues: ClaimIssues
 
 

class Adjustment(BaseModel):  
    total_claims: int
    claims_with_adjustments: int
    negative_adjustments: DataCount
    adjustment_greater_than_claim: DataCount
    adjustment_exceeds_50_percent: DataCount
    missing_adjustment_details: DataCount
    charge_negative_adjustments: DataCount
    charge_adjustment_exceeds_amount: DataCount
    charges_missing_adjustment_details: DataCount
 
class DataQualityResult(BaseModel):
    timestamp: datetime
    version: int = 1
    overview: Optional[Overview] = None
    diagnosis: Optional[Diagnosis] = None
    payer: Optional[Payer] = None
    charges: Optional[Charges] = None
    cpt: Optional[CPT] = None
    claims: Optional[Claims_info] = None
    adjustment: Optional[Adjustment] = None
 