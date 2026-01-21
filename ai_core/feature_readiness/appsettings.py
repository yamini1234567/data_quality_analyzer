"""
Application Settings for Data Quality Analyzer

Defines the structure of app_settings document stored in MongoDB. 
Following Satya's pattern for centralized configuration.
"""

from typing import List
from pydantic import BaseModel, Field
from beanie import Document

# STATS SETTINGS 

class StatsSettings(BaseModel):
    """Data field mappings for MongoDB queries"""
    
    payer_field: str = Field(
        default="payerMCO",
        description="Field name for payer in claims data"
    )
    
# AI SUGGESTION SETTINGS 


class Prompt(BaseModel):
    """AI prompt definition"""
    
    name: str = Field(description="Prompt identifier")
    prompt: str = Field(description="Prompt text")


class ChargeAnalysisPromptSettings(BaseModel):
    """Prompts for charge analysis AI"""
    
    prompts:  List[Prompt] = Field(
        default_factory=list,
        description="List of AI prompts"
    )


class AISuggestionSettings(BaseModel):
    """Settings for AI features"""
    
    charge_analysis: ChargeAnalysisPromptSettings = Field(
        default_factory=ChargeAnalysisPromptSettings,
        description="Charge analysis AI settings"
    )

# READINESS CHECK SETTINGS 

class ReadinessCheckSettings(BaseModel):
    """Thresholds for feature readiness checks"""
    
    claims_with_charges_threshold:  int = Field(
        default=10,
        description="Minimum claims with charges (Check 1)"
    )
    
    cpt_diversity_threshold: int = Field(
        default=5,
        description="Minimum unique CPT codes (Check 2)"
    )
    
    claims_minimum_total:  int = Field(
        default=100,
        description="Minimum total number of claims required for analysis"
    )
    
    claims_with_charges_percentage: float = Field(
        default=0.8,
        ge=0.0,
        le=1.0,
        description="Minimum % of claims that must have charges with CPT codes (0. 0-1.0)"
    )
    
    claims_with_diagnoses_percentage: float = Field(
        default=0.7,
        ge=0.0,
        le=1.0,
        description="Minimum % of claims that must have diagnoses with codes (0.0-1.0)"
    )
    
    cpt_minimum_unique_codes: int = Field(
        default=5,
        description="Minimum number of unique CPT codes required for diversity"
    )
    
    
    stats_coverage_threshold: float = Field(
        default=0.5,
        ge=0.0,
        le=1.0,
        description="Minimum % of CPT codes with sufficient stats (0.0-1.0)"
    )
    
    stats_minimum_record_count: int = Field(
        default=3,
        description="Minimum record_count for a CPT/payer combo to be considered valid"
    )
    
    stats_minimum_cpts_per_payer: int = Field(
        default=3,
        description="Minimum number of CPT codes per payer with sufficient stats"
    )
    
    stats_minimum_avg_record_count: float = Field(
        default=5.0,
        description="Minimum average record_count across all stats documents"
    )
    
    stats_maximum_staleness_days: int = Field(
        default=30,
        description="Maximum age (in days) of most recent stats update"
    )
      
    cdm_coverage_threshold: float = Field(
        default=0.3,
        ge=0.0,
        le=1.0,
        description="Minimum CDM coverage percentage (Check 4)"
    )
    
    data_quality_threshold: float = Field(
        default=0.8,
        ge=0.0,
        le=1.0,
        description="Minimum data quality percentage (Check 5)"
    )

#  APP SETTINGS


class AppSettings(BaseModel):
    """Application-wide settings"""
    
    stats_settings: StatsSettings = Field(
        default_factory=StatsSettings,
        description="Data field mappings"
    )
    
    readiness_settings: ReadinessCheckSettings = Field(
        default_factory=ReadinessCheckSettings,
        description="Readiness check thresholds"
    )
    
    ai_suggestion_settings:  AISuggestionSettings = Field(
        default_factory=AISuggestionSettings,
        description="AI feature settings"
    )

# BEANIE DOCUMENT 

class MAppSettings(Document, AppSettings):
    """MongoDB document for application settings"""
    
    class Settings: 
        name = "app_settings"
    
    class Config:
        arbitrary_types_allowed = True