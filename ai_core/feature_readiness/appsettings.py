from typing import List
from pydantic import BaseModel, Field
from beanie import Document


class StatsSettings(BaseModel):
    payer_field: str = Field(
        default="payerMCO",
        description="Field name for payer in claims data"
    )


class Prompt(BaseModel):
    name: str = Field(description="Prompt identifier")
    prompt: str = Field(description="Prompt text")


class ChargeAnalysisPromptSettings(BaseModel):
    prompts: List[Prompt] = Field(
        default_factory=list,
        description="List of AI prompts"
    )


class AISuggestionSettings(BaseModel):
    charge_analysis: ChargeAnalysisPromptSettings = Field(
        default_factory=ChargeAnalysisPromptSettings,
        description="Charge analysis AI settings"
    )


class ValidationSettings(BaseModel):
    valid_cpt_modifiers: List[str] = Field(
        default=[
            "22", "24", "25", "26", "27", "32", "33", "47",
            "50", "51", "52", "53", "54", "55", "56", "57", "58", "59",
            "62", "66", "73", "74", "76", "77", "78", "79",
            "80", "81", "82", "90", "91", "95", "99",
            "AA", "GA", "GC", "GT", "GY", "GZ",
            "JW", "JZ",
            "LT", "RT", "LC", "LD",
            "TC", "QW", "QX", "QY", "QZ",
            "XE", "XP", "XS", "XU"
        ],
        description="List of valid CPT modifier codes"
    )


class ReadinessCheckSettings(BaseModel):
    claims_with_charges_threshold: int = Field(default=10)
    cpt_diversity_threshold: int = Field(default=5)
    claims_minimum_total: int = Field(default=100)
    claims_with_charges_percentage: float = Field(default=0.8, ge=0.0, le=1.0)
    claims_with_diagnoses_percentage: float = Field(default=0.7, ge=0.0, le=1.0)
    cpt_minimum_unique_codes: int = Field(default=5)
    stats_coverage_threshold: float = Field(default=0.5, ge=0.0, le=1.0)
    stats_minimum_record_count: int = Field(default=3)
    stats_minimum_cpts_per_payer: int = Field(default=3)
    stats_minimum_avg_record_count: float = Field(default=5.0)
    stats_maximum_staleness_days: int = Field(default=30)
    cdm_coverage_threshold: float = Field(default=0.3, ge=0.0, le=1.0)
    data_quality_threshold: float = Field(default=0.8, ge=0.0, le=1.0)


class DataQualitySettings(BaseModel):
    database_name: str = Field(default="rcm_test_db")
    collection_name: str = Field(default="claims")
    run_checks: bool = Field(default=False)
    run_data_quality: bool = Field(default=False)
    use_new_validation: bool = Field(default=True)


class AppSettings(BaseModel):
    stats_settings: StatsSettings = Field(default_factory=StatsSettings)
    readiness_settings: ReadinessCheckSettings = Field(default_factory=ReadinessCheckSettings)
    ai_suggestion_settings: AISuggestionSettings = Field(default_factory=AISuggestionSettings)
    validation_settings: ValidationSettings = Field(default_factory=ValidationSettings)
    data_quality_settings: DataQualitySettings = Field(default_factory=DataQualitySettings)


class MAppSettings(Document, AppSettings):
    class Settings: 
        name = "app_settings"


async def get_app_settings() -> MAppSettings:
    settings = await MAppSettings.find_one()
    
    if not settings:
        settings = MAppSettings()
        await settings.insert()
    
    return settings