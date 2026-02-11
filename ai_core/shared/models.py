from pydantic import BaseModel, Field
from beanie import Document
from typing import Optional
from enum import StrEnum, IntEnum
from datetime import datetime
 
 
class QueryType(StrEnum):
    charge = "charge"
    diagnosis = "diagnosis"
    adjustment = "adjustment"
    cpt= "cpt"
    claim = "claim"
   
 
 
class Priority(IntEnum):
    high = 1
    medium = 2
    low = 3
    neutral = 4
 
 
class Query(BaseModel):
    name: str
    qtype: QueryType
    desc: Optional[str] = None
    run_for_payer: bool = False
    pipeline: list[dict]
    is_active: bool = True
    priority: Priority
 
 
class MQuery(Document, Query):
    class Settings:
        name = "data.query"
 
 
class Result(BaseModel):
    query_name: str
    status: str
    executed_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    filters: Optional[str] = None
    result: dict | list[dict]
    error: Optional[str] = None
 
 
class MResult(Document, Result):
    class Settings:
        name = "data.validation"
 