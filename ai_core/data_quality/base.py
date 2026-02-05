from ai_core.data_quality.models import DataCount
from abc import ABC, abstractmethod 

class BaseAnalyzer(ABC):
    
    def __init__(self, db, filters=None):
        self.db = db
        self.claims = db["claims"]
        self.total_claims = 0
    
        if filters and 'payer' in filters:
            self.filter = {'payerMCO': filters['payer']}
        else:
            self.filter = {}
        
    async def get_total_claims(self):
        if self.total_claims == 0:
            self.total_claims = await self.claims.count_documents(self.filter)
        return self.total_claims
    
    async def run_pipeline(self, pipeline, base_count=None):
        if self.filter:
            pipeline = [{'$match': self.filter}] + pipeline
        
        result = await self.claims.aggregate(pipeline).to_list(None)
        count = result[0]["total"] if result else 0 
        base = base_count if base_count else self.total_claims
        percentage = round((count / base * 100), 4) if base > 0 else 0.0
        return DataCount(count=count, percentage=percentage)
    
    def facet_to_datacount(self, facet_result, facet_name, base_count=None):
        data = facet_result.get(facet_name, [])
        count = data[0]["total"] if data else 0
        base = base_count if base_count else self.total_claims
        percentage = round((count / base * 100), 4) if base > 0 else 0.0
        return DataCount(count=count, percentage=percentage)
    
    @abstractmethod
    async def run_all(self):
        pass
    
    async def aggregate(self, pipeline):
        if self.filter:
            pipeline = [{'$match': self.filter}] + pipeline
        
        return await self.claims.aggregate(pipeline).to_list(None)
