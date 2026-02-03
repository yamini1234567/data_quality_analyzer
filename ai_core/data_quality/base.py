from .models import DataCount
 
class BaseAnalyzer:
   
    def __init__(self, db):
        self.db = db
        self.claims = db["claims"]
        self.total_claims = 0
   
    async def get_total_claims(self):
        if self.total_claims == 0:
            self.total_claims = await self.claims.count_documents({})
        return self.total_claims
   
    async def run_pipeline(self, pipeline, base_count=None):
        result = await self.claims.aggregate(pipeline).to_list(None)
        count = result[0]["total"] if result else 0
        base = base_count if base_count else self.total_claims
        percentage = round((count / base * 100), 4) if base > 0 else 0.0
       
        return DataCount(count=count, percentage=percentage)
   
    async def count_documents(self, filter_query):
        return await self.claims.count_documents(filter_query)
   
    async def aggregate(self, pipeline):
        return await self.claims.aggregate(pipeline).to_list(None)
   
    async def find(self, filter_query, limit=None):
        cursor = self.claims.find(filter_query)
        if limit:
            cursor = cursor.limit(limit)
        return await cursor.to_list(length=None)
   
    async def distinct(self, field, filter_query=None):
        if filter_query:
            return await self.claims.distinct(field, filter_query)
        return await self.claims.distinct(field)
 