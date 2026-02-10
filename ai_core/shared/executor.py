from loguru import logger
from .models import Query
 
 
class PipelineExecutor:
   
    def __init__(self, db):
        self.db = db
        self.claims = db["claims"]
   
    async def execute(self, query: Query, filters=None):
        logger.info(f"Executing: {query.name}")
   
        pipeline = query.pipeline
        if filters and 'payer' in filters:
            pipeline = [{'$match': {'payerMCO': filters['payer']}}] + pipeline
 
        results = await self.claims.aggregate(pipeline).to_list(None)
       
        if not results:
            count = 0
        elif "total" in results[0]:
            count = results[0]["total"]
        elif "count" in results[0]:
            count = results[0]["count"]
        else:
            count = len(results)
       
        logger.success(f"{query.name}: count={count}")
       
        return {
            "success": True,
            "data": {"count": count},
            "query_name": query.name,
            "error": None
        }
 