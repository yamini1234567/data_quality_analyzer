from loguru import logger
from .models import Query
 
 
class PipelineExecutor:
   
    def __init__(self, db):
        self.db = db
        self.claims = db["claims"]
   
    def get_pipeline(self, query: Query, filters=None):
        pipeline = query.pipeline
       
        if query.run_for_payer:
 
            pipeline = [
                {
                    '$group': {
                        '_id': '$payerMCO',
                        'count': {'$sum': 1}
                    }
                }
            ] + pipeline
       
        return pipeline
   
    async def execute(self, query: Query, filters=None):
        logger.info(f"Executing: {query.name}")
       
        pipeline = self.get_pipeline(query, filters)
        results = await self.claims.aggregate(pipeline).to_list(None)
       
        return results
 