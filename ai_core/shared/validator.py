from loguru import logger
from datetime import datetime
from .models import MQuery, MResult
from .executor import PipelineExecutor
 
 
class Validation:
   
    def __init__(self, db):
        self.db = db
        self.executor = PipelineExecutor(db)
        self.claims = db["claims"]
   
    async def fetch_queries(self, qtype, priority):
        query_filter = {"is_active": True}
       
        if qtype:
            query_filter["qtype"] = qtype
       
        if priority:
            query_filter["priority"] = priority
       
        queries = await MQuery.find(query_filter).sort("priority").to_list()
       
        return queries
   
    async def count_total_claims(self, filters):
        match = {}
        if filters and 'payer' in filters:
            match = {'payerMCO': filters['payer']}
   
        total = await self.claims.count_documents(match)
       
        return total
   
    async def _save_result(self, query_name, count, percentage, filters):
        payer = None
        if filters and 'payer' in filters:
            payer = filters['payer']
       
        result = MResult(
            query_name=query_name,
            status="success",
            executed_at=datetime.now(),
            result={"count": count, "percentage": percentage},
            filters=payer,
            error=None
        )
       
        await result.insert()
       
        logger.debug(f"Saved result for {query_name}")
   
    async def run_validations(self, qtype=None, priority=None, filters=None):
        logger.info("Starting validations")
       
        queries = await self.fetch_queries(qtype, priority)
       
       
        total_claims = await self.count_total_claims(filters)
        logger.info(f"Total claims: {total_claims}")
       
        results = []
        for query in queries:
            logger.info(f"Running: {query.name}")
           
            exec_result = await self.executor.execute(query, filters)
           
            count = exec_result["data"]["count"]
           
            if total_claims > 0:
                percentage = round((count / total_claims * 100), 4)
            else:
                percentage = 0.0
           
            await self._save_result(query.name, count, percentage, filters)
           
            results.append({
                "name": query.name,
                "count": count,
                "percentage": percentage
            })
       
        logger.success(f"Completed {len(results)} validations")
       
        return results
 