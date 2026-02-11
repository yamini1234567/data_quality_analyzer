import asyncio
from loguru import logger
from datetime import datetime
from .models import MQuery, MResult
from .executor import PipelineExecutor
 
 
class Validation:
   
    def __init__(self, db):
        self.db = db
        self.executor = PipelineExecutor(db)
        self.claims = db["claims"]
        self.max_concurrent = 10
   
    async def fetch_queries(self, qtype, priority):
       
        query_filter = {"is_active": True}
       
        if qtype:
            query_filter["qtype"] = qtype
       
        if priority:
            query_filter["priority"] = priority
       
        queries = await MQuery.find(query_filter).sort("priority").to_list()
        return queries
   
    async def count_total_claims(self):
        total = await self.claims.count_documents({})
        return total
   
    def get_count(self, results):
        if not results:
            return 0
        first = results[0]
        return first.get("total") or first.get("count") or len(results)
   
    async def save_result(self, query_name, count, percentage):
        result = MResult(
            query_name=query_name,
            status="success",
            executed_at=datetime.now(),
            result={"count": count, "percentage": percentage},
            filters=None,
            error=None
        )
        await result.insert()
        logger.debug(f"Saved result for {query_name}")
   
    async def run_single_validation(self, query, total_claims, semaphore):
        async with semaphore:
            logger.info(f"Running: {query.name}")
           
            results = await self.executor.execute(query)
            count = self.get_count(results)
            percentage = round((count / total_claims * 100), 4) if total_claims > 0 else 0.0
            await self.save_result(query.name, count, percentage)
           
            return {
                "name": query.name,
                "count": count,
                "percentage": percentage
            }
   
    async def run_validations(self, qtype=None, priority=None):
        logger.info("Starting validations")
        queries = await self.fetch_queries(qtype, priority)
        total_claims = await self.count_total_claims()
        semaphore = asyncio.Semaphore(self.max_concurrent)
   
        tasks = [
            self.run_single_validation(query, total_claims, semaphore)
            for query in queries
        ]
       
        results = await asyncio.gather(*tasks)
        return results
 