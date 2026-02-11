import os
from motor.motor_asyncio import AsyncIOMotorClient
from beanie import init_beanie
from dotenv import load_dotenv
from loguru import logger
from ai_core.feature_readiness.appsettings import MAppSettings
from ai_core.data_quality.models import DataQualityResult
from ai_core.shared.models import MQuery, MResult

load_dotenv()


def get_mongo_uri() -> str:
    return os.getenv("MONGODB_URI", "mongodb://localhost:27017")


def get_database_name() -> str:
    return os.getenv("MONGODB_DB_NAME", "rcm_test_db")


def get_mongo_client() -> AsyncIOMotorClient:
    uri = get_mongo_uri()
    return AsyncIOMotorClient(uri)


def get_database(client: AsyncIOMotorClient, db_name: str = None):
    if db_name is None:
        db_name = get_database_name()
    return client[db_name]


async def test_connection(client: AsyncIOMotorClient) -> bool:
    try:
        await client.admin.command('ping')
        logger.info("MongoDB connected")
        return True
    except Exception as e:
        logger.error(f"MongoDB connection failed: {e}")
        return False


async def init_db():
    client = get_mongo_client()
    
    if not await test_connection(client):
        raise Exception("Failed to connect to MongoDB")
    
    db = get_database(client, "rcm_test_db")
    
    await init_beanie(
        database=db, 
        document_models=[
            MAppSettings,
            DataQualityResult,
            MQuery,
            MResult
        ]
    )
    
    logger.info("Database initialized")
    
    return client, db


async def close_db(client: AsyncIOMotorClient):
    
    if client:
        client.close()
        
        