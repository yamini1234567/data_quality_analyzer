"""
Database Connection and Initialization
"""

import os
from motor.motor_asyncio import AsyncIOMotorClient
from beanie import init_beanie
from dotenv import load_dotenv
from loguru import logger

load_dotenv()


def get_mongo_uri() -> str:
    return os.getenv("MONGODB_URI", "mongodb://localhost:27017")


def get_database_name() -> str:
    return os.getenv("MONGODB_DB_NAME", "Data_Quality_Analyzer")


def get_mongo_client() -> AsyncIOMotorClient:
    uri = get_mongo_uri()
    logger.info(f"Creating MongoDB client: {uri}")
    return AsyncIOMotorClient(uri)


def get_database(client: AsyncIOMotorClient, db_name: str = None):
    if db_name is None:
        db_name = get_database_name()
    logger.info(f"Using database: {db_name}")
    return client[db_name]


async def test_connection(client: AsyncIOMotorClient) -> bool:
    try:
        await client.admin.command('ping')
        logger.info("MongoDB connection successful")
        return True
    except Exception as e:
        logger.error(f"MongoDB connection failed: {e}")
        return False


async def init_db():
    logger.info("=" * 70)
    logger.info("DATABASE INITIALIZATION")
    logger.info("=" * 70)
    
    logger.info("Step 1: Creating MongoDB client...")
    client = get_mongo_client()
    
    logger.info("Step 2: Testing connection...")
    if not await test_connection(client):
        raise Exception("Failed to connect to MongoDB")
    
    logger.info("Step 3: Getting database...")
    db = get_database(client, "rcm_test_db")
    
    logger.info("Step 4: Initializing Beanie models")
    from ai_core.feature_readiness.appsettings import MAppSettings
    await init_beanie(database=db, document_models=[MAppSettings])
    
    logger.info(f"Database initialized: {db.name}")
    logger.info(f"Initialized 1 model(s):")
    logger.info("  - MAppSettings")
    logger.info("=" * 70 + "\n")
    
    return client, db


async def close_db(client: AsyncIOMotorClient):
    if client:
        logger.info("Closing database connection...")
        client.close()
        logger.info("Database connection closed")
