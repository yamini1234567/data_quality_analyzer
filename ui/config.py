import os
from dotenv import load_dotenv

load_dotenv()

# MongoDB Configuration
MONGO_URI = os.getenv("MONGO_URI")
DATABASE_NAME = os.getenv("DATABASE_NAME")
RESULTS_COLLECTION = os.getenv("RESULTS_COLLECTION")
CLAIMS_COLLECTION = os.getenv("CLAIMS_COLLECTION")

# Flask Configuration
HOST = os.getenv("HOST")
PORT = int(os.getenv("PORT"))