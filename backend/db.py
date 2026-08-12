from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
import os

_client = None
_db = None


async def init_db():
    global _client, _db
    mongo_url = os.environ.get('MONGO_URL', 'mongodb://localhost:27017')
    db_name = os.environ.get('DB_NAME', 'nif_abstractor')
    _client = AsyncIOMotorClient(mongo_url)
    _db = _client[db_name]


async def close_db():
    global _client
    if _client:
        _client.close()


def get_db() -> AsyncIOMotorDatabase:
    return _db
