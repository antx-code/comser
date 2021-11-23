from loguru import logger
import asyncio
import uvloop
from storage.redis_service import RedisService
from storage.async_mongodb import AsyncMongo
from storage.mongodb import MongoDB
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
uvloop.install()
from __init__ import config

DB_CONF = config['MONGO']
REDIS_CONF = config['REDIS']

@logger.catch(level='ERROR')
async def db_connection(db, collection, port=DB_CONF['PORT']):
    mongodb = AsyncMongo(db, collection, port)
    return mongodb

@logger.catch(level='ERROR')
def redis_connection(port=REDIS_CONF['PORT'], redis_db=0):
    redis_service = RedisService(port=port, redis_db=redis_db)
    return redis_service

@logger.catch(level='ERROR')
def sync_db_connection(db, collection, port=DB_CONF['PORT']):
    mongodb = MongoDB(db, collection, port)
    return mongodb

