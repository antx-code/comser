from loguru import logger
import asyncio
import uvloop
from storage.redis_service import RedisService
from storage.async_mongo import AsyncMongo
from storage.mongo import MongoDB
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
uvloop.install()


class Connect():
    def __init__(self):
        self.mongo_address = '127.0.0.1'
        self.mongo_port = 27017
        self.mongo_username = ''
        self.mongo_password = ''
        self.redis_address = '127.0.0.1'
        self.redis_port = 6379
        self.redis_password = ''
        self.names = ['mongo_address', 'mongo_port', 'mongo_username', 'mongo_password', 'redis_address', 'redis_port', 'redis_password']

    @logger.catch(level='ERROR')
    def set_config(self, **kwargs):
        for k, v in kwargs.items():
            if k not in self.names:
                raise ValueError('Please set correct storage configuration!')
            if k == 'mongo_address':
                self.mongo_address = v
            elif k == 'mongo_port':
                self.mongo_port = v
            elif k == 'mongo_username':
                self.mongo_username = v
            elif k == 'mongo_password':
                self.mongo_password = v
            elif k == 'redis_address':
                self.redis_address = v
            elif k == 'redis_port':
                self.redis_port = v
            elif k == 'redis_password':
                self.redis_password = v
            else:
                pass

    @logger.catch(level='ERROR')
    async def async_mongo_connection(self, db, collection):
        mongodb = AsyncMongo(address=self.mongo_address, username=self.mongo_username, password=self.mongo_password, db=db, col=collection, port=self.mongo_port)
        return mongodb

    @logger.catch(level='ERROR')
    def redis_connection(self, redis_db=0):
        redis_service = RedisService(address=self.redis_address, password=self.redis_password, port=self.redis_port, db=redis_db)
        return redis_service

    @logger.catch(level='ERROR')
    def mongo_connection(self, db, collection):
        mongodb = MongoDB(address=self.mongo_address, username=self.mongo_username, password=self.mongo_password, db=db, col=collection, port=self.mongo_port)
        return mongodb
