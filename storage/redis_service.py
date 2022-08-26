from redis import Redis
from loguru import logger
import json


class RedisService():
    @logger.catch(level='ERROR')
    def __init__(self, address='127.0.0.1', password='', port=6379, db=0):
        """

        The init of the redis service.

        :param mongodb: The mongodb database that you want to operate in the future.
        :param mongocol: The mongodb collection that you want to operate in the future.
        :param redis_db: The redis database that you want to operate in the future, and it's defualt value is 0. You can
        chose a value in the scope of 0-15.
        """
        self.redis_client = Redis(host=address, password=password, port=port, db=db)

    @logger.catch(level='ERROR')
    def add2set(self, key_name, content):
        """

        将数据插入redis，如果数据不存在，返回1,如果数据已经存在，则不插入并返回0

        :param key_name: The redis key that you want to operate.
        :param content: The data that you want to insert and judge. Must be same as the exists data.
        :return: A bool value of the operate.
        """
        if self.redis_client.sadd(key_name, content) == 1:
            return True
        return False

    @logger.catch(level='ERROR')
    def get_set(self, key_name):
        """

        Read the redis keys' data.

        :param key_name: The redis key that you want to query.
        :return: A list of the query data.
        """
        data_list = []
        data_set = self.redis_client.smembers(key_name)
        for each_data in data_set:
            data_list.append(each_data.decode())
        return data_list

    @logger.catch(level='ERROR')
    def remove_set_element(self, key_name, element):
        """

        Delete a redis element data of the redis key.

        :param key_name: The redis key that you want to delete the element data.
        :param element: The data that you want to delete.
        :return: A bool value of the operate.
        """
        self.redis_client.srem(key_name, element)
        return True

    @logger.catch(level='ERROR')
    def set_exp_key(self, key_name, key_vaule, expire_secs=None):
        """

        Set a expire key, include it's name、value and expire time.

        :param key_name: The redis key that you want to set expire time.
        :param key_vaule: The content that you want to set.
        :param expire_secs: The expire of the key and value, and it's default value is -1, which means it never expire.
        The parameter unit is second.
        :return: A bool value of operate.
        """
        self.redis_client.set(key_name, key_vaule, ex=expire_secs)
        return True

    @logger.catch(level='ERROR')
    def get_exp_key(self, key_name):
        """

        Get the expire redis key's content.

        :param key_name: The redis key that you want to query.
        :return: A string of the expire content.
        """
        result = self.redis_client.get(key_name)
        return result.decode()

    @logger.catch(level='ERROR')
    def diff_set(self, new_key_name, small_key_name, big_key_name):
        """

        Get two redis keys' content difference set.

        :param new_key_name: New redis key that save the difference set data.
        :param small_key_name: The redis key that own a relative small content set.
        :param big_key_name: The redis key that own a relative big content set.
        :return: A bool value of the operate.
        """
        self.redis_client.sdiffstore(new_key_name, big_key_name, small_key_name)
        return True

    @logger.catch(level='ERROR')
    def rename_redis_key(self, old_key_name, new_key_name):
        self.redis_client.rename(old_key_name, new_key_name)
        return True

    @logger.catch(level='ERROR')
    def get_all_keys(self):
        keys = []
        all_keys = self.redis_client.keys()
        for each_key in all_keys:
            keys.append(each_key.decode())
        return keys

    @logger.catch(level='ERROR')
    def drop_db(self):
        """

        Drop and clear all keys in current activate redis storage.

        :return:
        """
        self.redis_client.flushdb()
        return True

    @logger.catch(level='ERROR')
    def del_key(self, key_name):
        """

        Delete a redis key in the select storage.

        :param key_name:
        :return:
        """
        self.redis_client.delete(key_name)
        return True

    @logger.catch(level='ERROR')
    def s_scan(self, key_name, match=None, count=1):
        """

        Use SSCAN method to get redis data, which can set query data's count.

        :param key_name: The target key that you want to query.
        :param match: The parameter that you want to match.
        :param count: The amount of data that you want to return.
        :return:
        """
        result_list = []
        result = self.redis_client.sscan(name=key_name, match=match, count=count)[1]
        for each_result in result:
            result_list.append(each_result.decode())
        return result_list

    @logger.catch(level='ERROR')
    def add2hashmap(self, key_name, content_key, content_value):
        """

        Insert hash data into redis, which have key and value like python's dic.

        :param key_name: The redis key that you want to set.
        :param content_key: The key of the hash data.
        :param content_value: The value of the hash data.
        :return: The result of the operate.
        """
        result = self.redis_client.hset(name=key_name, key=content_key, value=content_value)
        if result:
            return True
        return False

    @logger.catch(level='ERROR')
    def get_hashmap(self, key_name, content_key):
        """

        The get method of the hash data in redis.

        :param key_name: The redis key that you want to get.
        :param content_key: The key of the hash data.
        :return: A origin data type of the hash data value.
        """
        result = self.redis_client.hget(name=key_name, key=content_key).decode()
        result = json.loads(result)
        return result

    @logger.catch(level='ERROR')
    def get_key(self, key_name):
        result = self.redis_client.get(key_name)
        if not result:
            return ''
        return result.decode('utf-8')

    @logger.catch(level='ERROR')
    def incr(self, key_name, amount=1):
        result = self.redis_client.incr(key_name, amount)
        return result

    @logger.catch(level='ERROR')
    def incr_exp(self, key_name, amount=1, exp=60):
        self.redis_client.incr(key_name, amount)
        result_exp = self.redis_client.expire(key_name, exp)
        return result_exp

    @logger.catch(level='ERROR')
    def add2list(self, key_name, params):
        if isinstance(params) is not list:
            params = [params]
        for param in params:
            self.redis_client.lpush(key_name, param)
        return True

    @logger.catch(level='ERROR')
    def get_list(self, key_name, start: int = 1, end: int = -1):
        params = []
        results = self.redis_client.lrange(key_name, start=start, end=end)
        for result in results:
            params.append(result.decode('utf-8'))
        list_len = self.redis_client.llen(key_name)
        return params, list_len
