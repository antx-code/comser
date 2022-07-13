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
    def new_insert_content(self, redis_key, new_content):
        """

        将数据插入redis，如果数据不存在，返回1,如果数据已经存在，则不插入并返回0

        :param redis_key: The redis key that you want to operate.
        :param new_cobntent: The data that you want to insert and judge. Must be same as the exists data.
        :return: A bool value of the operate.
        """
        if self.redis_client.sadd(redis_key, new_content) == 1:
            return True
        return False


    @logger.catch(level='ERROR')
    def read_redis(self, redis_key):
        """

        Read the redis keys' data.

        :param redis_key: The redis key that you want to query.
        :return: A list of the query data.
        """
        data_list = []
        data_set = self.redis_client.smembers(redis_key)
        for each_data in data_set:
            data_list.append(each_data.decode())
        return data_list


    @logger.catch(level='ERROR')
    def del_redis_element(self, tar_redis_key, tar_element):
        """

        Delete a redis element data of the redis key.

        :param tar_redis_key: The redis key that you want to delete the element data.
        :param tar_element: The data that you want to delete.
        :return: A bool value of the operate.
        """
        self.redis_client.srem(tar_redis_key, tar_element)
        return True


    @logger.catch(level='ERROR')
    def set_expire_key(self, key_name, key_vaule, expire_secs=None):
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
    def get_key_expire_content(self, key_name):
        """

        Get the expire redis key's content.

        :param key_name: The redis key that you want to query.
        :return: A string of the expire content.
        """
        result = self.redis_client.get(key_name)
        return result.decode()


    @logger.catch(level='ERROR')
    def get_diff_set(self, tar_redis_key, small_redis_key, big_redis_key):
        """

        Get two redis keys' content difference set.

        :param tar_redis_key: New redis key that save the difference set data.
        :param small_redis_key: The redis key that own a relative small content set.
        :param big_redis_key: The redis key that own a relative big content set.
        :return: A bool value of the operate.
        """
        self.redis_client.sdiffstore(tar_redis_key, big_redis_key, small_redis_key)
        return True


    @logger.catch(level='ERROR')
    def set_dep_key(self, key_name, key_value, expire_secs=None):
        """

        Set a duplicate redis key, which include content and expire time.

        :param key_name: The duplicate redis key that you want to be saved.
        :param key_value: The duplicate content of the redis key
        :param expire_secs: The expire time of the redis key and value. It's default value is None
        :return: A bool value of the operate.
        """
        self.redis_client.set(key_name, key_value, ex=expire_secs)
        return True

    @logger.catch(level='ERROR')
    def rename_redis_key(self, old_redis_key, new_redis_key):
        self.redis_client.rename(old_redis_key, new_redis_key)
        return True

    @logger.catch(level='ERROR')
    def save_dict_data(self, redis_key, tar_data: dict):
        """

        Save the dict data into redis.

        :param redis_key: The key of the dict data.
        :param tar_data: The target data, and it must be a dict value.
        :return: The result of the operate.
        """
        self.redis_client.hmset(redis_key, tar_data)
        return True

    @logger.catch(level='ERROR')
    def get_all_keys(self):
        keys = []
        all_keys = self.redis_client.keys()
        for each_key in all_keys:
            keys.append(each_key.decode())
        return keys

    @logger.catch(level='ERROR')
    def drop_redis_db(self):
        """

        Drop and clear all keys in current activate redis storage.

        :return:
        """
        self.redis_client.flushdb()
        return True

    @logger.catch(level='ERROR')
    def del_redis_key(self, redis_key):
        """

        Delete a redis key in the select storage.

        :param redis_key:
        :return:
        """
        self.redis_client.delete(redis_key)
        return True

    @logger.catch('ERROR')
    def sscan_redis(self, redis_key, match=None, count=1):
        """

        Use SSCAN method to get redis data, which can set query data's count.

        :param redis_key: The target key that you want to query.
        :param match: The parameter that you want to match.
        :param count: The amount of data that you want to return.
        :return:
        """
        result_list = []
        result = self.redis_client.sscan(name=redis_key,match=match,count=count)[1]
        for each_result in result:
            result_list.append(each_result.decode())
        return result_list

    @logger.catch('ERROR')
    def hset_redis(self, redis_key, content_key, content_value):
        """

        Insert hash data into redis, which have key and value like python's dic.

        :param redis_key: The redis key that you want to set.
        :param content_key: The key of the hash data.
        :param content_value: The value of the hash data.
        :return: The result of the operate.
        """
        result = self.redis_client.hset(name=redis_key, key=content_key, value=content_value)
        if result:
            return True
        return False

    @logger.catch('ERROR')
    def hget_redis(self, redis_key, content_key):
        """

        The get method of the hash data in redis.

        :param redis_key: The redis key that you want to get.
        :param content_key: The key of the hash data.
        :return: A origin data type of the hash data value.
        """
        resp = self.redis_client.hget(name=redis_key, key=content_key).decode()
        result = json.loads(resp)
        return result