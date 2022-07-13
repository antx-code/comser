from pymongo import MongoClient
from loguru import logger

class MongoDB():
    @logger.catch(level='ERROR')
    def __init__(self, address='127.0.0.1', username='', password='', db='', col='', port=27017):
        """

        initial the mongodb and select the database and collection.

        :param db: The database that you want to operate.
        :param col: The collection that you want to opreate.
        """
        if not db or not col:
            raise ValueError('Please provide correct db or col!')
        client = MongoClient(f'mongodb://{username}:{password}@{address}:{port}', connect=False)
        try:
            self.database = client[db]
        except Exception as e:
            logger.error(e)
            logger.add(e)
            logger.error('Has error when operate database')
        try:
            self.collection = self.database[col]
        except Exception as e:
            logger.error(e)
            logger.add(e)
            logger.error('Has error when operate collection')


    @logger.catch(level='ERROR')
    def insert_one_data(self,data_dict):
        """

        onsert one data into the mongodb.

        :param data_dict:
        :return:
        """
        self.collection.insert_one(data_dict)


    @logger.catch(level='ERROR')
    def insert_many(self,data_dict_list):
        """

        insert many data into the mongodb.

        :param data_dict: A list of data dictionary that you want to insert.
        :return:
        """
        self.collection.insert_many(data_dict_list)
        # return True


    @logger.catch(level='ERROR')
    def query_data(self,data_dict={}, skip=0, limit=10, sort_key='_id'):
        """

        query the account info from mongodb.

        :param data_dict: The query parm dictionary, and it's default value is {},which means to find all value
        :return: user_dict: Return a cursor of the data object.
        """
        user_dict = self.collection.find(data_dict, {'_id':0}).skip(skip).limit(limit).sort(sort_key, -1)
        return user_dict

    @logger.catch(level='ERROR')
    def dep_data(self, parm):
        """

        Return duplicate data with the query parm.

        :param parm: The parameter that you want to as a basis to duplicate the data.
        :return: dep_data: Return a list of after duplicate data. It's only include the duplicate parameter datas.
        """
        dep_data = self.collection.distinct(parm)
        return dep_data


    @logger.catch(level='ERROR')
    def delete_one(self, data_dict):
        """

        delete one special data from mongodb.

        :param data_dict:
        :return:
        """
        self.collection.delete_one(data_dict)


    @logger.catch(level='ERROR')
    def update_one(self, query_dict, new_data_dict):
        """

        Update an exist data.

        :param query_dict: A dictionary of the segment that you want to update.
        :param new_data_dict: The new data dictionary that you want to replace the origin data.
        :return:
        """
        self.collection.update_one(query_dict, {'$set':new_data_dict}, upsert=True)
        return True


    @logger.catch(level='ERROR')
    def drop_collection(self):
        """

        Drop the current collection.

        :return: A bool vaule of the operate.
        """
        self.collection.drop()
        return True


    @logger.catch(level='ERROR')
    def rename_collection(self, new_name):
        """

        Rename the current collection.

        :param new_name: The new collection that you want to rename.
        :return: A bool value of the operate.
        """
        self.collection.rename(new_name)
        return True

