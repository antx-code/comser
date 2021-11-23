import os
import requests
from loguru import logger
from googletrans import Translator
import json
import re
import random
from time import sleep
from random import randint as rant
from bson import ObjectId
import hashlib
import geocoder
import time
from datetime import datetime
import datetime as dt
from collections import Counter
from misc.ua_pools import ua

# 合并两个不同的字典
@logger.catch(level='ERROR')
def merge_df_dict(base_dict, dict2):
    """

    Merge two different dictionary into one. The detail_dict will be added into the base_dict, and if there be same key,
    will be coverd by the detail_dict's element. The method is also union operate.

    :param base_dict: The base dict, which will be added new element from dict2.
    :param dict2: All it's element will add into base_dict.
    :return: new_dict: After the merge operate, will have a new dict.
    """
    new_dict = {**base_dict, **dict2}
    return new_dict

# 返回目录下所有文件
@logger.catch(level='ERROR')
def show_files(files_path):
    """

    Get all the file name in the directory, It is not include the all directory and it's child item.

    :param files_path: The absolute directory path, which you want to get file name.
    :return: file_name: A list of all file names in the given directory.
    """
    file_names = []
    for root, dirs, files in os.walk(files_path, topdown=False):
        for file in files:
            file_names.append(os.path.join(root, file).replace("\\", "/"))
    return file_names

# 重命名文件
@logger.catch(level='ERROR')
def rename_file(source, tar_format, want_name=None):
    """

    Rename all the files into the target format. The source and want_name params are must absolute path of name.

    :param source: The parent directory name of the files.
    :param tar_format: The output format that you want to rename and turn into.
    :param want_name: The name that you want name to rename, and the default value is None.
    :return:
    """
    if type(source) is list:
        for each in source:
            if want_name is not None:
                os.renames(each, want_name + f'.{tar_format}')
            else:
                new_each = each.replace(".", "_")
                os.renames(each, new_each + f'.{tar_format}')
    elif os.path.isdir(source):
        file_names = show_files(source)
        for each in file_names:
            if not each.endswith(('.py', '.js', '.md', '.txt', '.csv')):
                if want_name is not None:
                    os.renames(each, want_name + f'.{tar_format}')
                else:
                    if "." in each:
                        new_each = each.replace(".", "_")
                    os.renames(each, new_each + f'.{tar_format}')
    else:
        if want_name is not None:
            os.renames(source, want_name + f'.{tar_format}')
        else:
            new_source = source.replace(".", "_")
            os.renames(source, new_source + f'.{tar_format}')

# 翻译
@logger.catch(level='ERROR')
def translator(source, dest='zh-CN'):
    """

    The translator is rely on a third-party lib which named googleteans. It can translate the source text into all
    you like language. The function can receive a string or a list.

    :param source: The source parm is the origin text that you want to translate. It can be a string or a list.
    :param dest: The dest is the language that you want to translate to. It's default value is Chinese.
    :return:
    """
    ts = Translator(service_urls=["translate.google.cn"])
    if type(source) is list:
        trans_out = []
        for iterm in source:
            trans_text = ts.translate(text=iterm,dest=dest).text
            trans_out.append(trans_text)
    else:
        trans_out = ts.translate(text=source,dest=dest).text
    return trans_out

# 保存为json文件
@logger.catch(level='ERROR')
def save2json(source, filename):
    """

    Save the source data to the json file.

    :param source: The source data before json format.
    :param filename: The name that you want to save.
    :return:
    """
    with open(f'{filename}.json', 'w') as f:
        if type(source) is list:
            for each in source:
                each_data = json.dumps(each, ensure_ascii=False)
                f.write(each_data)
                f.write(',')
        else:
            json_data = json.dumps(source, ensure_ascii=False)
            f.write(json_data)

# 返回两个列表中较大列表有而较小列表没有的元素
@logger.catch(level='ERROR')
def diff_list(small_list, big_list):
    """

    Get the difference set of the two list.

    :param small_list: The small data list.
    :param big_list: The bigger data list.
    :return: diff_list: The difference set list of the two list.
    """
    # big_list有而small_list没有的元素
    diff_list = list(set(big_list).difference(set(small_list)))
    return diff_list

# 两个或多个列表取并集
@logger.catch(level='ERROR')
def union_list(cmp_lists):
    """

    Get the two or multiple lists' union. Support one empty list.

    :param cmp_lists: A list of will do union calculate lists. It must have two list at least.
    :return: result: The result of the lists union.
    """
    result = list(set().union(*cmp_lists))
    return result

# 对嵌套字典中的列表进行去重
@logger.catch(level='ERROR')
def dup_nested_list_of_dict(tar):
    """

    Get a duplicate list of nested dictionary.

    :param tar: The target list that you want to duplicate.
    :return: A duplicate list of nested dictionary.
    """
    temp = list(set([str(i) for i in tar]))
    tar = [eval(i) for i in temp]
    return tar

# 将counter类型数据转换为key, value结构
@logger.catch(level='ERROR')
def kv_counter(tar: Counter, keywords: list = None, reg_word: str = None):
    """

    Transform Counter or dictionary into {'key': xxx, 'value':yyy} and return a list of the result.

    :param tar: The target which you want to transform.
    :param keywords: The keyword that you want to replace.
    :param reg_word: The new word that you want to replace.
    :return: A list of the result.
    """
    result = []
    if keywords:
        temp = 0
        for k, v in tar.items():
            if k in keywords:
                temp += v
            else:
                result.append({'key': k, 'value': v})
        result.append({'key': reg_word, 'value': temp})
    else:
        for k, v in tar.items():
            result.append({'key': k, 'value': v})
    return result

# 将元组转换为字典
@logger.catch(level='ERROR')
def tuple2dict(target: tuple):
    """

    Transform tuple into dict.

    :param target: The target which you want to transform.
    :return: A dictionary of the result.
    """
    result = dict((x, y) for x, y in target)
    return result

# 计算给定字符串的MD5值
@logger.catch(level='ERROR')
def md5H(_str):
    hl = hashlib.md5()
    hl.update(_str.encode(encoding="utf-8"))
    return hl.hexdigest()

# 计算给定json的MD5值
@logger.catch(level='ERROR')
def json_md5H(result):
    return md5H(json.dumps(result, ensure_ascii=False))

# 根据datetime生成类似mongodb的object id
@logger.catch(level='ERROR')
def oid_from_datetime(from_datetime=None):
    ''' According to the time manually generated an ObjectId '''
    if not from_datetime:
        from_datetime = datetime.now()
    return ObjectId.from_datetime(generation_time=from_datetime)

# 返回在targets中存在的elements元素
@logger.catch(level='ERROR')
def select_from_list(elements: list, targets: list):
    """

    Judge multi string if have element in list, and which one in list, then return it.

    :return:
    """
    results = [element for element in elements if element in targets]
    logger.info(f'select_from_list: {results}')
    return results

# 将时间转换为时间戳
@logger.catch(level='ERROR')
def time2timestamp(tar_time: str, time_format: str):
    time_array = time.strptime(tar_time, time_format)
    timestamp = time.mktime(time_array)
    return int(timestamp)

# 获取任一日期(datetime.datetime类型)之前n个月的倒序月份
@logger.catch(level='ERROR')
def generate__n_month(date, n):
    """

    Get the special month list that you want to.

    :param date: The start datetime, which the type is datetime.datetime.
    :param n: The number of the length, that you want to get.
    :return:A list of the result.
    """
    tar = []
    month = date.month
    year = date.year
    for i in range(n):
        if month == 1:
            year -= 1
            month = 12
        else:
            month -= 1
        tar.append(dt.date(year, month, 1).strftime('%Y-%m'))
    tar.reverse()
    return tar

# 根据start_year和end_year生成连续的年份列表
@logger.catch(level='ERROR')
def generate_years(start_year: int, end_year=None):
    years = []
    this_year = datetime.today().year
    if not end_year:
        year_range = this_year - start_year
    else:
        year_range = end_year - start_year
    for i in range(year_range + 1):
        years.append(start_year + i)
    return years

# 从字符串中提取url
@logger.catch(level='ERROR')
def extract_url(target: str):
    regex = r"\b((?:https?://)?(?:(?:www\.)?(?:[\da-z\.-]+)\.(?:[a-z]{2,6})|(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)|(?:(?:[0-9a-fA-F]{1,4}:){7,7}[0-9a-fA-F]{1,4}|(?:[0-9a-fA-F]{1,4}:){1,7}:|(?:[0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|(?:[0-9a-fA-F]{1,4}:){1,5}(?::[0-9a-fA-F]{1,4}){1,2}|(?:[0-9a-fA-F]{1,4}:){1,4}(?::[0-9a-fA-F]{1,4}){1,3}|(?:[0-9a-fA-F]{1,4}:){1,3}(?::[0-9a-fA-F]{1,4}){1,4}|(?:[0-9a-fA-F]{1,4}:){1,2}(?::[0-9a-fA-F]{1,4}){1,5}|[0-9a-fA-F]{1,4}:(?:(?::[0-9a-fA-F]{1,4}){1,6})|:(?:(?::[0-9a-fA-F]{1,4}){1,7}|:)|fe80:(?::[0-9a-fA-F]{0,4}){0,4}%[0-9a-zA-Z]{1,}|::(?:ffff(?::0{1,4}){0,1}:){0,1}(?:(?:25[0-5]|(?:2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(?:25[0-5]|(?:2[0-4]|1{0,1}[0-9]){0,1}[0-9])|(?:[0-9a-fA-F]{1,4}:){1,4}:(?:(?:25[0-5]|(?:2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(?:25[0-5]|(?:2[0-4]|1{0,1}[0-9]){0,1}[0-9])))(?::[0-9]{1,4}|[1-5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])?(?:/[\w\.-]*)*/?)\b"
    result = re.findall(regex, target)[0]
    logger.info(f'url_dom->{result}')
    return result

# 根据经纬度获取OpenStreetMap提供的地理位置信息
@logger.catch(level='ERROR')
def OpenStreetMapNominatim(lon, lat):
    """

    Get the target's street infomation from ip lon and lat locations by using OpenStreetMap.
    請求次數少量可用，大量請求使用MapQuest提供的API.->使用geocoder庫即可。

    :param lon: The IP lon infos, which refer WGS84.
    :param lat: The IP lat infos, which refer WGS84.
    :return: The OpenStreetMap information.
    """
    osmn_ulr = f'http://nominatim.openstreetmap.org/reverse?format=json&lat={str(lat)}&lon={str(lon)}&zoom=18&addressdetails=1'
    try:
        header = {'User-Agent': random.choice(ua)}
        resp = requests.get(osmn_ulr, headers=header).content.decode('utf-8')
    except Exception as e:
        logger.info(f'OpenStreetMap request failed, will retry!')
        sleep(rant(1, 5))
        header = {'User-Agent': random.choice(ua)}
        resp = requests.get(osmn_ulr, headers=header).content.decode('utf-8')
    result = json.loads(resp)
    del result['place_id']
    del result['licence']
    del result['osm_type']
    del result['osm_id']
    return result

# 根据MapQuest提供的API, 根据经纬度获取OpenStreetMap提供的地理位置信息
@logger.catch(level='ERROR')
def GeocoderOSMN(lon:float, lat:float):
    """

    Use MapQuest Python lib api to get OpenStreetMap information.

    :param lon: The IP lon infos, which refer WGS84.
    :param lat: The IP lat infos, which refer WGS84.
    :return: The same as OpenStreetMap output information.
    """
    MapQuestKey = ''
    result = geocoder.osm([lat, lon], method='reverse', key=MapQuestKey).json
    if not result:
        logger.info(f'OpenStreetMap request failed, will retry!')
        sleep(rant(1, 3))
        result = geocoder.osm([lat, lon], method='reverse', key=MapQuestKey).json
    final_result = {'lat': result['raw']['lat'], 'lon': result['raw']['lon'], 'display_name': result['raw']['display_name'], 'address': result['raw']['address'], 'boundingbox': result['raw']['boundingbox']}
    return final_result
