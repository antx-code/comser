import queue
import uvloop
import aiodns
import asyncio
import traceback
from loguru import logger
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
uvloop.install()

class AioDns():
    @logger.catch(level='ERROR')
    def __init__(self):
        super(AioDns, self).__init__()
        self.coroutine_count = 300
        self.all_servers = [
            "223.5.5.5", "223.6.6.6",  # alidns
            "180.76.76.76",  # baidudns
            "119.29.29.29",  # tendns
            "208.67.222.222", "208.67.220.220",  # OpenDNS
            "84.200.69.80", "84.200.70.40",  # DNS Watch
            # "114.114.114.114",  # 114DNS
            # "8.8.8.8",  # Google DNS
            "1.1.1.1"  # CloudFlare DNS
        ]  # 标准 DNS 服务器

    @logger.catch(level='ERROR')
    async def analysis_query(self, resolver, subdomain, semaphore):
        """
        协程解析域名
        :param resolver:
        :param subdomain:
        :param semaphore:
        :return:
        """
        results = {subdomain: []}
        try:
            async with semaphore:
                a_result = await resolver.query(subdomain, 'A')
        except aiodns.error.DNSError as e:
            err_code, err_msg = e.args[0], e.args[1]
        except Exception as e:
            logger.error(traceback.format_exc())
        else:
            a_ret = [r.host for r in a_result]
            domain_ips = sorted([s for s in a_ret])
            if domain_ips:
                results[subdomain] = domain_ips
        return results

    @logger.catch(level='ERROR')
    async def get_analysis_result(self, loop, resolver, subdomain_queue):
        """
        获取域名解析结果并保存
        :return:
        """
        results = []
        tasks = []
        semaphore = asyncio.Semaphore(self.coroutine_count)
        while not subdomain_queue.empty():
            subdomain = subdomain_queue.get()
            tasks.append(self.analysis_query(resolver, subdomain, semaphore))
        result = await asyncio.gather(*tasks)
        for item in result:
            results.append(item)
        return results

    @logger.catch(level='ERROR')
    async def main(self, subdomains: list):
        """
        搜索程序执行
        :return:
        """
        uvloop.install()
        subdomain_queue = queue.Queue()
        for item in subdomains:
            subdomain_queue.put(item.strip())
        loop = asyncio.get_event_loop()
        resolver = aiodns.DNSResolver(loop=loop, nameservers=self.all_servers, timeout=3)
        result = await self.get_analysis_result(loop, resolver, subdomain_queue)
        logger.info(f'aiodns result: {result}')
        return result

