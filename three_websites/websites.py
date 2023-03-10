# -*-encoding: utf-8-*-
"""
@author: Evan
@description: 异步
@date: 2023/3/7 14:46
"""
import logging
import time
import warnings

import aiohttp
import asyncio
import chardet
import pymysql

from utils import is_last_workweek, trans_stamp_to_datetime, \
    trans_date_to_datetime, get_date_by_re, get_tree, db_store, log_store

warnings.filterwarnings('ignore')
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] - %(name)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger('spider_websites')


async def get_resp(url, timeout, retries) -> str:
    headers = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36"
    }
    if timeout is None and retries is None:
        timeout = 15
        retries = 3
    for i in range(retries):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers, timeout=timeout) as response:
                    if response.status == 200:
                        content = await response.read()
                        encoding = chardet.detect(content)['encoding']
                        if encoding is None:
                            encoding = 'utf-8'
                        return content.decode(encoding)
        except aiohttp.ClientError:
            logger.info(f"Failed to fetch data from {url}. Retrying ({i + 1}/{retries})...")
            await asyncio.sleep(1)
        raise Exception(f"Failed to fetch data from {url} after {retries} attempts.")


def commit_db_execute(sql) -> None:
    conn_config = {
        'user': 'root',
        'host': 'localhost',
        'password': 'Root@sail-fs',
        'port': 3306,
        'db': 'wechat_dev'
    }
    conn = pymysql.connect(**conn_config)
    cursor = conn.cursor()
    cursor.execute(sql)
    conn.commit()
    conn.close()


async def parse(dt_insight_url, digital_service_urls, digital_observation_urls, start, end, timeout,
                retries):
    # 全局变量
    log = []
    info = []
    info_category_map = []
    keywords = ["数字经济", "规划", "纲要", "白皮书", "标准", "评估",
                "报告", "手册", "数字化", "转型", "案例", "方案",
                "行业", "信息化", "智慧", "数智化", "数改", "区块链",
                "人工智能", "大数据", "数据安全", "数据治理", "数仓", "数据仓库",
                "数据架构", "中台", "BI"]

    # 数字化发展研究中心
    response = await get_resp(dt_insight_url, timeout, retries)
    tree = get_tree(response)
    root_node = list(set(tree.xpath('//div[@id="newsList402" or @id="newsList403"]/div//a/@href')))
    for i in root_node:
        source_name = source_link = link = spider_start = spider_end = None
        try:
            spider_start = trans_stamp_to_datetime(time.time())
            next_response = await get_resp(i, timeout, retries)
            next_tree = get_tree(next_response)
            title = next_tree.xpath('//div[@id="module12"]//h1/text()')[0].strip()
            swap_date = \
                next_tree.xpath('//div[@id="module12"]//div/span[@class="newsInfo"]/text()')[0].split("：")[-1].split(
                    " ")[
                    0].strip()
            publish_date = trans_date_to_datetime(swap_date)
            link = i
            source_link = "http://www.dtinsight.com.cn"
            source_name = "数智化发展研究中心"
            # 数据库是text类型， 但是以json写入
            content = "".join(next_tree.xpath('//*[@id="module12"]/div/div/div/div/div[3]//text()')[1:])
            # created_at 是文章的发布时间
            created_at = publish_date
            spider_end = trans_stamp_to_datetime(time.time())
            error_msg = ""
            status = "success"
            # 判断日期是否符合规则
            if is_last_workweek(swap_date, start, end):
                info_li, map_li = db_store(title, link, source_name, source_link, content, created_at, keywords)
                info.extend(info_li)
                info_category_map.extend(map_li)
                log_dic = log_store(link, source_name, source_link, spider_start, spider_end, status, error_msg)
                log.append(log_dic)
        except Exception as e:
            error_msg = str(e)
            status = "fail"
            log_dic = log_store(link, source_name, source_link, spider_start, spider_end, status, error_msg)
            log.append(log_dic)

    # 中国数字化服务平台
    root_nodes = []
    for digital_service_baseurl in digital_service_urls:
        response = await get_resp(digital_service_baseurl, timeout, retries)
        tree = get_tree(response)
        root_node = list(set(tree.xpath('//*[@id="listpage2"]/ul/li/a/@href')))
        root_nodes.extend(["http://china-credit.org.cn" + i for i in root_node])
    for i in root_nodes:
        source_name = source_link = link = spider_start = spider_end = None
        try:
            spider_start = trans_stamp_to_datetime(time.time())
            next_response = await get_resp(i, timeout, retries)
            next_tree = get_tree(next_response)
            title = next_tree.xpath('//div[@class="page-leftbox"]//h1/text()')[0].strip()
            swap_date_str = next_tree.xpath('//div[@class="page-leftbox"]//div[@class="art-status"]/text()')[0]
            swap_date = get_date_by_re(swap_date_str)
            publish_date = trans_date_to_datetime(swap_date)
            link = i
            source_name = "中国数字化服务平台"
            source_link = "http://china-credit.org.cn"
            # 数据库是text类型， 但是以json写入
            content = "".join(next_tree.xpath('//article[@class="art-view-box"]/div[2]//p/text()'))
            created_at = publish_date  # 文章的发布时间
            spider_end = trans_stamp_to_datetime(time.time())
            error_msg = ""
            status = "success"
            # 判断日期是否符合规则
            if is_last_workweek(swap_date, start, end):
                info_li, map_li = db_store(title, link, source_name, source_link, content, created_at, keywords)
                info.extend(info_li)
                info_category_map.extend(map_li)
                log_dic = log_store(link, source_name, source_link, spider_start, spider_end, status, error_msg)
                log.append(log_dic)
        except Exception as e:
            error_msg = str(e)
            status = "fail"
            log_dic = log_store(link, source_name, source_link, spider_start, spider_end, status, error_msg)
            log.append(log_dic)

    # 数字化观察网
    root_nodes = []
    for digital_observation_baseurl in digital_observation_urls:
        response = await get_resp(digital_observation_baseurl, timeout, retries)
        tree = get_tree(response)
        root_node = list(set(tree.xpath('//div[@class="cate-list"]/div//h3/a/@href')))
        root_nodes.extend(["http://www.miitnet.com/" + i for i in root_node])
    for i in root_nodes:
        source_name = source_link = link = spider_start = spider_end = None
        try:
            spider_start = trans_stamp_to_datetime(time.time())
            next_response = await get_resp(i, timeout, retries)
            next_tree = get_tree(next_response)
            title = next_tree.xpath('//div[@class="article-body"]/h2/text()')[0].strip()
            swap_date = next_tree.xpath('//div[@class="article-body"]/div/p/span[1]/text()')[0].strip()
            publish_date = trans_date_to_datetime(swap_date)
            link = i
            source_link = "http://www.miitnet.com/"
            source_name = "数字化观察网"
            # 数据库是text类型， 但是以json写入
            content = "".join(next_tree.xpath('//div[@class="article-content"]//p//text()'))
            # created_at 是文章的发布时间
            created_at = publish_date
            spider_end = trans_stamp_to_datetime(time.time())
            error_msg = ""
            status = "success"
            # 判断日期是否符合规则
            if is_last_workweek(swap_date, start, end):
                info_li, map_li = db_store(title, link, source_name, source_link, content, created_at, keywords)
                info.extend(info_li)
                info_category_map.extend(map_li)
                log_dic = log_store(link, source_name, source_link, spider_start, spider_end, status, error_msg)
                log.append(log_dic)
        except Exception as e:
            error_msg = str(e)
            status = "fail"
            log_dic = log_store(link, source_name, source_link, spider_start, spider_end, status, error_msg)
            log.append(log_dic)
    total_info = {
        "log": log,
        "info": info,
        "info_category_map": info_category_map
    }
    return total_info


if __name__ == '__main__':
    # dt_insight_baseurl = "http://www.dtinsight.com.cn/col.jsp?id=104"
    # digital_service_baseurls = [
    #     "http://china-credit.org.cn/article_list.aspx?classid=2&page=1",
    #     "http://china-credit.org.cn/article_list.aspx?classid=2&page=2"
    # ]
    # digital_observation_baseurls = [
    #     "http://www.miitnet.com/news/mrgc/index.html",
    #     "http://www.miitnet.com/news/mrgc/News_2.html",
    #     "http://www.miitnet.com/news/mrgc/News_3.html"
    # ]
    # start = None
    # end = None
    # timeout = None
    # retries = None
    # a = time.time()
    # results = asyncio.run(
    #     parse(dt_insight_baseurl, digital_service_baseurls, digital_observation_baseurls, start, end, timeout,
    #           retries))
    # print(results)
    # print(time.time() - a)
    pass
