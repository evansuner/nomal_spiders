# -*-encoding: utf-8-*-
"""
@author: Evan
@description: 异步
@date: 2023/3/9 15:35
"""
import asyncio
import datetime
import json
import random
import time

import aiohttp
import logging
from utils import trans_stamp_to_datetime, is_last_workweek, get_tree, db_store, log_store, send_mail


logging.basicConfig(level=logging.INFO, format='[%(asctime)s] - %(name)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger('spider_wechat')


async def get_resp(url, headers, payload, timeout, retries) -> json:
    if not timeout and not retries:
        timeout = 15
        retries = 3
    for i in range(retries):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers, timeout=timeout, params=payload) as response:
                    if response.status == 200:
                        content = await response.json()
                        return content
        except aiohttp.ClientError:
            logger.info(f"Failed to fetch data from {url}. Retrying ({i + 1}/{retries})...")
            await asyncio.sleep(1)
        raise Exception(f"Failed to fetch data from {url} after {retries} attempts.")


async def parse(c, s, e, t, r, to, co):
    """
    :param c: config
    :param s: start
    :param e: end
    :param t: timeout
    :param r: retries
    :param to: token
    :param co: cookie
    :return:
    """
    url_list = []
    url = "https://mp.weixin.qq.com/cgi-bin/appmsg?"
    headers = c["headers"]
    payload = c['payload']
    if to and co:
        headers["cookie"] = co
        payload["token"] = to
    keywords = ["数字经济", "规划", "纲要", "白皮书", "标准", "评估",
                "报告", "手册", "数字化", "转型", "案例", "方案",
                "行业", "信息化", "智慧", "数智化", "数改", "区块链",
                "人工智能", "大数据", "数据安全", "数据治理", "数仓", "数据仓库",
                "数据架构", "中台", "BI"]
    for key, values in c["wechat_official_accounts"].items():
        fakeid = values["fakeid"]
        i = 0
        max_page = c["max_page"]
        while i < max_page:
            begin = i * int(payload['count'])
            payload['begin'] = str(begin)
            payload["fakeid"] = fakeid
            time.sleep(random.randint(1, 2))
            resp = await get_resp(url, headers=headers, payload=payload, timeout=t, retries=r)
            if resp['base_resp']['ret'] == 200013:  # 微信流量控制
                print("frequencey control, stop at {}".format(str(begin)))
                time.sleep(3600)
                continue
            if 'app_msg_list' not in resp and c["mail"]["on"] == "true":
                print(f'Error: {resp["base_resp"]}')
                print('please try refresh token and cookie!')
                mail_user = c["mail"]["mail_user"]
                mail_host = c["mail"]["mail_host"]
                mail_pass = c["mail"]["mail_pass"]
                sent_mail = c["mail"]["sent_mail"]
                send_mail(mail_user, mail_host, mail_pass, sent_mail, [],
                          "Please try refresh token and cookie!", "", html=True)
                return []
            # 返回内容为空则结束
            if len(resp['app_msg_list']) == 0:
                print('all article parsed')
                break

            if 'app_msg_list' in resp:
                for item in resp['app_msg_list']:
                    # 时间范围
                    created_at = trans_stamp_to_datetime(item["update_time"])
                    if is_last_workweek(datetime.datetime.strftime(created_at, '%Y-%m-%d'), s, e):
                        title = item["title"]
                        link = item["link"]
                        keys = []
                        for k in keywords:
                            if k in title:
                                keys.append(k)
                        if keys:
                            print({"link": link, "source_name": values["name"]})
                            url_list.append({"link": link, "created_at": created_at,
                                             "keywords": keys, "title": title, "source_name": values["name"]})
                    else:
                        continue
            i += 1

    log = []
    info = []
    info_category_map = []
    if url_list:
        for dic in url_list:
            spider_start = trans_stamp_to_datetime(time.time())
            title = dic["title"]
            keywords = dic["keywords"]
            link = dic["link"]
            source_name = dic["source_name"]
            source_link = "https://mp.weixin.qq.com"
            created_at = dic["created_at"]
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(dic["link"], headers=headers, timeout=t) as response:
                        if response.status == 200:
                            resp = await response.text()
                            tree = get_tree(resp)
                            xpath = '//*[@id="js_content"]//p//text()'
                            content = "".join(tree.xpath(xpath))
                            spider_end = trans_stamp_to_datetime(time.time())
                            status = 'success'
                            error_msg = ""
                            # print(content)
                            info_li, map_li = db_store(title, link, source_name, source_link, content, created_at,
                                                       keywords)
                            info.extend(info_li)
                            info_category_map.extend(map_li)
                            log_dic = log_store(link, source_name, source_link, spider_start, spider_end, status,
                                                error_msg)
                            log.append(log_dic)
            except Exception as e:
                error_msg = str(e)
                spider_end = trans_stamp_to_datetime(time.time())
                status = "fail"
                log_dic = log_store(link, source_name, source_link, spider_start, spider_end, status,
                                    error_msg)
                log.append(log_dic)
    res = {
        "log": log,
        "info": info,
        "info_category_map": info_category_map
    }
    return res


if __name__ == '__main__':
    # config = read_conf(r'./config/wechat.yaml')
    # start = None
    # end = None
    # timeout = None
    # retries = None
    # token = None
    # cookie = None
    # loop = asyncio.get_event_loop()
    # results = loop.run_until_complete(parse(config, start, end, timeout, retries, token, cookie))
    # print(results)
    pass
