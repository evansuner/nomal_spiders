# -*-encoding: utf-8-*-
"""
@author: Evan
@description: None
@date: 2023/3/9 10:48
"""
import json
import re
import smtplib
import time
import uuid
from datetime import datetime, timedelta
from email.mime.text import MIMEText

import pymysql
import requests
import yaml
from lxml import etree


def fetch_db_execute(sql) -> list:
    """
    :param sql: sql语句
    :return: 数据库查询结果，返回list类型
    """
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
    cursor.close()
    conn.close()
    return [i[0] for i in cursor.fetchall()]


def get_tree(r) -> etree:
    """
    :param r: respponse.text
    :return: etree对象
    """
    tree = etree.HTML(r)
    return tree


def trans_date_to_datetime(d) -> datetime:
    """使用strptime()函数将字符串格式的日期转换为datetime对象"""
    date_obj = datetime.strptime(d, "%Y-%m-%d") if '-' in d else datetime.strptime(d, "%Y/%m/%d")
    return date_obj


def trans_stamp_to_datetime(stamp: time.time = None) -> datetime:
    """将时间戳转换成datetime"""
    dt_obj = datetime.fromtimestamp(stamp)
    dt_obj = dt_obj.replace(microsecond=0)
    return dt_obj


def is_last_workweek(date_str: str, start: str = None, end: str = None) -> bool:
    """
    :param date_str: 需要判断的时间
    :param start: 爬虫开始时间
    :param end: 爬虫结束时间
    :return: bool
    """
    # 将字符串类型的日期转换成datetime类型
    date = datetime.strptime(date_str, '%Y-%m-%d')
    # 获取今天的日期
    today = datetime.now().date()
    # 计算上一个工作周期的起始日期和结束日期
    if not start and not end:
        last_workweek_start = (today - timedelta(days=today.weekday() + 7)).strftime('%Y-%m-%d')
        last_workweek_end = (today - timedelta(days=today.weekday() + 1)).strftime('%Y-%m-%d')
    else:
        last_workweek_start = datetime.strptime(start, "%Y-%m-%d")
        last_workweek_end = datetime.strptime(end, '%Y-%m-%d')
    # 将上一个工作周期的日期范围转换成datetime类型
    last_workweek_start = datetime.strptime(last_workweek_start, '%Y-%m-%d')
    last_workweek_end = datetime.strptime(last_workweek_end, '%Y-%m-%d')
    # 判断给定日期是否在上一个工作周期内
    return last_workweek_start <= date <= last_workweek_end


def get_date_by_re(s) -> str:
    """
    :param s: "来源：人民日报 更新时间：2023/2/27 浏览："
    :return:
    """
    pattern = r"更新时间：(\d{4}/\d{1,2}/\d{1,2})"

    # 使用正则表达式进行匹配
    match = re.search(pattern, s)

    if match:
        # 如果匹配成功，则提取更新时间信息
        update_time = match.group(1)
        date_obj = datetime.strptime(update_time, "%Y-%m-%d") if '-' in update_time else datetime.strptime(update_time,
                                                                                                           "%Y/%m/%d")
        date = datetime.strftime(date_obj, "%Y-%m-%d")
        return date


def get_classify_code_by_keyword(keyword) -> str:
    """
    使用标题关键字获取对应的分类
    :param keyword: 标题关键字
    :return: 返回分类名 str类型
    """
    url = ""
    data = requests.post(url).json()
    for item in data:
        if item["keyword"] == keyword:
            classify_code = item["classify_code"]
            return classify_code


def db_store(title, link, source_name, source_link, content, created_at, keywords: list = None):
    """
    处理info和info_category_map
    :param title: 文章标题
    :param link: 文章链接
    :param source_name: 来源名称
    :param source_link: 来源名称对应的链接
    :param content: 文章内容
    :param created_at: 文章发布时间
    :param keywords: 文章标题所含关键词
    :return: 分别返回对应info和info_category_map的list
    """
    info = []
    info_map = []
    title_keywords = set()
    classify_codes = set()
    info_id = str(uuid.uuid4())
    for key in keywords:
        if key in title:
            title_keywords.add(key)
            # 使用接口获取到所有的keywords
            sql1 = "select classify_code from keywords where keyword='%s';" % key
            classify = fetch_db_execute(sql1)[0]
            classify_codes.add(classify)
            # 如果两者都存在， 执行数据库写入操作
            if title_keywords and classify_codes:
                # 写入之前判断数据库中是否已存在该条数据 使用title来做判断依据
                # 这里不再做任何判断， 直接使用接口提交数据
                json_keywords = json.dumps(list(title_keywords), ensure_ascii=False)
                # 提交数据
                info_dic = {
                    'id': info_id,
                    'title': title,
                    'keywords': json_keywords,
                    'link': link,
                    'source_name': source_name,
                    'source_link': source_link,
                    'content': content,
                    'created_at': created_at
                }
                info.append(info_dic)

                # one-many写入映射表
                for classify_code in classify_codes:
                    map_dic = {
                        classify_code: classify_code,
                        info_id: info_id,
                    }
                    info_map.append(map_dic)
    return info, info_map


def log_store(link, source_name, source_link, spider_start, spider_end, status, error_msg) -> dict:
    """
    处理log
    :param link: 文章链接
    :param source_name: 文章来源名称
    :param source_link: 文章来源链接
    :param spider_start: 爬虫开始时间
    :param spider_end: 爬虫结束时间
    :param status: 爬虫状态 success or fail
    :param error_msg: 错误信息， 当status=success时位"", 否则对应错误信息
    :return: dict对象
    """
    log_dic = {
        "source_name": source_name,
        "source_link": source_link,
        "url": link,
        "spider_start": spider_start,
        "spider_end": spider_end,
        "status": status,
        "error_msg": error_msg
    }
    return log_dic


def send_mail(mail_user, mail_host, mail_pass, to_list, cc_list, sub, content, html=False):
    """
    邮件发送
    :param mail_user: 发件人
    :param mail_host: 邮箱主机
    :param mail_pass: 邮箱密码
    :param to_list: 收件人列表
    :param cc_list: 抄送人列表
    :param sub: 主题
    :param content: 内容
    :param html: html内容
    :return:
    """
    msg = MIMEText(content)
    if html:
        msg = MIMEText(content, _subtype='html', _charset='UTF-8')
    msg['To'] = ';'.join(to_list)
    msg['Cc'] = ', '.join(cc_list)
    msg['From'] = mail_user
    msg['Subject'] = sub
    to_list.extend(cc_list)
    try:
        server = smtplib.SMTP()
        server.connect(mail_host)
        server.login(mail_user, mail_pass)
        server.sendmail(mail_user, to_list, msg.as_string())
        server.close()
        print()
        print('mail sent successfully: ' + sub)
    except Exception as e:
        print()
        print(e)
        print('Error: unable to send mail: ' + sub)


def read_conf(file) -> dict:
    """
    读配置文件
    :param file: 文件路径
    :return: dict对象
    """
    with open(file, 'r', encoding='utf-8') as f:
        config_file = yaml.safe_load(f)
        return config_file
