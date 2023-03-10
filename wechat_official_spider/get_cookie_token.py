from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from settings import *
import time

import json

service = Service(executable_path='C:\windows\chromedriver.exe')

post = {}

driver = webdriver.Chrome(service=service)

driver.get('https://mp.weixin.qq.com/')

time.sleep(2)
# 使用账号登录
driver.find_element('xpath', '//*[@id="header"]/div[2]/div/div/div[2]/a').click()
driver.find_element('xpath', '//*[@id="header"]/div[2]/div/div/div[1]/form/div[1]/div[1]/div/span/input').clear()
driver.find_element('xpath',
                    '//*[@id="header"]/div[2]/div/div/div[1]/form/div[1]/div[1]/div/span/input')\
    .send_keys(ACCOUNT)
driver.find_element('xpath', '//*[@id="header"]/div[2]/div/div/div[1]/form/div[1]/div[2]/div/span/input').clear()
driver.find_element('xpath',
                    '//*[@id="header"]/div[2]/div/div/div[1]/form/div[1]/div[2]/div/span/input')\
    .send_keys(PASSWORD)

# 在自动输完密码之后记得点一下记住我
driver.find_element('xpath', '//*[@id="header"]/div[2]/div/div/div[1]/form/div[3]/label/i').click()
time.sleep(5)

driver.find_element("xpath", '//*[@id="header"]/div[2]/div/div/div[1]/form/div[4]/a').click()

# 拿手机扫二维码！
time.sleep(15)

driver.get('https://mp.weixin.qq.com/')

cookie_items = driver.get_cookies()
current_url = driver.current_url
token = current_url.split("token=")[-1]
print(token)
string = ""
for cookie_item in cookie_items:
    string += f"{cookie_item['name']}={cookie_item['value']};"

print(string)