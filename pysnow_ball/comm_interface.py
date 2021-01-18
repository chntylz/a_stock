#!/usr/bin/env python
#coding:utf-8
#https://www.cnblogs.com/shaosks/p/5614630.html

import time
import datetime

import os,sys,os.path
from sys import argv
import shutil

#把时间戳转化为时间: 1479264792 to 2016-11-16 10:53:12
def get_date_from_timestamp(timestamp):
    d = datetime.datetime.fromtimestamp(timestamp / 1000, None)  # 时间戳转换成字符串日期时间
    my_date = d.strftime("%Y-%m-%d %H:%M:%S.%f")
    my_date = my_date[:10]
    return my_date

def get_snowball_timestamp():
    timestamp = time.time() * 1000
    return timestamp 



import requests
def get_cookie():
    token_key = 'xq_a_token'
    url = "https://xueqiu.com/S/SH000001"
    Hostreferer = {
        #'Host':'***',
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) \
                AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.102 Safari/537.36'
    }
    #urllib或requests在打开https站点是会验证证书。 
    #简单的处理办法是在get方法中加入verify参数，并设为False
    html = requests.get(url, headers=Hostreferer,verify=False)
    #获取cookie:DZSW_WSYYT_SESSIONID
    if html.status_code == 200:
        #print(html.cookies)
        for cookie in html.cookies:
            #print(cookie)
            if token_key in str(cookie):
                tmp = str(cookie)
                tmp = tmp[tmp.find(token_key):len(tmp)]
                tmp = tmp[:tmp.find(' ')]
                return tmp

    else:

        return -1

    
