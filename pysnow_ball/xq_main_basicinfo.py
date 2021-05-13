#!/usr/bin/env python  
# -*- coding: utf-8 -*-

import requests
import json
import numpy as np
import pandas as pd

import time
import datetime
from comm_interface import *

import psycopg2 #使用的是PostgreSQL数据库
from HData_xq_basicinfo import *

debug = 0


hdata_basicinfo=HData_xq_basicinfo("usr","usr")

#stock_type: sha, sza, sh-sz, zxb, cyb
def get_basicinfo_data(stock_type='sh-sz'):

    HEADERS = {
        'Accept': '*/*',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
        'Connection': 'keep-alive',
        'host': 'xueqiu.com',
        'Referer': 'https://xueqiu.com/',
        'User-agent': 'Mozilla/5.0 (Windows NT 10.0; WOW 64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36 QIHU 360SE'
    }

    timestamp=str(round(time.time() * 1000))
    url="https://xueqiu.com/service/v5/stock/screener/quote/list?page=1&size=6000"+ \
            "&order=desc&order_by=percent&exchange=CN&market=CN&type=" + \
            stock_type + "&_=" + timestamp

    response = requests.get(url, headers=HEADERS)
    response.encoding='utf-8'
    #response.text
    if debug:
        print(url)
        print(response.status_code)


    ret=json.loads(response.content)

    data=ret['data']['list']

    df = pd.DataFrame(data)

    df=df.fillna(0)
    df=df[(df.type==11)|(df.type==82)]
    df=df.reset_index(drop=True)
    df.issue_date_ts = df.issue_date_ts.apply(lambda x: get_date_from_timestamp(int(x)))

    return df



if __name__ == '__main__':

    t1 = time.time()
    start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    nowdate=datetime.datetime.now().date()

    table_exist = hdata_basicinfo.table_is_exist() 
    if not table_exist:
        hdata_basicinfo.db_hdata_xq_create()

    basicinfo_df = get_basicinfo_data()
    basicinfo_df.insert(1, 'record_date' , nowdate.strftime("%Y-%m-%d"), allow_duplicates=False)

    basicinfo_df.to_csv('./test_basicinfo.csv', encoding='gbk')

    hdata_basicinfo.copy_from_stringio(basicinfo_df)
    
    
    last_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print("start_time: %s, last_time: %s" % (start_time, last_time))

    t2 = time.time()
    print("t1:%s, t2:%s, delta=%s"%(t1, t2, t2-t1))
