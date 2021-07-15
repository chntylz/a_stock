#!/#!/usr/bin/env python  
# -*- coding: utf-8 -*-

#龙虎榜机构统计
#LHBJGTJ
#https://data.eastmoney.com/stock/jgmmtj.html
#https://datainterface3.eastmoney.com/EM_DataCenter_V3/api/LHBJGTJ/GetHBJGTJ?js=jQuery1123042907327005290474_1624455435981&sortfield=PBuy&sortdirec=1&pageSize=50&pageNum=1&tkn=eastmoney&code=&mkt=0&dateNum=&cfg=lhbjgtj&startDateTime=2021-06-23&endDateTime=2021-06-23



import pandas as pd
import json
import requests
import re
import numpy as np

import time
import datetime

import sys
sys.path.append("..")
from file_interface import *

from HData_eastmoney_dragon import *

debug = 0

hdata_dragon = HData_eastmoney_dragon('usr', 'usr')

def check_table():
    table_exist = hdata_dragon.table_is_exist()
    print('table_exist=%d' % table_exist)
    if table_exist:
        #hdata_dragon.db_hdata_xq_create()
        print('table already exist')
    else:
        hdata_dragon.db_hdata_xq_create()
        print('table not exist, create')



def get_dragon_tiger(date=None):

    timestamp=str(round(time.time() * 1000))

    if date == None:
        nowdate=datetime.datetime.now().date()
        date = nowdate.strftime("%Y-%m-%d")

    #https://www.zhihu.com/question/31600760
    url='https://datainterface3.eastmoney.com/EM_DataCenter_V3/api/LHBJGTJ/GetHBJGTJ?js=jQuery1123029660230486644434_'\
            + timestamp \
            + '&sortfield=PBuy&sortdirec=1&pageSize=5000&pageNum=1&tkn=eastmoney&code=&mkt=0&dateNum=&cfg=lhbjgtj&startDateTime='\
            + date \
            + '&endDateTime='\
            + date

    response = requests.get(url)
    p1 = re.compile(r'[(](.*?)[)]', re.S)
    response_array = re.findall(p1, response.text)
    api_param = json.loads(response_array[0])
    tmp_column= api_param['Data'][0]['FieldName']
    tmp_column=tmp_column.split(',')
    rawdata = api_param['Data'][0]['Data']
    data_df = pd.DataFrame(rawdata)
    if len(data_df):
        data_df = data_df[0].str.split('|', expand=True)
        data_df.columns=tmp_column

    return data_df


if __name__ == '__main__':

    cript_name, para1 = check_input_parameter()

    nowdate=datetime.datetime.now().date()
    nowdate=nowdate-datetime.timedelta(int(para1))
    
    check_table()

    df = get_dragon_tiger(nowdate.strftime("%Y-%m-%d"))
    if len(df):
        df = df.drop_duplicates('SCode')
        df = df.reset_index(drop=True)
        df = df.fillna(0)
        df = df.replace('',0)

        cur_time = time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime())
        df.to_csv('./csv_data/' + cur_time + '-dragon.csv', encoding='gbk')
        hdata_dragon.copy_from_stringio(df)
    else:
        print('dragon not found!!!')
