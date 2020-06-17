#!/usr/bin/env python  
# -*- coding: utf-8 -*-

import psycopg2 #使用的是PostgreSQL数据库
import tushare as ts
from Stocks import *
from HData_divided import *
import  datetime

from time import clock
from comm_generate_web_html import handle_divided

import pandas as pd
import numpy as np

import sys
import os

from file_interface import *
    
token='21dddafc47513ea46b89057b2c4edf7b44882b3e92274b431f199552'
pro = ts.pro_api(token)


debug=0
#debug=1


stocks=Stocks("usr","usr")
hdata_divided=HData_divided("usr","usr")

hdata_divided.db_hdata_date_create()

def get_daily_data(codestock_local, nowdate):

    length=len(codestock_local)

    #create a null dataframe
    #new_data =  pd.DataFrame()

    data_list = []
    for i in range(0,length):
        nowcode=codestock_local[i][0]
        #nowcode='600485'

        if handle_divided(nowcode, nowdate.strftime("%Y%m%d")):
            is_divided = 1
        else:
            is_divided = 0
        
        if debug:
            print('nowcode=%s, nowdate:%s, it is divided=%d'% (nowcode, nowdate.strftime("%Y%m%d"), is_divided))

        data_list.append([nowdate, nowcode, is_divided])


    #handle hist_data
    data_column = ['datetime', 'stock_code', 'is_divided']
    new_data =  pd.DataFrame(data_list, columns=data_column)

    hist_data = new_data.set_index('datetime')


    if debug:
        print(hist_data.head(10))

    hdata_divided.insert_allstock_hdatadate(hist_data)

if __name__ == '__main__':
    
    cript_name, para1 = check_input_parameter()
    t1 = clock()
    start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    nowdate=datetime.datetime.now().date()
    nowdate=nowdate-datetime.timedelta(int(para1))
    print("nowdate is %s"%(nowdate.strftime("%Y-%m-%d")))

    codestock_local=stocks.get_codestock_local()

    hdata_divided.db_connect()#由于每次连接数据库都要耗时0.0几秒，故获取历史数据时统一连接

    get_daily_data(codestock_local, nowdate)

    last_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print("start_time: %s, last_time: %s" % (start_time, last_time))

    t2 = clock()
    print("t1:%s, t2:%s, delta=%s"%(t1, t2, t2-t1))
    hdata_divided.db_disconnect()
