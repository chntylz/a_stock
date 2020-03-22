#!/usr/bin/env python  
# -*- coding: utf-8 -*-

import psycopg2 #使用的是PostgreSQL数据库
import tushare as ts
from Stocks import *
from HData_dailybasic import *
import  datetime

from time import clock


import pandas as pd

import sys
import os
from sys import argv
    
token='21dddafc47513ea46b89057b2c4edf7b44882b3e92274b431f199552'
pro = ts.pro_api(token)


debug = 1

def check_input_parameter():
# 如果执行的方式错误输出使用方法
    USAGE = '''
    用法错误，正确方式如下：
    python demo.py 1
    '''
    if len(argv) > 2:
        print(USAGE)  # 如果传入的参数不足，输出正确用法
        exit(1) # 异常退出(下面的代码将不会被执行)

    script_name, para1 = argv  # 将传入的参数赋值进行使用
    print("%s, %d"%(script_name, int(para1)))

    return script_name, para1


def update_dailybasic(curr_date):
    data_format = " stock_code, record_date, close, turnover_rate, turnover_rate_f, volume_ratio, pe, pe_ttm, pb,  ps, ps_ttm, dv_ratio, dv_ttm, total_share, float_share, free_share, total_mv, circ_mv"
    data_format = " ts_code, trade_date, close, turnover_rate, turnover_rate_f, volume_ratio, pe, pe_ttm, pb,  ps, ps_ttm, dv_ratio, dv_ttm, total_share, float_share, free_share, total_mv, circ_mv"
    hist_data = pro.daily_basic(ts_code='', trade_date=curr_date.strftime("%Y%m%d"), fields=data_format)

    if(len(hist_data) == 0):
        return

    hist_data=hist_data.fillna(0)
    hist_data['ts_code'] = hist_data['ts_code'].apply(lambda x: x[0:6])
    hist_data['trade_date']=hist_data['trade_date'].apply(lambda x: datetime.datetime.strptime(x,'%Y%m%d'))
    hist_data = hist_data.set_index('trade_date')


    if debug:
        print(hist_data.head(10))

    hdata_dailybasic.insert_allstock_hdatadate(hist_data)

    if debug:
        print("2", nowdate, hist_data)

if __name__ == '__main__':

    t1 = clock()
    script_name, para1 = check_input_parameter()

    hdata_dailybasic=HData_dailybasic("usr","usr")
    hdata_dailybasic.db_hdata_date_create()
    nowdate=datetime.datetime.now().date()
    nowdate=nowdate-datetime.timedelta(int(para1))

    hdata_dailybasic.db_connect()#由于每次连接数据库都要耗时0.0几秒，故获取历史数据时统一连接
    hdata_dailybasic.delete_data_of_day_stock(nowdate.strftime("%Y-%m-%d")) #delete first
    update_dailybasic(nowdate)
    t2 = clock()
    print("t1:%s, t2:%s, delta=%s"%(t1, t2, t2-t1))
    hdata_dailybasic.db_disconnect()
