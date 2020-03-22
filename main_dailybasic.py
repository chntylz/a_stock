#!/usr/bin/env python  
# -*- coding: utf-8 -*-

import psycopg2 #使用的是PostgreSQL数据库
import tushare as ts
from Stocks import *
from HData_dailybasic import *
import  datetime

from time import clock
from file_interface import *

import pandas as pd

import sys
import os
from sys import argv
    
token='21dddafc47513ea46b89057b2c4edf7b44882b3e92274b431f199552'
pro = ts.pro_api(token)


debug = 0

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
        print("2", nowdate, hist_data.head(10))

if __name__ == '__main__':

    t1 = clock()
    
    script_name, para1 = check_input_parameter()

    hdata_dailybasic=HData_dailybasic("usr","usr")
    #hdata_dailybasic.db_hdata_date_create()
    nowdate=datetime.datetime.now().date()
    nowdate=nowdate-datetime.timedelta(int(para1))

    #check_work_day
    if get_work_day(nowdate) is 0:
        print('%s is holiday, exit' % nowdate.strftime("%Y-%m-%d") )
        exit(1)

    exist_df = hdata_dailybasic.get_day_hdata_of_stock(nowdate.strftime("%Y-%m-%d"))
    if len(exist_df): 
        print("%s data is already exist! " % nowdate.strftime("%Y-%m-%d") )
    else:
        hdata_dailybasic.db_connect()#由于每次连接数据库都要耗时0.0几秒，故获取历史数据时统一连接
        update_dailybasic(nowdate)
        hdata_dailybasic.db_disconnect()

    t2 = clock()
    print("t1:%s, t2:%s, delta=%s"%(t1, t2, t2-t1))
