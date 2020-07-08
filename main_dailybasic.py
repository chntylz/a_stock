#!/usr/bin/env python  
# -*- coding: utf-8 -*-

import psycopg2 #使用的是PostgreSQL数据库
import tushare as ts
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
    if is_work_day(nowdate) :
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



#######################################
#test
'''

算法： 
选出市值小于50亿

select A.record_date, A.stock_code, A.close, A.pe, A.pb, A.total_mv, A.circ_mv from hdata_dailybasic as A where A.record_date='2020-03-20' and A.total_mv < 500000;

select B.cns_name, B.area, B.industry, B.list_date from stocks as B;

select A.record_date, A.stock_code, A.close, A.pe, A.pb, A.total_mv, A.circ_mv, B.cns_name, B.area, B.industry, B.list_date from hdata_dailybasic as A,  stocks as B where A.stock_code = B.stock_code and  A.record_date='2020-03-20' and A.total_mv < 500000 limit 10; 

两个表连起来：
select * from (
            select A.record_date as date, A.stock_code, A.close, A.pe, A.pb, A.total_mv, A.circ_mv, B.cns_name, B.area, B.industry, B.list_date from hdata_dailybasic as A,  stocks as B where A.stock_code = B.stock_code and A.record_date='2020-03-20') as tbl;

查行业信息
select industry, count(*) from stocks group by industry  order by count desc;

select * from (
    select industry, count(*) from stocks group by industry  order by count desc ) as tlb  
    where industry = ' 软件服务' or industry = '通信设备' or industry = '元器件' or industry='通信设备' or industry='半导体' or industry='生物制药';


final:
select * from ( 
        select A.record_date as date, A.stock_code, A.close, A.pe, A.pb, A.total_mv, A.circ_mv, B.cns_name, B.area, B.industry, B.list_date from hdata_dailybasic as A,  stocks as B where A.stock_code = B.stock_code and A.record_date='2020-03-20') as tbl
        where total_mv < 500000 and (
        industry = ' 软件服务' or 
        industry = '通信设备' or 
        /* industry = '元器件' or */
        industry = '通信设备' or 
        industry = '半导体' or 
        industry = '生物制药');
'''
