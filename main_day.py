#!/usr/bin/env python  
# -*- coding: utf-8 -*-

import psycopg2 #使用的是PostgreSQL数据库
import tushare as ts
from Stocks import *
from HData_day import *
import  datetime

from time import clock

from common import Log


import pandas as pd

import sys
import os
    
token='21dddafc47513ea46b89057b2c4edf7b44882b3e92274b431f199552'
pro = ts.pro_api(token)


debug=1



stocks=Stocks("usr","usr")
hdata_day=HData_day("usr","usr")

# stocks.db_stocks_create()#如果还没有表则需要创建
print(stocks.db_stocks_update())#根据todayall的情况更新stocks表

#hdata_day.db_hdata_date_create()

nowdate=datetime.datetime.now().date()


codestock_local=stocks.get_codestock_local()
#print(codestock_local)

hdata_day.db_connect()#由于每次连接数据库都要耗时0.0几秒，故获取历史数据时统一连接
hdata_day.delete_data_of_day_stock(nowdate.strftime("%Y-%m-%d")) #delete first

t1 = clock()
length=len(codestock_local)


for i in range(0,length):
    nowcode=codestock_local[i][0]
    #nowcode='600485'

    
    maxdate=hdata_day.db_get_maxdate_of_stock(nowcode)
    if debug:
        print('maxdate:%s, nowdate:%s' % (maxdate, nowdate))
    if(maxdate):

        today_data=ts.get_today_all()
        today_data=today_data.drop_duplicates('code')
        if debug:
            today_data.head(1)
        del today_data['name']
        del today_data['settlement']
        del today_data['per']
        del today_data['pb']
        del today_data['mktcap']
        del today_data['nmc']
        del today_data['turnoverratio']

        if debug:
            print(today_data.head(1))

        #curr_day=datetime.datetime.now().strftime("%Y-%m-%d")
        curr_day=datetime.datetime.now().date()
        today_data['record_date']=curr_day
        cols=['record_date', 'code', 'open', 'trade', 'high', 'low', 'volume', 'amount', 'changepercent']
        today_data=today_data.ix[:,cols]
        today_data=today_data.set_index('record_date')
        if debug:
            print(today_data.head(1))
        #insert dataform
        hdata_day.insert_allstock_hdatadate(today_data)
        
        #delete closed stock data according amount=0
        hdata_day.delete_amount_is_zero()
        break

        '''
        curr_day=datetime.datetime.now().date()
        today_data = pro.daily(trade_date=curr_day.strftime("%Y%m%d"))
        del today_data['change']
        del today_data['pre_close']

        today_data=today_data.fillna(0)
        cols=['trade_date', 'ts_code', 'open', 'close', 'high', 'low', 'vol', 'amount', 'pct_chg']
        today_data=today_data.ix[:,cols]
        today_data['trade_date']=today_data['trade_date'].apply(lambda x: datetime.datetime.strptime(x,'%Y%m%d'))
        today_data['ts_code'] = today_data['ts_code'].apply(lambda x: x[0:6])
        today_data = today_data.set_index('trade_date')

        if debug:
            print(today_data.head(1))
    
        #insert datafram
        hdata_day.insert_allstock_hdatadate(today_data)
        
        #delete closed stock data according amount=0
        hdata_day.delete_amount_is_zero()
        '''

        break #only 1 time, then exit for loop



    else:#说明从未获取过这只股票的历史数据
        if nowcode[0:1] == '6':
            stock_code_new= nowcode + '.SH'
        else:
            stock_code_new= nowcode + '.SZ'

        '''
        df = ts.pro_bar(ts_code='603699.SH', start_date='20180101', end_date='20200205', adj='qfq', freq='D')
        hist_data=df.head(10)
        '''
        hist_data = ts.pro_bar(ts_code=stock_code_new, start_date='20150101', end_date=str(nowdate), adj='qfq', freq='D')

        if hist_data is None:
            if debug:
                print("hist_data is None: %d, %s, %s" % (i,nowcode,codestock_local[i][1]))
            continue

        if(len(hist_data) == 0):
            if debug:
                print("hist_data length is 0: i=%d, nowcode:%s, nowname:%s " %(i,nowcode,codestock_local[i][1]))
            continue

        hist_data=hist_data.fillna(0)


        #handle hist_data
        new_data =  pd.DataFrame()
        new_data['datetime'] = hist_data['trade_date']
        new_data['code']     = nowcode
        new_data['open']     = hist_data['open']
        new_data['close']    = hist_data['close']    
        new_data['high']     = hist_data['high']     
        new_data['low']      = hist_data['low']      
        new_data['vol']      = hist_data['vol']      
        new_data['amount']   = hist_data['amount']   
        new_data['p_change'] = hist_data['pct_chg'] 

        new_data['datetime']=new_data['datetime'].apply(lambda x: datetime.datetime.strptime(x,'%Y%m%d'))
        
        hist_data = new_data.set_index('datetime')

        if debug:
            print(hist_data.head(10))

        hdata_day.insert_allstock_hdatadate(hist_data)

        if debug:
            print("2", maxdate, nowdate, hist_data)


t2 = clock()
print("t1:%s, t2:%s, delta=%s"%(t1, t2, t2-t1))
hdata_day.db_disconnect()
