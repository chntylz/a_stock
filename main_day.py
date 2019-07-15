#!/usr/bin/env python  
# -*- coding: utf-8 -*-

import psycopg2 #使用的是PostgreSQL数据库
import tushare as ts
from Stocks import *
from HData_day import *
import  datetime

from time import clock

from common import Log

import sys
import os
    
log = Log(__name__).getlog()
log.info("I am main_day.py")

stocks=Stocks("usr","usr")
hdata_day=HData_day("usr","usr")

# stocks.db_stocks_create()#如果还没有表则需要创建
#print(stocks.db_stocks_update())#根据todayall的情况更新stocks表
#hdata_day.db_hdata_date_create()

nowdate=datetime.datetime.now().date()

codestock_local=stocks.get_codestock_local()
#print(codestock_local)

hdata_day.db_connect()#由于每次连接数据库都要耗时0.0几秒，故获取历史数据时统一连接
t1 = clock()
lenth=len(codestock_local)
cons = ts.get_apis()

for i in range(0,lenth):
#for i in range(0,4000):
    nowcode=codestock_local[i][0]
    #nowcode='600485'

    #print(hdata_day.get_all_hdata_of_stock(nowcode))
    # print("%d,%s,%s"% (i, nowcode,codestock_local[i][1]))
    #log.warning("i=%d,%s,%s\n",  i,nowcode,codestock_local[i][1])

    
    maxdate=hdata_day.db_get_maxdate_of_stock(nowcode)
    #print('maxdate:%s, nowdate:%s' % (maxdate, nowdate))
    if(maxdate):
        #print("c1: %s" %(nowcode))
        if(maxdate>=nowdate):#maxdate小的时候说明还有最新的数据没放进去
            continue
        #hist_data=ts.get_k_data(nowcode, str(maxdate+datetime.timedelta(1)),str(nowdate), 'D', 'hfq', False, 3, 0.001)
        hist_data=ts.bar(nowcode, conn=cons, freq='D', adj='qfq', start_date=str(maxdate+datetime.timedelta(1)), end_date=str(nowdate))
        #print(hist_data.head(1))

        if hist_data is None:
            # print("hist_data is None: %d, %s, %s" % (i,nowcode,codestock_local[i][1]))
            continue

        if(len(hist_data) == 0):
            # print("hist_data length is 0: i=%d, nowcode:%s, nowname:%s " %(i,nowcode,codestock_local[i][1]))
            continue

        #fix bug: skip suspend stock, because get_bar can get data when the stock is supsend(volume < 0)
        if hist_data.iloc[0][5] < 0.0001 :
            print("stock suspend: %s" %(nowcode))
            continue

        hist_data=hist_data.fillna(0)
        #hist_data = hist_data.set_index('date')
        #print(hist_data.head(10))
        #print("1", maxdate, nowdate, hist_data)
        hdata_day.insert_perstock_hdatadate(nowcode, hist_data)
    else:#说明从未获取过这只股票的历史数据
        #print("c2: %s" %(nowcode))
        #hist_data = ts.get_k_data(nowcode, '2019-06-11', '2019-06-13', '30', 'qfq', False, 3, 0.001)
        #hist_data = ts.get_k_data(nowcode, '2018-06-01', str(nowdate), 'D', 'qfq', False, 3, 0.001)
        #hist_data = ts.get_k_data(nowcode, '2018-06-01', str(nowdate), 'D', 'qfq', False, 3, 0.001)
        hist_data=ts.bar(nowcode, conn=cons, freq='D', adj='qfq', start_date='2018-06-01', end_date=str(nowdate))
       
        if hist_data is None:
            # print("hist_data is None: %d, %s, %s" % (i,nowcode,codestock_local[i][1]))
            continue

        if(len(hist_data) == 0):
            # print("hist_data length is 0: i=%d, nowcode:%s, nowname:%s " %(i,nowcode,codestock_local[i][1]))
            continue

        hist_data=hist_data.fillna(0)
        #hist_data = hist_data.set_index('date')
        #print(hist_data.head(10))
        hdata_day.insert_perstock_hdatadate(nowcode, hist_data)
        #print("2", maxdate, nowdate, hist_data)

t2 = clock()
print("t1:%s, t2:%s, delta=%s"%(t1, t2, t2-t1))
print(i)
ts.close_apis(cons)
hdata_day.db_disconnect()
print(i)
sys.exit()
