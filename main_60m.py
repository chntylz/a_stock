#!/usr/bin/env python  
# -*- coding: utf-8 -*-

import psycopg2 #使用的是PostgreSQL数据库
import tushare as ts
from Stocks import *
from HData_60m import *
import  datetime

from time import clock

from common import Log

log = Log(__name__).getlog()
log.info("I am main.py")

stocks=Stocks("usr","usr")
hdata_60m=HData_60m("usr","usr")

# stocks.db_stocks_create()#如果还没有表则需要创建
#print(stocks.db_stocks_update())#根据todayall的情况更新stocks表

hdata_60m.db_hdata_date_create()

nowdate=datetime.datetime.now().date()

codestock_local=stocks.get_codestock_local()
#print(codestock_local)

hdata_60m.db_connect()#由于每次连接数据库都要耗时0.0几秒，故获取历史数据时统一连接
t1 = clock()
lenth=len(codestock_local)
for i in range(0,lenth):
#for i in range(0,4000):
    nowcode=codestock_local[i][0]
    #nowcode='600485'

    #print(hdata_60m.get_all_hdata_of_stock(nowcode))
    #print(i,nowcode,codestock_local[i][1])
    #log.warning("i=%d,%s,%s\n",  i,nowcode,codestock_local[i][1])

    maxdate=hdata_60m.db_get_maxdate_of_stock(nowcode)
    #print('maxdate:%s, nowdate:%s' % (maxdate, nowdate))
    if(maxdate):
        if(maxdate>=nowdate):#maxdate小的时候说明还有最新的数据没放进去
            continue
        hist_data=ts.get_k_data(nowcode, str(maxdate+datetime.timedelta(1)),str(nowdate), '60', 'hfq', False, 3, 0.001)
        '''
        if(len(hist_data) == 0):
            #print("data update is null: i=%d, nowcode:%s, nowname:%s " %(i,nowcode,codestock_local[i][1]))
            continue
        else:
        '''
        if(len(hist_data) != 0):
            hist_data = hist_data.set_index('date')
            #print(hist_data.head(10))
            #print("1", maxdate, nowdate, hist_data)
            hdata_60m.insert_perstock_hdatadate(nowcode, hist_data)
    else:#说明从未获取过这只股票的历史数据
        #hist_data = ts.get_k_data(nowcode, '2019-06-11', '2019-06-13', '30', 'qfq', False, 3, 0.001)
        hist_data = ts.get_k_data(nowcode, '2018-06-01', str(nowdate), '60', 'qfq', False, 3, 0.001)
        '''
        if(len(hist_data) == 0):
            print("data create is null: i=%d, nowcode:%s, nowname:%s " %(i,nowcode,codestock_local[i][1]))
        else:
        '''
        if(len(hist_data) != 0):
            hist_data = hist_data.set_index('date')
            #print(hist_data.head(10))
            hdata_60m.insert_perstock_hdatadate(nowcode, hist_data)
            #print("2", maxdate, nowdate, hist_data)

t2 = clock()
print("t1:%s, t2:%s, delta=%s"%(t1, t2, t2-t1))
print(i)
hdata_60m.db_disconnect()
