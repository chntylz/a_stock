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
print(stocks.db_stocks_update())#根据todayall的情况更新stocks表
today_all = stocks.todayall
#print(today_all)

#hdata_day.db_hdata_date_create()

nowdate=datetime.datetime.now().date()


codestock_local=stocks.get_codestock_local()
#print(codestock_local)

hdata_day.db_connect()#由于每次连接数据库都要耗时0.0几秒，故获取历史数据时统一连接
hdata_day.delete_data_of_day_stock(nowdate.strftime("%Y-%m-%d")) #delete first

t1 = clock()
length=len(codestock_local)
cons = ts.get_apis()

data_flag = False

for i in range(0,length):
    nowcode=codestock_local[i][0]
    #nowcode='600485'

    #print(hdata_day.get_all_hdata_of_stock(nowcode))
    # print("%d,%s,%s"% (i, nowcode,codestock_local[i][1]))
    #log.warning("i=%d,%s,%s\n",  i,nowcode,codestock_local[i][1])

    
    maxdate=hdata_day.db_get_maxdate_of_stock(nowcode)
    # print('maxdate:%s, nowdate:%s' % (maxdate, nowdate))
    if(maxdate):
        '''
        #print("c1: %s" %(nowcode))
        if(maxdate>=nowdate):#maxdate小的时候说明还有最新的数据没放进去
            continue
        #hist_data=ts.get_k_data(nowcode, str(maxdate+datetime.timedelta(1)),str(nowdate), 'D', 'hfq', False, 3, 0.001)
        hist_data=ts.bar(nowcode, conn=cons, freq='D', adj='qfq', start_date=str(maxdate+datetime.timedelta(1)), end_date=str(nowdate))
        # print(hist_data.head(1))

        if hist_data is None:
            # print("hist_data is None: %d, %s, %s" % (i,nowcode,codestock_local[i][1]))
            continue

        if(len(hist_data) == 0):
            # print("hist_data length is 0: i=%d, nowcode:%s, nowname:%s " %(i,nowcode,codestock_local[i][1]))
            continue

        #fix bug: skip suspend stock, because get_bar can get data when the stock is supsend(volume < 0)
        if hist_data.iloc[0][5] < 0.0001 :
            #print("stock suspend: %s" %(nowcode))
            continue

        hist_data=hist_data.fillna(0)
          
        if data_flag is False:
            g_df = hist_data
            data_flag = True
        else:
            g_df = g_df.append(hist_data)

        #hist_data = hist_data.set_index('date')
        # print(g_df.head(10))
        # print (len(g_df))
        # print("1", maxdate, nowdate, hist_data, g_df)
        # hdata_day.insert_perstock_hdatadate(nowcode, hist_data)
        if len(g_df)%1000 == 0:
            hdata_day.insert_allstock_hdatadate(nowcode, g_df)
            data_flag = False

        '''
        #today_data=ts.get_today_all()
        today_data=today_all
        today_data=today_data.drop_duplicates('code')
        today_data.head(1)
        del today_data['name']
        del today_data['settlement']
        del today_data['per']
        del today_data['pb']
        del today_data['mktcap']
        del today_data['nmc']
        del today_data['turnoverratio']
        today_data.head(1)
        # curr_day=datetime.datetime.now().strftime("%Y-%m-%d")
        curr_day=datetime.datetime.now().date()
        today_data['record_date']=curr_day
        cols=['record_date', 'code', 'open', 'trade', 'high', 'low', 'volume', 'amount', 'changepercent']
        today_data=today_data.ix[:,cols]
        today_data=today_data.set_index('record_date')
        today_data.head(1)
        
        hdata_day.insert_allstock_hdatadate(nowcode, today_data)
        break



    else:#说明从未获取过这只股票的历史数据
        #print("c2: %s" %(nowcode))
        #hist_data = ts.get_k_data(nowcode, '2019-06-11', '2019-06-13', '30', 'qfq', False, 3, 0.001)
        #hist_data = ts.get_k_data(nowcode, '2018-06-01', str(nowdate), 'D', 'qfq', False, 3, 0.001)
        #hist_data = ts.get_k_data(nowcode, '2018-06-01', str(nowdate), 'D', 'qfq', False, 3, 0.001)
        
        hist_data=ts.bar(nowcode, conn=cons, freq='D', adj='qfq', start_date='2018-06-01', end_date=str(nowdate))
        #hist_data=ts.bar(nowcode, conn=cons, freq='D', adj='qfq', start_date='2019-08-18', end_date=str(nowdate))
       
        if hist_data is None:
            # print("hist_data is None: %d, %s, %s" % (i,nowcode,codestock_local[i][1]))
            continue

        if(len(hist_data) == 0):
            # print("hist_data length is 0: i=%d, nowcode:%s, nowname:%s " %(i,nowcode,codestock_local[i][1]))
            continue

        hist_data=hist_data.fillna(0)
        #hist_data = hist_data.set_index('date')
        #print(hist_data.head(10))
        hdata_day.insert_allstock_hdatadate(nowcode, hist_data)
        #print("2", maxdate, nowdate, hist_data)


t2 = clock()
print("t1:%s, t2:%s, delta=%s"%(t1, t2, t2-t1))
print(i)
ts.close_apis(cons)
hdata_day.db_disconnect()
print(i)
sys.exit()
