#!/usr/bin/env python  
# -*- coding: utf-8 -*-

import psycopg2 #使用的是PostgreSQL数据库
import tushare as ts
from Stocks import*
from HData import*
import  datetime

stocks=Stocks("usr","usr")
hdata=HData("usr","usr")

# stocks.db_stocks_create()#如果还没有表则需要创建
#print(stocks.db_stocks_update())#根据todayall的情况更新stocks表

#hdata.db_hdata_date_create()

nowdate=datetime.datetime.now().date()

codestock_local=stocks.get_codestock_local()

st_list = [] 
st_number=len(codestock_local)
print ('st_number:' + str(st_number))
hdata.db_connect()#由于每次连接数据库都要耗时0.0几秒，故获取历史数据时统一连接

for i in range(0,st_number):
    nowcode=codestock_local[i][0]

    st_list.append(nowcode);
    #print(hdata.get_all_hdata_of_stock(nowcode))

#print(nowcode, i, codestock_local[i][1])
    
#print(codestock_local[i][1])

#print st_list

time1=datetime.datetime.now()
data1=ts.get_realtime_quotes(st_list[0:880])
data2=ts.get_realtime_quotes(st_list[881:1760])
data3=ts.get_realtime_quotes(st_list[1761:2640])
data4=ts.get_realtime_quotes(st_list[2641:3520])
data5=ts.get_realtime_quotes(st_list[3521:3612])
time2=datetime.datetime.now()
print('开始时间：'+str(time1))
print('结束时间：'+str(time2))

hdata.db_disconnect()
