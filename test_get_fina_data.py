#!/usr/bin/env python  
# -*- coding: utf-8 -*-

import psycopg2 #使用的是PostgreSQL数据库
import tushare as ts
from Stocks import *
from HData_fina import *
import  datetime

from time import clock


import sys
import os

#set token
token='21dddafc47513ea46b89057b2c4edf7b44882b3e92274b431f199552'
pro = ts.pro_api(token)
    

stocks=Stocks("usr","usr")
hdata_fina=HData_fina("usr","usr")


hdata_fina.db_hdata_fina_create()#如果还没有表则需要创建

nowdate=datetime.datetime.now().date()


codestock_local=stocks.get_codestock_local()
#print(codestock_local)

hdata_fina.db_connect()#由于每次连接数据库都要耗时0.0几秒，故获取历史数据时统一连接

t1 = clock()
length=len(codestock_local)

data_flag = False

for i in range(0,length):
#for i in range(0, 1):
    print("----------------------------------------------------------")
    nowcode=codestock_local[i][0]
    #nowcode='600485'

    if nowcode[0:1] == '6':
        nowcode_new=nowcode + '.SH'
    else:
        nowcode_new=nowcode + '.SZ'



    #print("%d,%s,%s"% (i, nowcode,codestock_local[i][1]))

    
    maxdate=hdata_fina.db_get_maxdate_of_stock(nowcode)
    print('maxdate:%s, nowdate:%s' % (maxdate, nowdate))

    if(maxdate):
        print("%s: fina is already exist!" % nowcode)
        newdate=datetime.datetime.strptime(maxdate,'%Y%m%d')
        newdate=newdate + datetime.timedelta(1)
        fina_data = pro.query('fina_indicator', ts_code=nowcode_new, start_date=newdate.strftime("%Y%m%d"), end_date=nowdate.strftime("%Y%m%d"))
    else:#说明从未获取过这只股票的历史数据
        print("%s: fina is null" % nowcode)
        fina_data= pro.fina_indicator(ts_code=nowcode_new)
   
    if fina_data is None:
        continue

    if(len(fina_data) == 0):
        continue

    fina_data=fina_data.fillna(0)
    fina_data['ts_code']=nowcode


    #print(fina_data.head(10))
    hdata_fina.insert_allstock_hdatadate(nowcode, fina_data)


t2 = clock()
print("t1:%s, t2:%s, delta=%s"%(t1, t2, t2-t1))
hdata_fina.db_disconnect()
print(i)
sys.exit()


