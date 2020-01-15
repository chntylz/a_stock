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
import time

stocks=Stocks("usr","usr")
hdata_fina=HData_fina("usr","usr")
debug = False
   
def set_fina_data():
    nowdate=datetime.datetime.now().date()


    codestock_local=stocks.get_codestock_local()
    #print(codestock_local)


    length=len(codestock_local)


    for i in range(0,length):
    #for i in range(0, 1):
        print("----------------------------------------------------------")
        
        
        nowcode=codestock_local[i][0]
        #nowcode='000401'

        #add SH or SZ because fina interface request
        if nowcode[0:1] == '6':
            nowcode_new=nowcode + '.SH'
        else:
            nowcode_new=nowcode + '.SZ'

        if debug:
            print("%d,%s,%s"% (i, nowcode,codestock_local[i][1]))

        
        maxdate=hdata_fina.db_get_maxdate_of_stock(nowcode)
        print('maxdate:%s, nowdate:%s' % (maxdate, nowdate))


        if(maxdate):
            continue
            print("%s: fina is already exist!" % nowcode)
            newdate=datetime.datetime.strptime(maxdate,'%Y%m%d')
            newdate=newdate + datetime.timedelta(1)
            fina_data = pro.query('fina_indicator', ts_code=nowcode_new, start_date=newdate.strftime("%Y%m%d"), end_date=nowdate.strftime("%Y%m%d"))

            #add delay for 80/min limitation
            time.sleep(0.5)

        else:#说明从未获取过这只股票的历史数据
            print("%s: fina is null" % nowcode)
            fina_data= pro.fina_indicator(ts_code=nowcode_new)

            #add delay for 80/min limitation
            time.sleep(0.5)
       
        if fina_data is None:
            continue

        if(len(fina_data) == 0):
            continue

        fina_data=fina_data.fillna(0)
       
        if debug:
            print("ts_code:%s" % fina_data['ts_code'][1])

        fina_data['ts_code']= str(nowcode) 

        if debug:
            print("ts_code:%s" % fina_data['ts_code'][1])


        if debug:
            print(fina_data.head(10))
        #insert to hdata_fina_table
        hdata_fina.insert_allstock_hdatadate(nowcode, fina_data)
        

    return fina_data


if __name__ == '__main__':

    t1 = clock()
    #set token
    token='21dddafc47513ea46b89057b2c4edf7b44882b3e92274b431f199552'
    pro = ts.pro_api(token)


    hdata_fina.db_connect()#由于每次连接数据库都要耗时0.0几秒，故获取历史数据时统一连接

    #hdata_fina.db_hdata_fina_create()#如果还没有表则需要创建
    df =  set_fina_data()
    t2 = clock()
    print("t1:%s, t2:%s, delta=%s"%(t1, t2, t2-t1))
    hdata_fina.db_disconnect()


