#!/usr/bin/env python  
# -*- coding: utf-8 -*-

import psycopg2 #使用的是PostgreSQL数据库
import tushare as ts
from Stocks import *
from HData_holder import *
import  time,datetime

from time import clock

from file_interface import *

import pandas as pd

import sys
import os
    
token='21dddafc47513ea46b89057b2c4edf7b44882b3e92274b431f199552'
pro = ts.pro_api(token)


debug=0
#debug=1


def update_holder(nowdate):
    codestock_local=stocks.get_codestock_local()
    if debug:
        print(codestock_local)

    length=len(codestock_local)


    for i in range(0,length):
        nowcode=codestock_local[i][0]

        maxdate=hdata_holder.db_get_maxdate_of_stock(nowcode)
        if debug:
            print('maxdate:%s, nowdate:%s' % (maxdate, nowdate))
       
        #get start date
        if(maxdate):
            start_date=(maxdate + datetime.timedelta(1)).strftime("%Y%m%d")
        else:#说明从未获取过这只股票的历史数据
            start_date='20180101'

        #get end date
        end_date=nowdate.strftime("%Y%m%d")

        #get stock_code
        if nowcode[0:1] == '6':
            stock_code_new= nowcode + '.SH'
        else:
            stock_code_new= nowcode + '.SZ'

        if debug:
            print('stock_code_new:%s, start_date:%s, end_date:%s' % (stock_code_new, start_date, end_date))

        #get data
        hist_data = pro.stk_holdernumber(ts_code=stock_code_new, start_date=start_date, end_date=end_date)
        
        time.sleep(0.65) #fix bug: 抱歉，您每分钟最多访问该接口100次

        if hist_data is None:
            if debug:
                print("hist_data is None: %d, %s, %s" % (i,nowcode,codestock_local[i][1]))
            continue

        if(len(hist_data) == 0):
            if debug:
                print("hist_data length is 0: i=%d, nowcode:%s, nowname:%s " %(i,nowcode,codestock_local[i][1]))
            continue

        hist_data=hist_data.fillna(0)

        hist_data=hist_data.drop_duplicates(subset='end_date', keep='first', inplace=False)  #delete end_date line with the same data

        #handle hist_data
        new_data =  pd.DataFrame()
        new_data['record_date'] = hist_data['end_date']
        new_data['stock_code']     = nowcode
        new_data['ann_date']     = hist_data['ann_date']
        new_data['end_date']    = hist_data['end_date']    
        new_data['holder_num']     = hist_data['holder_num']     

        new_data['record_date']=new_data['record_date'].apply(lambda x: datetime.datetime.strptime(x,'%Y%m%d'))
        
        hist_data = new_data.set_index('record_date')

        if debug:
            print(hist_data.head(10))

        hdata_holder.insert_allstock_hdatadate(hist_data)

        if debug:
            print("2", maxdate, nowdate, hist_data)

if __name__ == '__main__':
    
    t1 = clock()
    
    nowdate=datetime.datetime.now().date()

    stocks=Stocks("usr","usr")
    hdata_holder=HData_holder("usr","usr")

    #hdata_holder.db_hdata_date_create()

    hdata_holder.db_connect()#由于每次连接数据库都要耗时0.0几秒，故获取历史数据时统一连接

    update_holder(nowdate)

    t2 = clock()

    print("t1:%s, t2:%s, delta=%s"%(t1, t2, t2-t1))
    hdata_holder.db_disconnect()
