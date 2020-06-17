#!/usr/bin/env python  
# -*- coding: utf-8 -*-

import psycopg2 #使用的是PostgreSQL数据库
import tushare as ts
from Stocks import *
from HData_day import *
from HData_divided import *
import  datetime

from time import clock

from common import Log

from comm_generate_web_html import handle_divided


import pandas as pd
import numpy as np

import sys
import os


from file_interface import *
    
token='21dddafc47513ea46b89057b2c4edf7b44882b3e92274b431f199552'
pro = ts.pro_api(token)


debug=0
#debug=1

yesterday=0

stocks=Stocks("usr","usr")
hdata_day=HData_day("usr","usr")
hdata_divided=HData_divided("usr","usr")

# stocks.db_stocks_create()#如果还没有表则需要创建
print(stocks.db_stocks_update())#根据todayall的情况更新stocks表

#hdata_day.db_hdata_date_create()


def get_daily_data(codestock_local, nowdate, div_df):

    length=len(codestock_local)

    #create a null dataframe
    #new_data =  pd.DataFrame()

    for i in range(0,length):
        nowcode=codestock_local[i][0]
        #nowcode='600485'

        is_divided = int(div_df[div_df['stock_code'] == nowcode]['is_divided'])

        if is_divided:
            if debug:
                print('nowcode=%s, nowdate:%s, delete first because of divided'% (nowcode, nowdate.strftime("%Y%m%d")))
            hdata_day.delete_data_of_stock_code(nowcode)

        
        maxdate=hdata_day.db_get_maxdate_of_stock(nowcode)
        if debug:
            print('maxdate:%s, nowdate:%s' % (maxdate, nowdate))

        if yesterday:
            maxdate = 0

        if(maxdate):


            '''
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
            '''
            #very late, it is about 16:00 to get valid data
            today_data = pro.daily(trade_date=nowdate.strftime("%Y%m%d"))
            del today_data['change']
            del today_data['pre_close']

            #df=df.ix[:,cols]语句表示，DataFrame的行索引不变，列索引是cols中给定的索引。
            today_data=today_data.fillna(0)
            cols=['trade_date', 'ts_code', 'open', 'close', 'high', 'low', 'vol', 'amount', 'pct_chg']
            #today_data=today_data.ix[:,cols]
            today_data=today_data.loc[:,cols]
            today_data['trade_date']=today_data['trade_date'].apply(lambda x: datetime.datetime.strptime(x,'%Y%m%d'))
            today_data['ts_code'] = today_data['ts_code'].apply(lambda x: x[0:6])
            today_data = today_data.set_index('trade_date')

            if debug:
                print(today_data.head(1))
        
            #insert datafram
            hdata_day.insert_allstock_hdatadate(today_data)
            
            #delete closed stock data according amount=0
            hdata_day.delete_amount_is_zero()

            break #only 1 time, then exit for loop
            '''

            df = ts.get_realtime_quotes(nowcode)
 
            #only current day
            df=df[df['date'] == nowdate.strftime("%Y-%m-%d")]

            if df is None:
                if debug:
                    print("df is None: %d, %s, %s" % (i,nowcode,codestock_local[i][1]))
                continue

            if(len(df) == 0):
                if debug:
                    print("df length is 0: i=%d, nowcode:%s, nowname:%s " %(i,nowcode,codestock_local[i][1]))
                continue

            if debug:
                print('before df=%s' % df)
        
               
            cols=['date', 'code', 'open', 'price', 'high', 'low', 'volume', 'amount', 'pre_close']
            #df=df.ix[:,cols]语句表示，DataFrame的行索引不变，列索引是cols中给定的索引。
            df=df.loc[:,cols]
            df['volume'] = float(df['volume'][0])/100
            df['amount'] = float(df['amount'][0])/1000

            df.rename(columns={'price': 'close'}, inplace=True)
            df['pre_close'] = (float(df['close'][0]) - float( df['pre_close'][0]) ) * 100 / float(df['pre_close'])

            df.rename(columns={'pre_close': 'pct_chg'}, inplace=True)
            df=df.round(2)

            df['date']=df['date'].apply(lambda x: datetime.datetime.strptime(x,'%Y-%m-%d'))
            df = df.set_index('date')

            if debug:
                print('before df=%s' % df)
        
            #insert datafram
            hdata_day.insert_allstock_hdatadate(df)
            
            
            '''
            new_data=pd.concat([new_data, df],ignore_index=True)
            if debug:
                print('len(new_data)= %d' % len(new_data))
            '''

        else:#说明从未获取过这只股票的历史数据
            if nowcode[0:1] == '6':
                stock_code_new= nowcode + '.SH'
            else:
                stock_code_new= nowcode + '.SZ'

            '''
            df = ts.pro_bar(ts_code='603699.SH', start_date='20180101', end_date='20200205', adj='qfq', freq='D')
            hist_data=df.head(10)
            '''
            if yesterday:
                s_date =  (nowdate-datetime.timedelta(1)).strftime("%Y%m%d")
                e_date =  nowdate.strftime("%Y%m%d")
            else:
                s_date =  '20150101'
                e_date =  nowdate.strftime("%Y%m%d")


            #hist_data = ts.pro_bar(ts_code=stock_code_new, start_date='20200422', end_date=str(nowdate), adj='qfq', freq='D')
            hist_data = ts.pro_bar(ts_code=stock_code_new, start_date=s_date, end_date=e_date, adj='qfq', freq='D')

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




if __name__ == '__main__':
    
    
    cript_name, para1 = check_input_parameter()

    t1 = clock()
    start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    nowdate=datetime.datetime.now().date()
    nowdate=nowdate-datetime.timedelta(int(para1))
    print("nowdate is %s"%(nowdate.strftime("%Y-%m-%d")))


    codestock_local=stocks.get_codestock_local()

    hdata_day.db_connect()#由于每次连接数据库都要耗时0.0几秒，故获取历史数据时统一连接
    hdata_day.delete_data_of_day_stock(nowdate.strftime("%Y-%m-%d")) #delete first


    div_df = hdata_divided.get_data_from_hdata()
    get_daily_data(codestock_local, nowdate, div_df)

    #delete closed stock data according amount=0
    hdata_day.delete_amount_is_zero()

    last_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print("start_time: %s, last_time: %s" % (start_time, last_time))


    t2 = clock()
    print("t1:%s, t2:%s, delta=%s"%(t1, t2, t2-t1))
    hdata_day.db_disconnect()
