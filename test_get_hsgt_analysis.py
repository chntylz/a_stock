#!/usr/bin/env python
#coding:utf-8
import os,sys,gzip
import json


from file_interface import *


import psycopg2 #使用的是PostgreSQL数据库
import tushare as ts
import numpy as np

from HData_hsgt import *

import  datetime

#keep 0.01 accrucy
pd.set_option('display.float_format',lambda x : '%.2f' % x)

###################################################################################


debug=0
#debug=1


hdata_hsgt=HData_hsgt("usr","usr")
hdata_hsgt.db_connect()



###################################################################################

def hsgt_get_stock_list():
    df=hdata_hsgt.get_all_list_of_stock()
    if debug:
        print("df size is %d"% (len(df)))
    
    return df


def hsgt_get_all_data():
    df=hdata_hsgt.get_all_hdata_of_stock()
    if debug:
        print("df size is %d"% (len(df)))
    
    return df

def hsgt_analysis_data():
    all_df=hsgt_get_all_data()
    all_df["record_date"]=all_df["record_date"].apply(lambda x: str(x))
    latest_date=all_df.loc[0,'record_date']
    print(latest_date)

    all_df['delta1']  =all_df.groupby('stock_code')['percent'].apply(lambda i:i.diff(-1))
    all_df['delta2']  =all_df.groupby('stock_code')['percent'].apply(lambda i:i.diff(-2))
    all_df['delta3']  =all_df.groupby('stock_code')['percent'].apply(lambda i:i.diff(-3))
    all_df['delta5']  =all_df.groupby('stock_code')['percent'].apply(lambda i:i.diff(-5))
    all_df['delta10'] =all_df.groupby('stock_code')['percent'].apply(lambda i:i.diff(-10))
    all_df['delta21'] =all_df.groupby('stock_code')['percent'].apply(lambda i:i.diff(-21))
    all_df['delta120']=all_df.groupby('stock_code')['percent'].apply(lambda i:i.diff(-120))

    return all_df

    pass
            
def hsgt_get_daily_data(all_df):
    latest_date=all_df.loc[0,'record_date']
    daily_df=all_df[all_df['record_date'] == latest_date]
    return daily_df


def hsgt_daily_sort(daily_df, orderby='delta1'):
    sort_df=daily_df.sort_values(orderby, ascending=0)
    return sort_df;

###################################################################################
all_df =  hsgt_analysis_data()
daily_df = hsgt_get_daily_data(all_df)
delta1_df = hsgt_daily_sort(daily_df, 'delta1')
delta2_df = hsgt_daily_sort(daily_df, 'delta2')

print('day1')
print("%s"%(delta1_df.head(10)))
print('day2')
print("%s"%(delta2_df.head(10)))



hdata_hsgt.db_disconnect()
