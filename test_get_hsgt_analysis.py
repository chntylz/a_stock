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


###################################################################################


debug=False
#debug=True


hdata_hsgt=HData_hsgt("usr","usr")
#hdata_hsgt.db_hdata_date_create()
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
    list_df=hsgt_get_stock_list()
    all_df=hsgt_get_all_data()
    latest_date=all_df.loc[0,'record_date'].strftime("%Y-%m-%d")
    print(latest_date)
    
    list_df_size = len(list_df)
    list_tmp=[]
    
    
    for i in range(0, list_df_size):
        _stock_code = list_df.loc[i, 'stock_code']
        #print('stock_code:%s'%(_stock_code))
        
        each_stock_data_df = all_df[all_df['stock_code'] == _stock_code]
        each_stock_data_df.reset_index(drop=True, inplace=True) # reset index
        
        percent0 = each_stock_data_df.loc[0, 'percent']
        percent1 = each_stock_data_df.loc[0+1, 'percent']
        percent2 = each_stock_data_df.loc[0+2, 'percent']
        percent3 = each_stock_data_df.loc[0+3, 'percent']
        percent5 = each_stock_data_df.loc[0+5, 'percent']
        percent10 = each_stock_data_df.loc[0+10, 'percent']
        percent21 = each_stock_data_df.loc[0+21, 'percent']
        
        delta1=round(percent0-percent1, 2)
        delta2=round(percent0-percent2, 2)
        delta3=round(percent0-percent3, 2)
        delta5=round(percent0-percent5, 2)
        delta10=round(percent0-percent10, 2)
        delta21=round(percent0-percent21, 2)
        
        #print('p0=%f, p1=%f, p2=%f, p3=%f, p5=%f, p10=%f, p21=%f'%(percent0, percent1, percent2, percent3, percent5, percent10, percent21))
        #print('delta1=%f, delta2=%f, delta3=%f, delta5=%f, delta10=%f, delta21=%f '%(delta1, delta2, delta3, delta5, delta10, delta21))
        
        each_list_tmp=each_stock_data_df[0:1].values.tolist()
        each_list_tmp[0][0]=each_list_tmp[0][0].strftime("%Y-%m-%d") #date -> string
        if debug:
            print(each_list_tmp)
        
        each_list_tmp[0].extend([ delta1,delta2, delta3, delta5, delta10, delta21])
        if debug:
            print(each_list_tmp)
        
        if latest_date ==  each_list_tmp[0][0]:
            list_tmp.extend(each_list_tmp)
            
        if debug:
            print(list_tmp)
            if i > 2:
                break
        
    dataframe_cols = ['record_date', 'stock_code', 'share_holding', 'percent', 'day1', 'day2', 'day3', 'day5', 'day10', 'day21']            
    df= pd.DataFrame(list_tmp, columns=dataframe_cols)
    print(df)
    
    return df
    
    pass
            

###################################################################################


hsgt_analysis_data()

hdata_hsgt.db_disconnect()
