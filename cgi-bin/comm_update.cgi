#!/usr/bin/env python
# -*- coding: utf-8 -*- 
import os,sys,time, datetime
import cgi

import psycopg2 #使用的是PostgreSQL数据库
import tushare as ts
import numpy as np
import pandas as pd

from HData_hsgt import *
hdata_hsgt=HData_hsgt("usr","usr")

from comm_generate_html import *

debug=0

nowdate=datetime.datetime.now().date()
str_date= nowdate.strftime("%Y-%m-%d")

def get_stock_info(file_name):
    stock_list = []
    with open(file_name) as f:
        for line in f:
            if debug:
                print (line, len(line))
            if len(line) < 6 or '#' in line:
                if debug:
                    print('unvalid line data, skip!')
                continue
            space_pos = line.rfind(' ')
            stock_list.append([line[0:space_pos], line[space_pos+1: -1]])

    return stock_list



def show_realdata():
    #my_list=['300750','300552', '000401', '300458','300014', '601958', '601117', '600588', '002230']
    #my_list_cn=['ningdeshidai','wanjikeji', 'jidongshuini', 'quanzhikeji', 'yiweilineng', 'jinmugufen', 'zhongguohuaxue', 'yongyouwangluo', 'kedaxunfei']

    data_list = []

    file_name = 'my_optional.txt'
    my_list = get_stock_info(file_name)
    if debug:
        print(my_list)

    for i in range(len(my_list)):

        new_date        = str_date
        new_code        = my_list[i][0]
        new_name        = my_list[i][1]
        if debug:
            print("new_code:%s" % new_code)
        
        df = ts.get_realtime_quotes(new_code)
        new_pre_price   = df['pre_close'][0]
        new_price       = df['price'][0]
        new_percent     = ((float(new_price) - float(new_pre_price)) / float(new_pre_price)) * 100
        new_percent     = round (new_percent, 2)
       
        hsgt_df = hdata_hsgt.get_data_from_hdata(stock_code=new_code, limit=60)
        

        new_hsgt_date, new_hsgt_share_holding, new_hsgt_percent, new_hsgt_delta1, new_hsgt_deltam, p_count, money_total = comm_handle_hsgt_data(hsgt_df)
        
        data_list.append([new_date, new_code, new_name, new_pre_price, new_price, new_percent, new_hsgt_date, new_hsgt_share_holding, new_hsgt_percent, new_hsgt_delta1, new_hsgt_deltam, p_count, money_total])


        #data_list.append([str_date, my_list[i], my_list_cn[i], df['pre_close'][0], df['price'][0] ])

    data_column = ['curr_date', 'code', 'name', 'pre_price', 'price', 'a_pct', 'hk_date', 'hk_share_holding', 'hk_pct', 'hk_delta1', 'hk_deltam', 'conti_day', 'hk_m_total' ]
    ret_df=pd.DataFrame(data_list, columns=data_column)
    ret_df['m_per_day'] = ret_df.hk_m_total / ret_df.conti_day
    ret_df = ret_df.fillna(0)
    ret_df=ret_df.round(2)

 
    return ret_df

   
def cgi_generate_html(df):
    cgi_handle_html_head('comm_update', refresh=1)
    cgi_handle_html_body(df)
    cgi_handle_html_end()
    
    
    

if __name__ == '__main__':

    df=show_realdata()
    if debug:
        print(df)

    cgi_generate_html(df)
