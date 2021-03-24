#!/usr/bin/env python3
# -*- coding: utf-8 -*- 
import os,sys,time, datetime
import cgi

import psycopg2 
import tushare as ts
import numpy as np
import pandas as pd

from HData_hsgt import *
from HData_xq_fina import *
from HData_xq_holder import *
hdata_hsgt=HData_hsgt("usr","usr")
hdata_fina=HData_xq_fina("usr","usr")
hdata_holder=HData_xq_holder("usr","usr")

from comm_generate_html import *

from HData_eastmoney_zlje import *
from HData_eastmoney_zlje_3 import *
from HData_eastmoney_zlje_5 import *
from HData_eastmoney_zlje_10 import *

from get_daily_zlje import *
from test_get_basic_data import *


debug=0


nowdate=datetime.datetime.now().date()
str_date= nowdate.strftime("%Y-%m-%d")

def is_work_time():
    ret = False
    s_time = datetime.datetime.strptime(str(datetime.datetime.now().date())+'9:25', '%Y-%m-%d%H:%M')
    e_time = datetime.datetime.strptime(str(datetime.datetime.now().date())+'15:10', '%Y-%m-%d%H:%M')

    n_time = datetime.datetime.now()

    if n_time > s_time and n_time < e_time:
        ret = True
    else:
        ret = False

    return ret



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
    length=len(my_list)
   
    real_zlje_df = get_daily_zlje()
    real_zlje_df = handle_raw_data(real_zlje_df)
    
    ####get zlje start####
    zlje_df   = get_zlje_data_from_db(url='url',     curr_date=str_date)
    zlje_3_df = get_zlje_data_from_db(url='url_3',   curr_date=str_date)
    zlje_5_df = get_zlje_data_from_db(url='url_5',   curr_date=str_date)
    zlje_10_df = get_zlje_data_from_db(url='url_10', curr_date=str_date)
    ####get zlje end####
    i = 0
    eastmoney_begin = 10
    for i in range(length):
        new_date        = str_date
        new_date        = str_date[2:]
        stock_code_new  = my_list[i][0]
        new_code        = stock_code_new[2:]
        new_name        = my_list[i][1]
        if debug:
            print("i=%d,  new_code:%s" %(i, new_code))

        new_pre_price = new_price = new_percent = 0
        
        if new_name == '5GETF':
            eastmoney_begin = i

        if i > eastmoney_begin:
            if debug: 
                print('use eastmoney data')

            real_df = real_zlje_df[real_zlje_df['code'] == new_code]
            if len(real_df):
                real_df = real_df.reset_index(drop=True)
                new_price       = real_df['zxj'][0]
                new_percent     = real_df['zdf'][0]
                new_pre_price   = round(new_price/(1+(new_percent/100)), 2)
        else:
            if debug: 
                print('use snowball data')

            real_df  =  get_real_data(stock_code_new)
            if len(real_df):
                new_pre_price   = real_df['last_close'][0]
                new_price       = real_df['current'][0]
                new_percent     = real_df['percent'][0]


        '''
        real_df  =  get_his_data(stock_code_new, def_cnt=2)
        if len(real_df) > 1:
            new_pre_price   = real_df['close'][0]
            new_price       = real_df['close'][1]
            new_percent     = real_df['percent'][1]
        elif len(real_df) > 0:
            new_pre_price   = 0
            new_price       = real_df['close'][0]
            new_percent     = real_df['percent'][0]
        '''
      
        hsgt_df = hdata_hsgt.get_data_from_hdata(stock_code=new_code, limit=60)
        

        new_hsgt_date, new_hsgt_share_holding, new_hsgt_percent, \
                new_hsgt_delta1, new_hsgt_deltam,\
                conti_day, money_total, \
                is_zig, is_quad, is_peach = comm_handle_hsgt_data(hsgt_df)
        new_hsgt_date = new_hsgt_date[5:]
        
        #### zlje start ####
        zlje    = get_zlje(zlje_df,     new_code, curr_date=str_date)
        zlje_3  = get_zlje(zlje_3_df,   new_code, curr_date=str_date)
        zlje_5  = get_zlje(zlje_5_df,   new_code, curr_date=str_date)
        zlje_10 = get_zlje(zlje_10_df,  new_code, curr_date=str_date)
        #### zlje end ####


        
        #### fina start ####
        if new_code[0:1] == '6':
            stock_code_new= 'SH' + new_code 
        else:
            stock_code_new= 'SZ' + new_code 

        
        fina_df = hdata_fina.get_data_from_hdata(stock_code = stock_code_new)
        
        fina_df = fina_df.sort_values('record_date', ascending=0)
        fina_df = fina_df.reset_index(drop=True)
        #print(fina_df)
        
        op_yoy = net_yoy = 0
        fina_date = new_date
        if len(fina_df):
            fina_date = fina_df['record_date'][0]
            op_yoy = fina_df['operating_income_yoy'][0]
            net_yoy = fina_df['net_profit_atsopc_yoy'][0]
            
            if debug:
                print(stock_code_new)
                print(fina_df)

        fina=str(round(op_yoy,2)) +' ' + str(round(net_yoy,2))
        new_date = fina_date + '<br>'+ fina + '</br>'
        #### fina end ####
        
        #### holder start ####

        holder_df = hdata_holder.get_data_from_hdata(stock_code = stock_code_new)
        holder_df = holder_df.sort_values('record_date', ascending=0)
        holder_df = holder_df.reset_index(drop=True)
        h0 = h1 = h2 = 0
        if len(holder_df) > 0:
            h0 = holder_df['chg'][0]
        if len(holder_df) > 1:
            h1 = holder_df['chg'][1]
        if len(holder_df) > 2:
            h2 = holder_df['chg'][2]
        h_chg = str(h0) + ' ' + str(h1) + ' ' + str(h2)
        #new_code = new_code + '<br>'+ h_chg + '</br>'

        #### holder start ####



        data_list.append([new_date, new_code, new_name, new_pre_price, new_price, new_percent, \
                is_peach, is_zig, is_quad, zlje, zlje_3, zlje_5, zlje_10, h_chg,\
                new_hsgt_date, new_hsgt_share_holding, new_hsgt_percent, \
                new_hsgt_delta1, new_hsgt_deltam, conti_day, money_total])


        #data_list.append([str_date, my_list[i], my_list_cn[i], df['pre_close'][0], df['price'][0] ])

    data_column = ['curr_date', 'code', 'name', 'pre_price', 'price', 'a_pct', \
            'peach', 'zig', 'quad', 'zlje', 'zlje_3', 'zlje_5', 'zlje_10', 'holder_change', \
            'hk_date', 'hk_share', 'hk_pct', 'hk_delta1', 'hk_deltam', 'days', 'hk_m_total']

    ret_df=pd.DataFrame(data_list, columns=data_column)
    ret_df['m_per_day'] = ret_df.hk_m_total / ret_df.days
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
