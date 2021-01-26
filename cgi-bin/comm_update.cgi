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

from get_daily_fund import *

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
    
    stock_list = []

    for i in range(length):
        stock_list.append(my_list[i][0])

    real_df = ts.get_realtime_quotes(stock_list)
    
    ####get zlje start####
    fund_df = get_daily_fund()
    if len(fund_df):
        fund_df = handle_raw_data(fund_df)

    fund_3_df = get_daily_fund(url='url_3')
    if len(fund_3_df):
        fund_3_df = handle_raw_data(fund_3_df)

    fund_5_df = get_daily_fund(url='url_5')
    if len(fund_5_df):
        fund_5_df = handle_raw_data(fund_5_df)

    fund_10_df = get_daily_fund(url='url_10')
    if len(fund_10_df):
        fund_10_df = handle_raw_data(fund_10_df)
    ####get zlje end####



    for i in range(length):
        new_date        = str_date
        new_date        = str_date[2:]
        new_code        = my_list[i][0]
        new_name        = my_list[i][1]
        if debug:
            print("new_code:%s" % new_code)
        
        new_pre_price   = real_df['pre_close'][i]
        new_price       = real_df['price'][i]
        new_percent     = ((float(new_price) - float(new_pre_price)) / float(new_pre_price)) * 100
        new_percent     = round (new_percent, 2)
      
        hsgt_df = hdata_hsgt.get_data_from_hdata(stock_code=new_code, limit=60)
        

        new_hsgt_date, new_hsgt_share_holding, new_hsgt_percent, \
                new_hsgt_delta1, new_hsgt_deltam,\
                conti_day, money_total, \
                is_zig, is_quad, is_peach = comm_handle_hsgt_data(hsgt_df)
        new_hsgt_date = new_hsgt_date[5:]
        
        #### zlje start ####
        tmp_fund_df = fund_df[fund_df['code'] == new_code]
        tmp_fund_df = tmp_fund_df.reset_index(drop=True)
        if debug:
            print(new_code, len(tmp_fund_df))

        tmp_fund_3_df = fund_3_df[fund_3_df['code'] == new_code]
        tmp_fund_3_df = tmp_fund_3_df.reset_index(drop=True)
        if debug:
            print(new_code, len(tmp_fund_3_df))

        tmp_fund_5_df = fund_5_df[fund_5_df['code'] == new_code]
        tmp_fund_5_df = tmp_fund_5_df.reset_index(drop=True)
        if debug:
            print(new_code, len(tmp_fund_5_df))

        tmp_fund_10_df = fund_10_df[fund_10_df['code'] == new_code]
        tmp_fund_10_df = tmp_fund_10_df.reset_index(drop=True)
        if debug:
            print(new_code, len(tmp_fund_3_df))


        zlje = zlje_3 = zlje_5 = zlje_10 =  0
        if len(tmp_fund_df):
            zlje = tmp_fund_df['zlje'][0]
            zdf  = tmp_fund_df['zdf'][0]
            zlje = str(zlje) + '<br>' + str(zdf)+ '</br>' 

        if len(tmp_fund_3_df):
            zlje_3 = tmp_fund_3_df['zlje'][0]
            zdf_3  = tmp_fund_3_df['zdf'][0]
            zlje_3 = str(zlje_3) + '<br>' + str(zdf_3)+ '</br>' 


        if len(tmp_fund_5_df):
            zlje_5 = tmp_fund_5_df['zlje'][0]
            zdf_5  = tmp_fund_5_df['zdf'][0]
            zlje_5 = str(zlje_5) + '<br>' + str(zdf_5)+ '</br>' 


        if len(tmp_fund_10_df):
            zlje_10 = tmp_fund_10_df['zlje'][0]
            zdf_10  = tmp_fund_10_df['zdf'][0]
            zlje_10 = str(zlje_10) + '<br>' + str(zdf_10)+ '</br>' 
        #### zlje end ####


        
        #### fina start ####
        if new_code[0:1] == '6':
            stock_code_new= 'SH' + new_code 
        else:
            stock_code_new= 'SZ' + new_code 

        fina_df = hdata_fina.get_data_from_hdata(stock_code = stock_code_new)
        fina_df = fina_df.sort_values('record_date', ascending=0)
        fina_df = fina_df.reset_index(drop=True)
        op_yoy = net_yoy = 0
        if len(fina_df):
            op_yoy = fina_df['operating_income_yoy'][0]
            net_yoy = fina_df['net_profit_atsopc_yoy'][0]
        fina=str(round(op_yoy,2)) +' ' + str(round(net_yoy,2))
        new_date = new_date + '<br>'+ fina + '</br>'
        #### fina end ####
        
        #### holder start ####

        holder_df = hdata_holder.get_data_from_hdata(stock_code = stock_code_new)
        holder_df = holder_df .sort_values('record_date', ascending=0)
        holder_df = holder_df .reset_index(drop=True)
        h0 = h1 = h2 = 0
        if len(holder_df) > 0:
            h0 = holder_df['chg'][0]
        if len(holder_df) > 1:
            h1 = holder_df['chg'][1]
        if len(holder_df) > 2:
            h2 = holder_df['chg'][2]
        h_chg = str(h0) + ' ' + str(h1) + ' ' + str(h2)
        new_code = new_code + '<br>'+ h_chg + '</br>'

        #### holder start ####



        data_list.append([new_date, new_code, new_name, new_pre_price, new_price, new_percent, \
                is_peach, is_zig, is_quad, zlje, zlje_3, zlje_5, zlje_10,\
                new_hsgt_date, new_hsgt_share_holding, new_hsgt_percent, \
                new_hsgt_delta1, new_hsgt_deltam, conti_day, money_total])


        #data_list.append([str_date, my_list[i], my_list_cn[i], df['pre_close'][0], df['price'][0] ])

    data_column = ['curr_date', 'code', 'name', 'pre_price', 'price', 'a_pct', \
            'peach', 'zig', 'quad', 'zlje', 'zlje_3', 'zlje_5', 'zlje_10', \
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
