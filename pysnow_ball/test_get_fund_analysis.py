#!/usr/bin/env python  
# -*- coding: utf-8 -*-

import sys
import os
import time
sys.path.append("..")
sys.path.append("../eastmoney/")


import psycopg2 #使用的是PostgreSQL数据库
from Stocks import *
from HData_fina import *
from HData_hsgt import *
from HData_xq_holder import *
from HData_xq_day import *

from HData_eastmoney_fund import *

from comm_generate_web_html import *
import  datetime

from time import clock
import pandas as pd

#funcat
from funcat import *
from funcat.data.aaron_backend import AaronDataBackend
set_data_backend(AaronDataBackend())

hdata_holder=HData_xq_holder("usr","usr")
hdata_hsgt=HData_hsgt("usr","usr")
hdata_day=HData_xq_day("usr","usr")
hdata_fund=HData_eastmoney_fund("usr","usr")

debug = 0 
#debug = 1
 
def get_fund_data(date=None):
    nowdate = date
    if date is None: 
        nowdate = datetime.datetime.now().date()    
    lastdate = nowdate - datetime.timedelta(365 * 1) #1 years ago

    #get the latest date from guizhoumaotai
    maxdate = hdata_fund.db_get_maxdate_of_stock('600519')

    print('nowdate:%s, lastdate:%s' % (nowdate, lastdate))
    df = hdata_fund.get_data_from_hdata( start_date=lastdate.strftime("%Y-%m-%d"), \
            end_date=nowdate.strftime("%Y-%m-%d"))
    
    df = df.sort_values('record_date', ascending=0)
    df = df.reset_index(drop=True)

    df['quantity_last']=df.groupby('stock_code')['quantity'].shift((-1))
    #insert the fourth column
    df.insert(6, 'delta_quantity', df['quantity'] - df['quantity_last'])
    df = df.fillna(0)

    del df['quantity_last']
    del df['type']
    del df['value']

    #df = df.sort_values('delta_quantity', ascending=0)
    df = df.sort_values('quantity', ascending=0)
    df = df.reset_index(drop=True)
    df = df[df['record_date'] == maxdate.strftime("%Y-%m-%d")]
    df = df.reset_index(drop=True)
    return df

def get_curr_day_k_data(date=None):

    daily_df = pd.DataFrame()
    if date is None:
        now_hour = int(datetime.datetime.now().strftime("%H"))
        nowdate=datetime.datetime.now().date()
        curr_day=nowdate.strftime("%Y-%m-%d")

        print('now_hour=%d' % now_hour )

        if now_hour > 12:
            daily_df = hdata_day.get_data_from_hdata(start_date=curr_day,
                    end_date=curr_day)
        else:
            lastdate=nowdate-datetime.timedelta(1)
            last_day=lastdate.strftime("%Y-%m-%d")
            daily_df = hdata_day.get_data_from_hdata(start_date=last_day,
                    end_date=last_day)
    else:
        curr_day = date
        daily_df = hdata_day.get_data_from_hdata(start_date=curr_day,
                end_date=curr_day)

    print('len(daily_df) = %d' % len(daily_df))
    return daily_df

    
def holder_handle_html_special(newfile):
    with open(newfile,'a') as f:
        f.write('\n')
        f.write('<p>-----------------------------------我是分割线-----------------------------------</p>\n')
        f.write('<p  style="color:blue;"> holder_num:      截止日期股东数</p>')
        f.write('<p  style="color:blue;"> conti_day:       连续减少次数</p>')
        f.write('<p>-----------------------------------我是分割线-----------------------------------</p>\n')
        f.write('\n')
        f.write('\n')
    pass

    
    
def fund_generate_html(df, save_dir=None, file_name=None):
    if save_dir is None:
        save_dir = "fund"
    if file_name is None:
        file_name='fund'

    exec_command = "mkdir -p " + (save_dir)
    print(exec_command)
    os.system(exec_command)

    newfile=save_dir + '/' + file_name + '.html'

    comm_handle_html_head(newfile, save_dir, datetime.datetime.now().date().strftime("%Y-%m-%d")  )
    holder_handle_html_special(newfile)
    comm_handle_html_body(newfile, df)
    comm_handle_html_end(newfile)
    
     
def get_fund_df(date):

    df = get_fund_data(date)
   
    str_date = date.strftime('%Y-%m-%d')
    data_list = []

    
    ####get zlje start####
    zlje_df   = get_zlje_data_from_db(url='url',     curr_date=str_date)
    zlje_3_df = get_zlje_data_from_db(url='url_3',   curr_date=str_date)
    zlje_5_df = get_zlje_data_from_db(url='url_5',   curr_date=str_date)
    zlje_10_df = get_zlje_data_from_db(url='url_10', curr_date=str_date)

    length = len(df)
    for i in range(length):
        new_date        = str_date
        new_code = df.loc[i]['stock_code']
        new_name = df.loc[i]['stock_name']

        fu_num = df.loc[i]['quantity'] 
        fu_delta = df.loc[i]['delta_quantity'] 
        fu_value = df.loc[i]['total_value'] 
        fu_ratio = df.loc[i]['float_ratio'] 
        fu_chg_share = df.loc[i]['chg_share']
        fu_chg_ratio = df.loc[i]['chg_ratio'] 


        real_df = zlje_df[zlje_df['stock_code'] == new_code]
        if len(real_df):
            real_df = real_df.reset_index(drop=True)
            new_price       = real_df['zxj'][0]
            new_percent     = real_df['zdf'][0]
            new_pre_price   = round(new_price/(1+(new_percent/100)), 2)


        hsgt_df = hdata_hsgt.get_data_from_hdata(stock_code=new_code, limit=60)
        

        new_hsgt_date, new_hsgt_share_holding, new_hsgt_percent, \
                new_hsgt_delta1, new_hsgt_deltam,\
                conti_day, money_total, \
                is_zig, is_quad, is_peach = comm_handle_hsgt_data(hsgt_df)
        if debug:
            print('new_hsgt_date=%s' % new_hsgt_date)
        #new_hsgt_date = new_hsgt_date[5:]
        
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

        #### holder end ####

        data_list.append([new_date, new_code, new_name, new_pre_price, new_price, new_percent, \
                is_peach, is_zig, is_quad, zlje, zlje_3, zlje_5, zlje_10, h_chg,\
                fu_num, fu_delta, fu_value, fu_ratio, fu_chg_share, fu_chg_ratio, \
                new_hsgt_date, new_hsgt_share_holding, new_hsgt_percent, \
                new_hsgt_delta1, new_hsgt_deltam, conti_day, money_total])



    data_column = ['curr_date', 'code', 'name', 'pre_price', 'price', 'a_pct', \
            'peach', 'zig', 'quad', 'zlje', 'zlje_3', 'zlje_5', 'zlje_10', 'holder_change', \
            'fu_num', 'fu_delta', 'fu_value', 'fu_ratio', 'fu_chg_share', 'fu_chg_ratio', \
            'hk_date', 'hk_share', 'hk_pct', 'hk_delta1', 'hk_deltam', 'days', 'hk_m_total']

    ret_df=pd.DataFrame(data_list, columns=data_column)
    ret_df['m_per_day'] = ret_df.hk_m_total / ret_df.days
    ret_df = ret_df.fillna(0)
    ret_df=ret_df.round(2)

    return ret_df

if __name__ == '__main__':

    t1 = time.time()
    nowdate=datetime.datetime.now().date()
    lastdate=nowdate-datetime.timedelta(0)
    curr_day=nowdate.strftime("%Y-%m-%d")
    last_day=lastdate.strftime("%Y-%m-%d")

    print("curr_day:%s"%(curr_day))

    fund_df = get_fund_df(lastdate)
    fund_generate_html(fund_df)

    t2 = time.time()
    print("t1:%s, t2:%s, delta=%s"%(t1, t2, t2-t1))

           

