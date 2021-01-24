#!/usr/bin/env python  
# -*- coding: utf-8 -*-

import sys
import os
import time
sys.path.append("..")


import psycopg2 #使用的是PostgreSQL数据库
from Stocks import *
from HData_fina import *
from HData_hsgt import *
from HData_xq_holder import *
from HData_xq_day import *
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

debug = 0 
#debug = 1
   

def get_holder_data():
    nowdate = datetime.datetime.now().date()    
    lastdate = nowdate - datetime.timedelta(365 * 3) #3 years ago

    print('nowdate:%s, lastdate:%s' % (nowdate, lastdate))
    holder_data  =  hdata_holder.get_data_from_hdata( start_date=lastdate.strftime("%Y%m%d"), end_date=nowdate.strftime("%Y%m%d"))
    
    holder_data = holder_data.sort_values('record_date', ascending=0)
    return holder_data

def get_curr_day_k_data():

    daily_df = pd.DataFrame()
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

    print('len(daily_df) = %d' % len(daily_df))
    return daily_df

def holder_get_continuous_info(df, curr_day):

    daily_df = get_curr_day_k_data()

    df=df[~df['holder_num'].isin([0])]  #delete the line which holder_num value is 0
    df = df.fillna(0)
    df['holder_last']=df.groupby('stock_code')['holder_num'].shift((-1))
    df['holder_pct']=(df['holder_num'] - df['holder_last'])  * 100 / df['holder_last']

    all_df = df
    data_list = []
    group_by_stock_code_df=all_df.groupby('stock_code')
    for stock_code, group_df in group_by_stock_code_df:
        if debug:
            tt1 = time.time()
            print(stock_code)
            print(group_df.head(1))
            print('len(group_df)=%d' % len(group_df))

        stock_code_new = stock_code[2:]

   
        
        #get stock_cname
        stock_name = symbol(stock_code_new)
        pos_s=stock_name.rfind('[')
        pos_e=stock_name.rfind(']')
        stock_name=stock_name[pos_s+1: pos_e]
        if debug:
            print('stock_name=%s' % stock_name)
        #skip ST
        if ('ST' in stock_name):
            continue
 
        
        group_df=group_df.reset_index(drop=True) #reset index
        holder_pct = group_df.loc[0, 'holder_pct']
        max_date=group_df.loc[0, 'record_date']
        holder_num=group_df.loc[0, 'holder_num']

        length=len(group_df)
        i = 0
        for i in range(length-1):
            if length < 2:
                break

            if debug:
                print('holder_0:%f, holder_1')

            holder_0 = group_df.loc[i]['holder_num']
            holder_1 = group_df.loc[i+1]['holder_num']
            if debug:
                print('holder_0:%f, holder_1:%f'%(holder_0, holder_1))

            if holder_0 <= holder_1:
                pass
            else:
                break

        #algorithm
        if debug:
            print('i=%d' % i)
        if(i > 1):
            curr_holder_num = group_df.loc[0, 'holder_num']
            i_holder_num = group_df.loc[i, 'holder_num']
            holder_pct_i =  (curr_holder_num - i_holder_num ) * 100 / i_holder_num
            pass
            #if group_df.loc[0]['holder_num'] < group_df.loc[1]['holder_num']:  #decline, skip
            #   continue
        else:
            continue

        #funcat call
        T(curr_day)
        S(stock_code_new)
        if debug:
            print(stock_code_new, O, H, L, C)
            
        pre_close = REF(C, 1)
        open_p = (O - pre_close)/pre_close
        open_p = round (open_p.value, 4)
        open_jump=open_p - 0.02
        if debug:
            print(stock_code_new, O, H, L, C, open_p)

        close_p = (C - pre_close)/pre_close
        close_p = round (close_p.value, 4) * 100

        all_df = hdata_hsgt.get_data_from_hdata(stock_code=stock_code_new, end_date=curr_day, limit=60)
        hsgt_date, hsgt_share, hsgt_percent, hsgt_delta1, hsgt_deltam, conti_day, money_total \
                = comm_handle_hsgt_data(all_df)
       

        tmp_df = daily_df[daily_df['stock_code']==stock_code]
        tmp_df = tmp_df.reset_index(drop=True)
        if debug:
            print(stock_code)
            print(len(tmp_df), tmp_df)

        if(len(tmp_df)):
            is_zig   = tmp_df['is_zig'][0]
            is_quad  = tmp_df['is_quad'][0]
            is_peach = tmp_df['is_peach'][0]

        else:
            is_zig   = 0
            is_quad  = 0
            is_peach = 0
        
        '''
        is_zig   = daily_df[daily_df['stock_code']==stock_code]['is_zig'][0]
        is_quad  = daily_df[daily_df['stock_code']==stock_code]['is_quad'][0]
        is_peach = daily_df[daily_df['stock_code']==stock_code]['is_peach'][0]
        '''


        if debug:
            print( max_date, stock_code_new, stock_name, holder_num, i,  holder_pct, holder_pct_i, close_p, C.value, \
                    hsgt_share, hsgt_date, hsgt_percent, hsgt_delta1, hsgt_deltam, \
                    conti_day, money_total,\
                    is_peach, is_zig, is_quad )

        data_list.append([ max_date, stock_code_new, stock_name, holder_num,  i,  holder_pct, holder_pct_i, \
                close_p, C.value,  hsgt_date, hsgt_share, hsgt_percent, hsgt_delta1, hsgt_deltam, \
                conti_day, money_total,\
                is_peach, is_zig, is_quad])  #i  is conti_day

        tt2 = time.time()
        if debug:
            print('tt2-tt1=%s' % (tt2-tt1))
            print('---------------------------------------------------------------------')

    data_column=['record_date', 'stock_code', 'stock_name', 'holder_num', 'cont_d', 'holder_pct', 'holder_pct_i', \
            'a_pct', 'close', 'hk_date', 'hsgt_share', 'hk_pct', 'hk_delta1', 'hk_deltam', \
            'hk_cont_d', 'hk_m_total',\
            'peach', 'zig', 'quad']

    ret_df = pd.DataFrame(data_list, columns=data_column)
    ret_df = ret_df.fillna(0)
    ret_df=ret_df.round(2)
    #ret_df = ret_df.sort_values('cont_d', ascending=0)

    data_column=['record_date', 'stock_code', 'stock_name', 'holder_num', 'cont_d', 'holder_pct', 'holder_pct_i', \
            'a_pct', 'close', \
            'peach', 'zig', 'quad',\
            'hk_date', 'hsgt_share', 'hk_pct', 'hk_delta1', 'hk_deltam', \
            'hk_cont_d', 'hk_m_total']
    ret_df=ret_df.loc[:,data_column]
                        
    ret_df = ret_df.sort_values('holder_pct', ascending=1)


    return ret_df
    
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

    
    
def holder_generate_html(df):
    save_dir = "holder"
    exec_command = "mkdir -p " + (save_dir)
    print(exec_command)
    os.system(exec_command)

    file_name='holder'
    newfile=save_dir + '/' + file_name + '.html'

    comm_handle_html_head(newfile, save_dir, datetime.datetime.now().date().strftime("%Y-%m-%d")  )
    holder_handle_html_special(newfile)
    comm_handle_html_body(newfile, df)
    comm_handle_html_end(newfile)
    
    
    
def get_example_data(curr_day):

    hdata_holder.db_connect()#由于每次连接数据库都要耗时0.0几秒，故获取历史数据时统一连接
    df = get_holder_data()
    df_holder =  holder_get_continuous_info(df, curr_day )
    hdata_holder.db_disconnect()

    return df, df_holder



if __name__ == '__main__':

    t1 = time.time()
    nowdate=datetime.datetime.now().date()
    lastdate=nowdate-datetime.timedelta(1)
    curr_day=nowdate.strftime("%Y-%m-%d")
    last_day=lastdate.strftime("%Y-%m-%d")

    print("curr_day:%s"%(curr_day))

    df, df_holder = get_example_data(last_day)
    holder_generate_html(df_holder)
    t2 = time.time()
    print("t1:%s, t2:%s, delta=%s"%(t1, t2, t2-t1))

           
