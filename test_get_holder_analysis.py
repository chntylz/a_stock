#!/usr/bin/env python  
# -*- coding: utf-8 -*-

import psycopg2 #使用的是PostgreSQL数据库
import tushare as ts
from Stocks import *
from HData_fina import *
from HData_hsgt import *
from HData_holder import *
from comm_generate_web_html import *
import  datetime

from time import clock


import sys
import os
import time

#funcat
from funcat import *
from funcat.data.aaron_backend import AaronDataBackend
set_data_backend(AaronDataBackend())

hdata_fina=HData_fina("usr","usr")
hdata_holder=HData_holder("usr","usr")
hdata_hsgt=HData_hsgt("usr","usr")

debug = 0 
#debug = 1
   

def get_holder_data():
    nowdate = datetime.datetime.now().date()    
    lastdate = nowdate - datetime.timedelta(365 * 2) #two years ago

    print('nowdate:%s, lastdate:%s' % (nowdate, lastdate))
    holder_data  =  hdata_holder.get_data_from_hdata( start_date=lastdate.strftime("%Y%m%d"), end_date=nowdate.strftime("%Y%m%d"))
    
    return holder_data

def holder_get_continuous_info(df, curr_day):
    all_df = df
    data_list = []
    group_by_stock_code_df=all_df.groupby('stock_code')
    for stock_code, group_df in group_by_stock_code_df:
        if debug:
            print(stock_code)
            print(group_df.head(1))

   
        
        #get stock_cname
        stock_name = symbol(stock_code)
        pos_s=stock_name.rfind('[')
        pos_e=stock_name.rfind(']')
        stock_name=stock_name[pos_s+1: pos_e]
        if debug:
            print(stock_name)
        #skip ST
        if ('ST' in stock_name):
            continue
 
        
        group_df=group_df.reset_index(drop=True) #reset index
        max_date=group_df.loc[0, 'record_date']
        holder_num=group_df.loc[0, 'holder_num']

        length=len(group_df)
        for i in range(length-1):
            holder_0 = group_df.loc[i]['holder_num']
            holder_1 = group_df.loc[i+1]['holder_num']
            if debug:
                print('holder_0:%f, holder_1:%f'%(holder_0, holder_1))

            if holder_0 <= holder_1:
                pass
            else:
                break

        #algorithm
        if(i > 1):
            pass
            #if group_df.loc[0]['holder_num'] < group_df.loc[1]['holder_num']:  #decline, skip
            #   continue
        else:
            continue

        #funcat call
        T(curr_day)
        S(stock_code)
        pre_close = REF(C, 1)
        open_p = (O - pre_close)/pre_close
        open_p = round (open_p.value, 4)
        open_jump=open_p - 0.02
        if debug:
            print(str(nowdate), stock_code, O, H, L, C, open_p)

        close_p = (C - pre_close)/pre_close
        close_p = round (close_p.value, 4) * 100

        all_df = hdata_hsgt.get_data_from_hdata(stock_code=stock_code, end_date=curr_day, limit=60)
        hsgt_date, hsgt_share, hsgt_percent, hsgt_delta1, hsgt_deltam, conti_day, money_total = comm_handle_hsgt_data(all_df)
        

        if debug:
            print( max_date, stock_code, stock_name, holder_num, i, close_p, C.value, hsgt_share, hsgt_date, hsgt_percent, hsgt_delta1, hsgt_deltam, conti_day, money_total )

        data_list.append([ max_date, stock_code, stock_name, holder_num,  i, close_p, C.value,  hsgt_date, hsgt_share, hsgt_percent, hsgt_delta1, hsgt_deltam, conti_day, money_total])  #i  is conti_day

    data_column=['record_date', 'stock_code', 'stock_name', 'holder_num', 'cont_d', 'a_pct', 'close', 'hk_date', 'hsgt_share', 'hk_pct', 'hk_delta1', 'hk_deltam', 'hk_cont_d', 'hk_m_total']

    ret_df = pd.DataFrame(data_list, columns=data_column)
    ret_df = ret_df.fillna(0)
    ret_df=ret_df.round(2)
    ret_df = ret_df.sort_values('cont_d', ascending=0)


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

    t1 = clock()
    nowdate=datetime.datetime.now().date()
    curr_day=nowdate.strftime("%Y-%m-%d")
    print("curr_day:%s"%(curr_day))

    df, df_holder = get_example_data(curr_day)
    holder_generate_html(df_holder)
    t2 = clock()
    print("t1:%s, t2:%s, delta=%s"%(t1, t2, t2-t1))

           

