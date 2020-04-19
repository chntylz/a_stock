#!/usr/bin/env python  
# -*- coding: utf-8 -*-

import psycopg2 #使用的是PostgreSQL数据库
import tushare as ts
from Stocks import *
from HData_fina import *
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
debug = False
   

def get_fina_data():
    nowdate = datetime.datetime.now().date()    
    lastdate = nowdate - datetime.timedelta(365 * 2) #two years ago

    print('nowdate:%s, lastdate:%s' % (nowdate, lastdate))
    fina_data  =  hdata_fina.get_all_hdata_of_stock_accord_time( lastdate.strftime("%Y%m%d"), nowdate.strftime("%Y%m%d"))
    
    return fina_data

def fina_get_continuous_info(df, select='or_yoy', net_percent=20):
    all_df = df
    data_list = []
    group_by_stock_code_df=all_df.groupby('ts_code')
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
        max_date=group_df.loc[0, 'ann_date']
        or_yoy=group_df.loc[0, 'or_yoy']
        netprofit_yoy=group_df.loc[0, 'netprofit_yoy']

        length=len(group_df)
        for i in range(length):
            or_item = group_df.ix[i]['or_yoy']
            netprofit_item = group_df.ix[i]['netprofit_yoy']
            if debug:
                print('netprofit_item =%f'%(netprofit_item))

            if or_item >= net_percent and netprofit_item >= net_percent:
                pass
            else:
                break

        #algorithm
        if(i > 1):
             if group_df.ix[0]['or_yoy'] < group_df.ix[1]['or_yoy']:  #decline, skip
                continue
        else:
             continue


        if debug:
            print(max_date, stock_code, stock_name, or_yoy,  netprofit_yoy, i)

        data_list.append([max_date, stock_code, stock_name, or_yoy, netprofit_yoy,  i])  #i  is p_count

    data_column=['record_date', 'stock_code', 'stock_name', 'or_yoy', 'netprofit_yoy', 'p_count']

    ret_df = pd.DataFrame(data_list, columns=data_column)
    if select is 'or_yoy':
        ret_df = ret_df.sort_values('or_yoy', ascending=0)
    elif select is 'netprofit_yoy':
        ret_df = ret_df.sort_values('netprofit_yoy', ascending=0)


    return ret_df
    
def fina_handle_html_special(newfile):
    with open(newfile,'a') as f:
        f.write('\n')
        f.write('<p>-----------------------------------我是分割线-----------------------------------</p>\n')
        f.write('<p  style="color:blue;"> or_yoy:        营业收入同比增长</p>')
        f.write('<p  style="color:blue;"> netprofit_yoy: 净利润同比增长</p>')
        f.write('<p  style="color:blue;"> p_count:       连续增长次数，并且or_yoy不低于上一次 </p>')
        f.write('<p>-----------------------------------我是分割线-----------------------------------</p>\n')
        f.write('\n')
        f.write('\n')
    pass

    
    
def fina_generate_html(df):
    save_dir = "fina"
    exec_command = "mkdir -p " + (save_dir)
    print(exec_command)
    os.system(exec_command)

    file_name='finance'
    newfile=save_dir + '/' + file_name + '.html'

    comm_handle_html_head(newfile, save_dir, datetime.datetime.now().date().strftime("%Y-%m-%d")  )
    fina_handle_html_special(newfile)
    comm_handle_html_body(newfile, df)
    comm_handle_html_end(newfile)
    
    
    
def get_example_data():

    hdata_fina.db_connect()#由于每次连接数据库都要耗时0.0几秒，故获取历史数据时统一连接
    df = get_fina_data()
    df_fina =  fina_get_continuous_info(df, 'or_yoy')
    hdata_fina.db_disconnect()

    return df, df_fina



if __name__ == '__main__':

    t1 = clock()
    df, df_fina = get_example_data()
    fina_generate_html(df_fina)
    t2 = clock()
    print("t1:%s, t2:%s, delta=%s"%(t1, t2, t2-t1))

           

