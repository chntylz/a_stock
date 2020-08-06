#!/usr/bin/env python  
# -*- coding: utf-8 -*-

import psycopg2 #使用的是PostgreSQL数据库
import tushare as ts
from Stocks import *
from HData_hsgt import *
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

token='21dddafc47513ea46b89057b2c4edf7b44882b3e92274b431f199552'
pro = ts.pro_api(token)

hsgtdata=HData_hsgt("usr","usr")

debug = 0 
#debug = 1

'''
#https://tushare.pro/document/2?doc_id=124
'''

def get_repurchase_data():
    nowdate = datetime.datetime.now().date()    
    lastdate = nowdate - datetime.timedelta(30) #two years ago
    print('nowdate:%s, lastdate:%s' % (nowdate, lastdate))
    repurchase_data  =  pro.repurchase(
            ann_date='', 
            start_date=lastdate.strftime("%Y%m%d"),
            end_date=nowdate.strftime("%Y%m%d"))
    ret_df = repurchase_data.fillna(0)
    return ret_df

#              ts_code  ann_date  end_date   proc  exp_date  vol        amount     high_limit  low_limit
#       0    002540.SZ  20200507  20200430   实施   None     1623510.00 6675075.47    4.14       3.89
def repurchase_get_hk_info(df, curr_day):
    data_list = []
    df=df[df['amount'] > (10000 * 10000)]
    df=df.reset_index(drop=True)

    length=len(df)
    for i in range(length):

        max_date = df.loc[i,'ann_date']
        end_date = df.loc[i,'end_date']
        proc     = df.loc[i,'proc']
        exp_date = df.loc[i,'exp_date']
        repu_vol = df.loc[i,'vol']
        repu_amount = df.loc[i,'amount'] / 10000.0/10000.0
        repu_amount = str(round(repu_amount, 2))+'Y'
        high_limit  = df.loc[i,'high_limit']        
        low_limit   =  df.loc[i,'low_limit']


        stock_code=df.loc[i, 'ts_code']
        stock_code=stock_code[:6]
        if debug:
            print(stock_code)
            print(df.head(1))

   
        
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

        hk_df = hsgtdata.get_data_from_hdata(stock_code=stock_code, end_date=curr_day, limit=60)
        hsgt_date, hsgt_share, hsgt_percent, hsgt_delta1, hsgt_deltam, conti_day, money_total = comm_handle_hsgt_data(hk_df)
       

        if debug:
            print(curr_day, max_date, stock_code, stock_name, close_p, C.value, hsgt_share, hsgt_date, hsgt_percent, hsgt_delta1, hsgt_deltam, conti_day, money_total )

        data_list.append([ max_date, stock_code, stock_name, close_p, C.value, end_date, proc, exp_date, repu_vol, repu_amount, high_limit, low_limit, hsgt_share, hsgt_date, hsgt_percent, hsgt_delta1, hsgt_deltam, conti_day, money_total])  

    data_column=['record_date', 'stock_code', 'stock_name', 'a_pct', 'close', 'end_date', 'proc', 'exp_date', 'repu_vol', 'repu_amount', 'high_limit', 'low_limit', 'hk_share', 'hk_date', 'hk_pct', 'hk_delta1', 'hk_deltam', 'conti_day', 'hk_m_total']

    ret_df = pd.DataFrame(data_list, columns=data_column)
    ret_df = ret_df.sort_values('proc', ascending=0)
    ret_df = ret_df.reset_index(drop=True)

    ret_df = ret_df.fillna(0)
    ret_df=ret_df.round(2)


    return ret_df
    
def repurchase_handle_html_special(newfile):
    with open(newfile,'a') as f:
        f.write('\n')
        f.write('<p>-----------------------------------我是分割线-----------------------------------</p>\n')
        f.write('<p  style="color:blue;"> or_yoy:        营业收入同比增长</p>')
        f.write('<p  style="color:blue;"> netprofit_yoy: 净利润同比增长</p>')
        f.write('<p  style="color:blue;"> conti_day:       连续增长次数，并且or_yoy不低于上一次 </p>')
        f.write('<p>-----------------------------------我是分割线-----------------------------------</p>\n')
        f.write('\n')
        f.write('\n')
    pass

    
    
def repurchase_generate_html(df):
    save_dir = "repurchase"
    exec_command = "mkdir -p " + (save_dir)
    print(exec_command)
    os.system(exec_command)

    file_name='repurchase'
    newfile=save_dir + '/' + file_name + '.html'

    comm_handle_html_head(newfile, save_dir, datetime.datetime.now().date().strftime("%Y-%m-%d")  )
    repurchase_handle_html_special(newfile)
    comm_handle_html_body(newfile, df)
    comm_handle_html_end(newfile)
    
    
    
def get_example_data(curr_day):

    df = get_repurchase_data()
    df_repurchase =  repurchase_get_hk_info(df, curr_day)
    return df, df_repurchase



if __name__ == '__main__':

    t1 = clock()
    nowdate=datetime.datetime.now().date()
    curr_day=nowdate.strftime("%Y-%m-%d")
    print("curr_day:%s"%(curr_day))

    df, df_repurchase = get_example_data(curr_day)
    repurchase_generate_html(df_repurchase)
    t2 = clock()
    print("t1:%s, t2:%s, delta=%s"%(t1, t2, t2-t1))

           

