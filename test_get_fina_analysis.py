#!/usr/bin/env python  
# -*- coding: utf-8 -*-

import sys
import os
import time
sys.path.append("pysnow_ball")

import psycopg2 #使用的是PostgreSQL数据库
from Stocks import *
from HData_xq_fina import *
from HData_hsgt import *
from HData_xq_day import *
from comm_generate_web_html import *
import  datetime

from time import clock



#funcat
from funcat import *
from funcat.data.aaron_backend import AaronDataBackend
set_data_backend(AaronDataBackend())

hdata_fina=HData_xq_fina("usr","usr")
hsgtdata=HData_hsgt("usr","usr")
hdata_day=HData_xq_day("usr","usr")

debug = 0 
debug = 0
   

def get_fina_data():
    nowdate = datetime.datetime.now().date()    
    lastdate = nowdate - datetime.timedelta(365 * 2) #two years ago

    print('nowdate:%s, lastdate:%s' % (nowdate, lastdate))
    fina_data = hdata_fina.get_data_from_hdata( \
            start_date=lastdate.strftime("%Y-%m-%d"), \
            end_date=nowdate.strftime("%Y-%m-%d"))
    
    return fina_data

def fina_get_continuous_info(df, curr_day, select='operating_income_yoy', net_percent=20):
    all_df = df
    data_list = []
    
    now_hour = int(datetime.datetime.now().strftime("%H"))
    daily_df = pd.DataFrame()
    if now_hour > 12:
        daily_df = hdata_day.get_data_from_hdata(start_date=curr_day, end_date=curr_day)
    else:
        nowdate=datetime.datetime.now().date()
        lastdate=nowdate-datetime.timedelta(1)
        last_day=lastdate.strftime("%Y-%m-%d")
        daily_df = hdata_day.get_data_from_hdata(start_date=last_day, end_date=last_day)
        if debug:
            print('last_day:%s' % last_day)
            print('daily_df:%s' % daily_df)

    group_by_stock_code_df=all_df.groupby('stock_code')
    for stock_code, group_df in group_by_stock_code_df:

        group_df=group_df.sort_values('record_date', ascending=0) #reset index
        group_df=group_df.reset_index(drop=True) #reset index
        max_date=group_df.loc[0, 'record_date']

        if debug:
            print(stock_code)
            print(group_df)

        stock_code = stock_code[2:] 
   
        
        #get stock_cname
        stock_name = symbol(stock_code)
        pos_s=stock_name.rfind('[')
        pos_e=stock_name.rfind(']')
        stock_name=stock_name[pos_s+1: pos_e]
        if debug:
            print(stock_name)
            
        operating_income_yoy=group_df.loc[0, 'operating_income_yoy']
        net_profit_atsopc_yoy=group_df.loc[0, 'net_profit_atsopc_yoy']

        length=len(group_df)
        i = 0
        for i in range(length):
            or_item = group_df.loc[i]['operating_income_yoy']
            netprofit_item = group_df.loc[i]['net_profit_atsopc_yoy']
            if debug:
                print('netprofit_item =%f'%(netprofit_item))

            if or_item >= net_percent and netprofit_item >= net_percent:
                pass
            else:
                break
        if debug:
            print('i=%d' % i)

        #algorithm
        if(i > 1):
            pass
            #if group_df.loc[0]['operating_income_yoy'] < group_df.loc[1]['operating_income_yoy']:  #decline, skip
            #   continue
        else:
            continue

        #funcat call
        T(curr_day)
        S(stock_code)

        if stock_code[0:1] == '6':
            stock_code_new= 'SH' + stock_code
        else:
            stock_code_new= 'SZ' + stock_code


        open_p = close_p = 0

        pre_close = REF(C, 1)
        if pre_close.value != 0:
            open_p = (O - pre_close)/pre_close
            open_p = round (open_p.value, 4)
            open_jump=open_p - 0.02
            if debug:
                print( curr_day, stock_code, O, H, L, C, open_p)

            close_p = (C - pre_close)/pre_close
            close_p = round (close_p.value, 4) * 100

        all_df = hsgtdata.get_data_from_hdata(stock_code=stock_code, end_date=curr_day, limit=60)
        hsgt_date, hsgt_share, hsgt_percent, hsgt_delta1, hsgt_deltam, conti_day, money_total, \
                is_zig, is_quad, is_peach = comm_handle_hsgt_data(all_df)
                
        tmp_df  = daily_df[daily_df['stock_code']==stock_code_new]
        tmp_df = tmp_df.reset_index(drop=True)
        if debug:
            print(stock_code)
            print('len(tmp_df)=%d, tmp_df=%s' % (len(tmp_df), tmp_df))

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
            print(curr_day, max_date, stock_code, stock_name, \
                    operating_income_yoy,  net_profit_atsopc_yoy, i, \
                    close_p, C.value, hsgt_share, hsgt_date, hsgt_percent, hsgt_delta1, hsgt_deltam,\
                    conti_day, money_total,\
                    is_peach, is_zig, is_quad )

        data_list.append([ max_date, stock_code, stock_name, \
                operating_income_yoy, net_profit_atsopc_yoy,  i, \
                close_p, C.value, hsgt_share, hsgt_date, hsgt_percent, hsgt_delta1, hsgt_deltam,\
                conti_day, money_total,\
                is_peach, is_zig, is_quad])  #i  is conti_day
        #break

    '''
    data_column=['record_date', 'stock_code', 'stock_name', 'operating_income_yoy', 'net_profit_atsopc_yoy', \
            'conti_day', \
            'a_pct', 'close', \
            'hk_share', 'hk_date', 'hk_pct', 'hk_delta1', 'hk_deltam', \
            'conti_day', 'hk_m_total',\
            'peach', 'zig', 'quad']

    ret_df = pd.DataFrame(data_list, columns=data_column)
    if select is 'operating_income_yoy':
        ret_df = ret_df.sort_values('operating_income_yoy', ascending=0)
    elif select is 'net_profit_atsopc_yoy':
        ret_df = ret_df.sort_values('net_profit_atsopc_yoy', ascending=0)

    '''

    data_column=['record_date', 'stock_code', 'stock_name', 'op_yoy', 'net_yoy',\
            'fina_days', \
            'a_pct', 'close', \
            'hk_share', 'hk_date', 'hk_pct', 'hk_delta1', 'hk_deltam', \
            'hk_days', 'hk_m_total',\
            'peach', 'zig', 'quad']

    ret_df = pd.DataFrame(data_list, columns=data_column)
    if select is 'operating_income_yoy':
        ret_df = ret_df.sort_values('op_yoy', ascending=0)
    elif select is 'net_profit_atsopc_yoy':
        ret_df = ret_df.sort_values('net_yoy', ascending=0)

    ret_df = ret_df.fillna(0)
    ret_df=ret_df.round(2)

    data_column=['record_date', 'stock_code', 'stock_name', 'op_yoy', 'net_yoy', 'fina_days', \
            'a_pct', 'close', \
            'peach', 'zig', 'quad',\
            'hk_share', 'hk_date', 'hk_pct', 'hk_delta1', 'hk_deltam', \
            'hk_days', 'hk_m_total']

    print(ret_df.columns)

    ret_df=ret_df.loc[:,data_column]




    return ret_df
    
def fina_handle_html_special(newfile):
    with open(newfile,'a') as f:
        f.write('\n')
        f.write('<p>-----------------------------------我是分割线-----------------------------------</p>\n')
        f.write('<p  style="color:blue;"> operating_income_yoy:        营业收入同比增长</p>')
        f.write('<p  style="color:blue;"> net_profit_atsopc_yoy: 净利润同比增长</p>')
        f.write('<p  style="color:blue;"> conti_day:       连续增长次数，并且operating_income_yoy不低于上一次 </p>')
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
    
    
    
def get_example_data(curr_day):

    df = get_fina_data()
    df_fina =  fina_get_continuous_info(df, curr_day, select='operating_income_yoy')

    df.to_csv('./test_fina.csv', encoding='gbk')
    df_fina.to_csv('./test_fina_conti.csv', encoding='gbk')
    return df, df_fina



if __name__ == '__main__':

    t1 = clock()
    nowdate=datetime.datetime.now().date()
    #nowdate=nowdate-datetime.timedelta(1)
    curr_day=nowdate.strftime("%Y-%m-%d")
    print("curr_day:%s"%(curr_day))

    df, df_fina = get_example_data(curr_day)
    fina_generate_html(df_fina)
    t2 = clock()
    print("t1:%s, t2:%s, delta=%s"%(t1, t2, t2-t1))

           

