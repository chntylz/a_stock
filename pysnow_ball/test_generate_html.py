#!/usr/bin/env python
#coding:utf-8
import os,sys
sys.path.append('..')
sys.path.append('../eastmoney')

import psycopg2 #使用的是PostgreSQL数据库
from HData_xq_day import *
from HData_hsgt import *
import  datetime
import time


from comm_generate_web_html import *

import numpy as np
import pandas as pd
#keep 0.01 accrucy
pd.set_option('display.float_format',lambda x : '%.2f' % x)


#funcat
from funcat import *
from funcat.data.aaron_backend import AaronDataBackend
set_data_backend(AaronDataBackend())

hdata=HData_xq_day("usr","usr")
hsgtdata=HData_hsgt("usr","usr")

dict_industry={}
df=None


debug=0
debug=1

today_date=datetime.datetime.now().date()
#test
nowdate=today_date
lastdate=nowdate-datetime.timedelta(1)

curr_day=nowdate.strftime("%Y-%m-%d")
curr_day_w=nowdate.strftime("%Y-%m-%d-%w")
last_day=lastdate.strftime("%Y-%m-%d")
print("curr_day:%s, last_day:%s"%(curr_day, last_day))

stock_data_dir="stock_data"
curr_dir=curr_day_w+'-zig'


def zig_continue_handle_html_body_special(newfile, date):
    f = newfile
    curr_day = date
    with open(newfile,'a') as f:
        f.write('\n')
        f.write('\n')
        f.write('\n')
        f.write('\n')
        f.write('<p> 日期 %s </p>\n' %(curr_day))

        t1 = time.time()
        #df = get_today_item(curr_day)
        print('delta time= %s ' % (time.time() - t1))

        # 找出上涨的股票
        df_up = df[df['percent'] > 0.00]
        # 走平股数
        df_even = df[df['percent'] == 0.00]
        # 找出下跌的股票
        df_down = df[df['percent'] < 0.00]

        # 找出涨停的股票
        limit_up = df[df['percent'] >= 9.70]
        limit_down = df[df['percent'] <= -9.70]

        s_debug= ('<p> A股上涨个数： %d,  A股下跌个数： %d,  A股走平个数:  %d</p>' % (df_up.shape[0], df_down.shape[0], df_even.shape[0]))
        print(s_debug)
        f.write('%s\n'%(s_debug))

        s_debug=('<p> 涨停数量：%d 个</p>' % (limit_up.shape[0]))
        print(s_debug)
        f.write('%s\n'%(s_debug))

        s_debug=('<p> 跌停数量：%d 个</p>' % (limit_down.shape[0]))
        print(s_debug)
        f.write('%s\n'%(s_debug))

        f.write('<p>-----------------------------------我是分割线-----------------------------------</p>\n')
    
        f.write('<p  style="color:green;">绿色: 当天跳空高开2个点以上 </p>')
        f.write('<p>-----------------------------------我是分割线-----------------------------------</p>\n')
        f.write('\n')

    pass


def zig_continue_handle_html_end_special(newfile, dict_industry):
    with open(newfile,'a') as f:
        f.write('\n')
        f.write('\n')
        f.write('\n')
        f.write('\n')
        f.write('<p>-----------------------------------我是分割线-----------------------------------</p>\n')
        f.write('\n')
        f.write('<p>industry %s</p>\n' % (sorted(dict_industry.items(),key=lambda x:x[1],reverse=True)))
        f.write('\n')
        f.write('<p>-----------------------------------我是分割线-----------------------------------</p>\n')

    pass
    
   
def generate_zig_html(df):
    os.system('mkdir -p ' + stock_data_dir)
    os.system('mkdir -p ' + stock_data_dir +'/' + curr_dir)
    newfile='%s/%s'%(stock_data_dir, curr_dir + '/' + curr_dir + '.html')
    comm_handle_html_head(newfile, stock_data_dir, curr_day )
    zig_continue_handle_html_body_special(newfile, curr_day)
    comm_handle_html_body(newfile, df)
    zig_continue_handle_html_end_special(newfile, dict_industry)
    comm_handle_html_end(newfile, curr_dir)

   

def get_current_data(date):
    print(date)
    df = hdata.get_data_from_hdata(start_date=date, \
            end_date=date)
    if len(df):
        print(df.head(2))
    return df


def handle_zig_to_html(df):
    if len(df) < 1:
        print('#error, df data len < 1, return')
        return
    zig_df = df[(df.is_zig == 1) | (df.is_zig == 2) ]
    zig_df = zig_df.reset_index(drop=True)
    html_df = comm_generate_web_dataframe_new(zig_df, curr_day_w, curr_day, dict_industry )
    return html_df
    
if __name__ == '__main__':

    script_name, para1 = check_input_parameter()
    print("%s, %d"%(script_name, int(para1)))
    today_date=datetime.datetime.now().date()
    nowdate=today_date-datetime.timedelta(int(para1))

    print("nowdate is %s"%(nowdate.strftime("%Y-%m-%d"))) 
    #test
    lastdate=nowdate-datetime.timedelta(1)

    curr_day=nowdate.strftime("%Y-%m-%d")
    curr_day_w=nowdate.strftime("%Y-%m-%d-%w")
    last_day=lastdate.strftime("%Y-%m-%d")
    print("curr_day:%s, last_day:%s"%(curr_day, last_day))

    curr_dir=curr_day_w+'-zig'
    df = get_current_data(curr_day)

    html_zig_df = handle_zig_to_html(df)

    generate_zig_html(html_zig_df)


