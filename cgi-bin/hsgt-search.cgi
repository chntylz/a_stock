#!/usr/bin/python3.6
# -*- coding: utf-8 -*- 
import os,sys
import cgi

import psycopg2 #使用的是PostgreSQL数据库
import tushare as ts
import numpy as np

from HData_hsgt import *
from HData_day import *
from zig import *
from test_plot import *

import matplotlib.pyplot as plt

import  datetime





debug=0

nowdate=datetime.datetime.now().date()
#nowdate=nowdate-datetime.timedelta(1)

hdata_day=HData_day("usr","usr")
hdata_day.db_connect()

hdata_hsgt=HData_hsgt("usr","usr")
hdata_hsgt.db_connect()

form = cgi.FieldStorage()
name = form.getvalue('name', '000401')




def aaron_get_stock_code(name):
    info = ts.get_stock_basics()
    aaron_df = info[info['name'] == name ]
    if(len(aaron_df) > 0):
        return aaron_df.index[0]
    else:
        return ''




def hsgt_get_delta_m_of_day(df, days):
    delta_dict={2:'delta2_m',  3:'delta3_m', 4:'delta4_m', 5:'delta5_m', 10:'delta10_m', 21:'delta21_m', 120:'delta120_m'}
    target_column=delta_dict[days]
    df[target_column] = df['delta1_m']
    for i in range(1, days):
        if debug:
            print('i=%d, days=%d'%(i, days))
        src_column='money_sft_'+ str(i)
        df[target_column] = df[target_column] + df[src_column]

    return df


def plot_stock_picture(nowcode, nowname):
    #define canvas out of loop
    plt.style.use('bmh')
    fig = plt.figure(figsize=(24, 30),dpi=80)

    detail_info = hdata_day.get_limit_hdata_of_stock(nowcode, nowdate.strftime("%Y-%m-%d"), 600)
    #print(detail_info)
    save_dir = 'stock_data'
    sub_name = ''
    plot_picture(nowdate, nowcode, nowname, detail_info, save_dir, fig, sub_name)
    plt.close('all')

def get_xueqiu_url(stock_code_tmp):

    #if stock_code_tmp[0:2] == '60':
    if stock_code_tmp[0:1] == '6':
        stock_code_new='SH'+stock_code_tmp
    else:
        stock_code_new='SZ'+stock_code_tmp
    xueqiu_url='https://xueqiu.com/S/' + stock_code_new
    finance_url = xueqiu_url + '/detail#/ZYCWZB'

    return xueqiu_url, finance_url


def gete_df_and_stock_code(name):
    if name.isdigit():
        df=hdata_hsgt.get_all_hdata_of_stock_code(name)
        stock_code_tmp=name
    else:
        df=hdata_hsgt.get_all_hdata_of_stock_cname(name)
        stock_code_tmp=aaron_get_stock_code(name)
        
    return df,stock_code_tmp 



def get_html_data(all_df):
    if len(all_df) is 0:
        var = "data is null, please input correct code or name"
    else:

        del all_df['open']
        del all_df['high']
        del all_df['low']
        del all_df['volume']

        all_df['delta1']  =all_df.groupby('stock_code')['percent'].apply(lambda i:i.diff(-1))
        all_df['delta1_share'] = all_df.groupby('stock_code')['share_holding'].apply(lambda i:i.diff(-1))
        all_df['delta1_m'] = all_df['close'] * all_df['delta1_share'] / 10000;
        del all_df['delta1_share']

        all_df['delta2']  =all_df.groupby('stock_code')['percent'].apply(lambda i:i.diff(-2))
        all_df['delta3']  =all_df.groupby('stock_code')['percent'].apply(lambda i:i.diff(-3))
        all_df['delta5']  =all_df.groupby('stock_code')['percent'].apply(lambda i:i.diff(-5))
        all_df['delta10'] =all_df.groupby('stock_code')['percent'].apply(lambda i:i.diff(-10))
        all_df['delta21'] =all_df.groupby('stock_code')['percent'].apply(lambda i:i.diff(-21))
        all_df['delta120']=all_df.groupby('stock_code')['percent'].apply(lambda i:i.diff(-120))


        max_number=21
        #temp column added
        for index in range(1, max_number):
            column='money_sft_'+ str(index)
            all_df[column] = all_df.groupby('stock_code')['delta1_m'].shift(index*(-1))

        all_df=all_df.fillna(0)



        ll_df=hsgt_get_delta_m_of_day(all_df, 2)
        all_df=hsgt_get_delta_m_of_day(all_df, 3)
        all_df=hsgt_get_delta_m_of_day(all_df, 4)
        all_df=hsgt_get_delta_m_of_day(all_df, 5)
        all_df=hsgt_get_delta_m_of_day(all_df, 10)
        all_df=hsgt_get_delta_m_of_day(all_df, 21)

        all_df=all_df.round(2)

        #temp column delete
        for index in range(1, max_number):
            column='money_sft_'+ str(index)
            del all_df[column]

        del all_df['stock_cname']

        var = all_df.to_html()
    return var

all_df, stock_code_tmp = gete_df_and_stock_code(name)
xueqiu_url, finance_url=get_xueqiu_url(stock_code_tmp)
var = get_html_data(all_df)

#print(df['stock_cname'].head(10))


print ("Content-type: text/html")
print()

print ('<html lang="zh">')
print (' <head>')
print ('   <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">')
print ('   <title>hsgt search</title>')
print (' </head>')
print (' <body>')
print ('   <form action="hsgt-search.cgi">')
print ('   code or name <input type="text" name="name" />')
print ('   <input type="submit" />')
print ('   </form>')
print ('   <a href="%s" target="_blank"> %s[xueqiu]</a>' % (xueqiu_url, stock_code_tmp))
print ('   <a href="%s" target="_blank"> [finance]</a>' % (finance_url))
print ('   <a href="%s" target="_blank"> picture</a>' % ('../html/test.png'))
print ('   <p></p>')
print ('   %s'% var)
print (' </body>')
print ('</html>')
# print ('(xueqiu_url, stock_code_tmp, var)
   
plot_stock_picture(stock_code_tmp, name)
