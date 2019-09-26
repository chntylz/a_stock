#!/usr/bin/env python
# -*- coding: utf-8 -*- 
import os,sys
import cgi

import psycopg2 #使用的是PostgreSQL数据库
import tushare as ts
import numpy as np

from HData_hsgt import *

import  datetime


debug=0

hdata_hsgt=HData_hsgt("usr","usr")
hdata_hsgt.db_connect()

form = cgi.FieldStorage()
name = form.getvalue('name', '000401')


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




if name.isdigit():
    df=hdata_hsgt.get_all_hdata_of_stock_code(name)
else:
    df=hdata_hsgt.get_all_hdata_of_stock_cname(name)

all_df=df

if len(all_df) is 0:
    var = "data is null, please input correct code or name"
    xueqiu_url=''
    stock_code_tmp=name
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



    stock_code_tmp=all_df['stock_code'][0]
    stock_cname_tmp=all_df['stock_cname'][0]
    if stock_code_tmp[0:2] == '60':
        stock_code_new='SH'+stock_code_tmp
    else:
        stock_code_new='SZ'+stock_code_tmp
    xueqiu_url='https://xueqiu.com/S/' + stock_code_new


    del all_df['stock_cname']

    var = all_df.to_html()

#print(df['stock_cname'].head(10))


print """Content-type: text/html

<html lang="zh">
  <head>
    <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
    <title>hsgt search</title>
  </head>
  <body>
    <form action='hsgt-search.cgi'>
    code or name <input type='text' name='name' />
    <input type='submit' />
    </form>
    <a href="%s" target="_blank"> %s[xueqiu]</a>
    <p></p>
    %s
  </body>
</html>
""" % (xueqiu_url, stock_code_tmp, var)

