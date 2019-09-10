#!/usr/bin/env python
# -*- coding: utf-8 -*- 
import os,sys
import cgi

import psycopg2 #使用的是PostgreSQL数据库
import tushare as ts
import numpy as np

from HData_hsgt import *

import  datetime




hdata_hsgt=HData_hsgt("usr","usr")
hdata_hsgt.db_connect()

form = cgi.FieldStorage()
name = form.getvalue('name', '000401')

if name.isdigit():
    df=hdata_hsgt.get_all_hdata_of_stock_code(name)
else:
    df=hdata_hsgt.get_all_hdata_of_stock_cname(name)

all_df=df

if len(all_df) is 0:
    var = "data is null, please input correct code or name"
else:
    all_df['delta1']  =all_df.groupby('stock_code')['percent'].apply(lambda i:i.diff(-1))
    all_df['delta1_share'] = all_df.groupby('stock_code')['share_holding'].apply(lambda i:i.diff(-1))
    all_df['delta1_money'] = all_df['close'] * all_df['delta1_share'] / 10000;
    del all_df['delta1_share']
    all_df['delta2']  =all_df.groupby('stock_code')['percent'].apply(lambda i:i.diff(-2))
    all_df['delta3']  =all_df.groupby('stock_code')['percent'].apply(lambda i:i.diff(-3))
    all_df['delta5']  =all_df.groupby('stock_code')['percent'].apply(lambda i:i.diff(-5))
    all_df['delta10'] =all_df.groupby('stock_code')['percent'].apply(lambda i:i.diff(-10))
    all_df['delta21'] =all_df.groupby('stock_code')['percent'].apply(lambda i:i.diff(-21))
    all_df['delta120']=all_df.groupby('stock_code')['percent'].apply(lambda i:i.diff(-120))



    all_df=all_df.round(2)

    del df['stock_cname']
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
    %s
  </body>
</html>
""" % (var)

