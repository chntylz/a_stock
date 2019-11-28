#!/usr/bin/env python
# -*- coding: utf-8 -*- 
import os,sys,time
import cgi

import tushare as ts
import numpy as np
import pandas as pd




def show_realdata():
    my_list=['300750','300552', '000401', '300458','300014']
    my_list_cn=['ndsd','wjkj', 'jdsn', 'qzkj', 'ywld']
    my_price=[]
    for code in my_list:
        df = ts.get_realtime_quotes(code)
        my_price.append(df['price'][0])
        #print(i, my_price)
    ret_df=pd.DataFrame(my_price, index=my_list_cn)
    return ret_df



df=show_realdata()


print """Content-type: text/html\r\n\r\n

<html lang="zh">
  <head>
    <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
    <meta http-equiv="refresh" content="5">
    <title>comm_update</title>
  </head>
  <body>
   %s 
  </body>
</html>
""" % (df.to_html())

