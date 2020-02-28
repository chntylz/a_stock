#!/usr/bin/env python
# -*- coding: utf-8 -*- 
import os,sys,time
import cgi

import tushare as ts
import numpy as np
import pandas as pd




def show_realdata():
    my_list=['300750','300552', '000401', '300458','300014', '601958', '601117', '600588', '002230']
    my_list_cn=['ningdeshidai','wanjikeji', 'jidongshuini', 'quanzhikeji', 'yiweilidian', 'jinmugufen', 'zhongguohuaxue', 'yongyouwangluo', 'kedaxunfei']
    data_list = []
    for i in range(len(my_list)):
        df = ts.get_realtime_quotes(my_list[i])
        data_list.append([my_list_cn[i], df['pre_close'][0], df['price'][0] ])

    data_column = ['name', 'pre_price', 'price']
    ret_df=pd.DataFrame(data_list, columns=data_column)
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
