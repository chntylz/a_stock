#!/usr/bin/env python  
# -*- coding: utf-8 -*-
# 2019-05-24, aaron

import datetime as datetime
import time
import os
import sys
sys.path.append('pwsnow_ball')

import psycopg2 #使用的是PostgreSQL数据库
from Stocks import *
from HData_day import *
from HData_select import *
from pysnow_ball.HData_xq_day import *
import  datetime


import matplotlib
matplotlib.use('Agg')



from zig import *
from test_plot import *
from file_interface import *




# basic
import numpy as np
import pandas as pd


# visual
import matplotlib.pyplot as plt
import mplfinance as mpf
#%matplotlib inline

#time

#talib
import talib


from funcat import *

from Algorithm import *

#delete runtimer warning
import warnings
warnings.simplefilter(action = "ignore", category = RuntimeWarning)

#log
from common import Log
log = Log(__name__).getlog()

#funcat
from funcat import *
from funcat.data.aaron_backend import AaronDataBackend
set_data_backend(AaronDataBackend())


################################################################
today_date = time.strftime("%Y-%m-%d", time.localtime())
start = datetime.datetime(2018,10,1)

stocks=Stocks("usr","usr")
hdata=HData_xq_day("usr","usr")
sdata=HData_select("usr","usr")

# stocks.db_stocks_create()#如果还没有表则需要创建
#print(stocks.db_stocks_update())#根据todayall的情况更新stocks表

#hdata.db_hdata_date_create()

#print("line number: " + str(sys._getframe().f_lineno) )
#sdata.db_hdata_date_create()
#print("line number: " + str(sys._getframe().f_lineno) )
######################################################################


from sys import argv
# 如果执行的方式错误输出使用方法
USAGE = '''
用法错误，正确方式如下：
python demo.py 1
'''
if len(argv) > 2:
    print(USAGE)  # 如果传入的参数不足，输出正确用法
    exit(1) # 异常退出(下面的代码将不会被执行)

script_name, para1 = argv  # 将传入的参数赋值进行使用
print("%s, %d"%(script_name, int(para1)))


nowdate=datetime.datetime.now().date()
nowdate=nowdate-datetime.timedelta(int(para1))
print("nowdate is %s"%(nowdate.strftime("%Y-%m-%d"))) 

codestock_local=stocks.get_codestock_local()
#print(codestock_local)

sdata.db_connect()#由于每次连接数据库都要耗时0.0几秒，故获取历史数据时统一连接

start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
#all_info = hdata.my2_get_all_hdata_of_stock()
end_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
print("start_time: %s, end_time: %s" % (start_time, end_time))


#debug switch
debug = 0
#debug = 1

clean_flag = True

#define canvas out of loop
plt.style.use('bmh')
fig = plt.figure(figsize=(24, 30),dpi=80)

stock_len=len(codestock_local)
for i in range(0,stock_len):
#for i in range(0,2):
#if (True):
    #i = 0
    draw_flag = False

    nowcode=codestock_local[i][0]
    nowname=codestock_local[i][1]
    log.debug("code:%s, name:%s" % (nowcode, nowname ))
    if debug:
        print("code:%s, name:%s" % (nowcode, nowname ))


    #skip ST
    #if ('ST' in nowname or '300' in nowcode):
    if ('ST' in nowname) or  ('300028' in nowcode): #300028-金亚科技
        #log.debug("ST: code:%s, name:%s" % (nowcode, nowname ))
        if debug:
            print("skip code: code:%s, name:%s" % (nowcode, nowname ))
        continue

    if nowcode[0:1] == '6':
        stock_code_new= 'SH' + nowcode
    else:
        stock_code_new= 'SZ' + nowcode
    detail_info = hdata.get_data_from_hdata(stock_code=stock_code_new, \
            end_date=nowdate.strftime("%Y-%m-%d"), limit=600)
    #detail_info = all_info[all_info['stock_code'].isin([nowcode])]  #get date if nowcode == all_info['stock_code']
    #detail_info = detail_info.tail(100)
    if debug:
        print(detail_info)

    #fix NaN bug
    # if len(detail_info) == 0 or (detail_info is None):
    if len(detail_info) < 6  or (detail_info is None):
        # print('NaN: code:%s, name:%s' % (nowcode, nowname ))
        continue
    

    db_max_date = detail_info['record_date'][len(detail_info)-1]
    db_max_date = db_max_date.replace('-','')

    if time_is_equal(db_max_date, nowdate.strftime("%Y%m%d")):
        if debug:
            print('date is ok')
    else:
        #invalid data, skip this
        print('###error###: nowcode:%s, database max date:%s, nowdate:%s' % (nowcode, db_max_date, nowdate.strftime("%Y%m%d")))
        continue

    
    
    #funcat call
    T(str(nowdate))
    S(nowcode)
    # print(str(nowdate), nowcode, nowname, O, H, L, C)

    #zig condition
    z_df, z_peers, z_d, z_k, z_buy_state=zig(detail_info)
    z_len = len(z_peers)
    #calculate buy or sell
    if z_len >= 3:  # it should have one valid zig data at least
        if z_peers[-1] - z_peers[-2] < 3: #delta days  < 3 from today
            if z_buy_state[-2] is 1:  #valid zig must 1, that means valley
                print('%s gold node, buy it!!'% nowcode)
                draw_flag = True
    
   ################################################################


    ################################################################
    #check need to generate png 
    if draw_flag == False:
        continue
   

    save_dir = 'stock_data'
    sub_name = '-zig'

    #################### delete begin ##################
    if clean_flag:
        clean_flag = False
        remove_dir(nowdate, save_dir, sub_name)
    #################### delete end ##################


    plot_picture(nowdate, nowcode, nowname, detail_info, save_dir, fig, sub_name) 
    ################################################################


shell_cmd='cp -rf stock_data/' + nowdate.strftime("%Y-%m-%d") +'*'  + ' /var/www/html/stock_data' +'/'
os.system(shell_cmd)
if debug:
    print('shell_cmd: %s' % shell_cmd)

plt.close('all')

sdata.db_disconnect()
last_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
print("start_time: %s, last_time: %s" % (start_time, last_time))

