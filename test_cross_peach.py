#!/usr/bin/env python  
# -*- coding: utf-8 -*-
# 2019-05-24, aaron


import psycopg2 #使用的是PostgreSQL数据库
import tushare as ts
from Stocks import *
from HData import *
from HData_day import *
from HData_select import *
from HData_60m import *
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
import datetime as datetime
import time
import os
import sys

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
hdata=HData_day("usr","usr")
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

hdata.db_connect()#由于每次连接数据库都要耗时0.0几秒，故获取历史数据时统一连接
sdata.db_connect()#由于每次连接数据库都要耗时0.0几秒，故获取历史数据时统一连接

start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
#all_info = hdata.my2_get_all_hdata_of_stock()
end_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
print("start_time: %s, end_time: %s" % (start_time, end_time))


#debug switch
debug = 0

#define canvas out of loop
plt.style.use('bmh')
fig = plt.figure(figsize=(24, 30),dpi=80)

'''
ax04  = fig.add_axes([0, 0.7, 1, 0.3])
ax04.grid()
ax03  = fig.add_axes([0, 0.4, 1, 0.3])
ax02 = fig.add_axes([0, 0.3, 1, 0.1])
ax01 = fig.add_axes([0, 0.2, 1, 0.1])
ax00 = fig.add_axes([0, 0,   1, 0.2])
'''

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
    if ('ST' in nowname or '68' in nowcode):
        #log.debug("ST: code:%s, name:%s" % (nowcode, nowname ))
        if debug:
            print("skip code: code:%s, name:%s" % (nowcode, nowname ))
        continue

    detail_info = hdata.get_limit_hdata_of_stock(nowcode, nowdate.strftime("%Y-%m-%d"), 300)
    #detail_info = hdata.get_limit_hdata_of_stock('000029',100) # test 'Exception: inputs are all NaN'
    #detail_info = all_info[all_info['stock_code'].isin([nowcode])]  #get date if nowcode == all_info['stock_code']
    #detail_info = detail_info.tail(100)
    if debug:
        print(detail_info)
   
    #fix NaN bug
    # if len(detail_info) == 0 or (detail_info is None):
    if len(detail_info) < 6  or (detail_info is None):
        # print('NaN: code:%s, name:%s' % (nowcode, nowname ))
        continue
 
    db_max_date = detail_info['record_date'][len(detail_info)-1].strftime("%Y%m%d")

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

    # dif: 12， 与26日的差别
    # dea:dif的9日以移动平均线
    # 计算MACD指标
    dif, dea, macd_hist = talib.MACD(np.array(detail_info['close'], dtype=float), fastperiod=12, slowperiod=26, signalperiod=9)
   
    upperband, middleband, lowerband = talib.BBANDS(np.array(detail_info['close']),timeperiod=20, nbdevdn=2, nbdevup=2)

       
    ##############################################################################
    '''
    桃园三结义的技术要点：

    1 股价要以阳线的形式上冲EXPMA（线 股价一定要站上这条线 光头阳线比带着影线的阳线强 涨停的光头阳线是最好的阳线

    2. MACD出现金叉 MACD一般有4种金叉，0轴之下的金叉 0轴之下的双次金叉 0轴之上的金叉 0轴之上的2次金叉。 一般是来说二次金叉>0轴上的金叉>0轴下的双次金叉>0轴之下的金叉

    3 BOLL突破中轨 一定是在中轨上方 有2中情况 第一种 从下面穿上来 第二种站在上方

    满足这3个条件 我们就叫桃园三结义。 。买入这种形态之后 就一路上涨。

    尤其是通过长期横盘之后的出现桃园三结义。这样横有多长，竖有多高。很容易出现翻翻行情。

    桃园三结义的买点：

    第一介入点条件达成当天尾盘

    第二介入点第二天开盘价如果开盘价过高。盘中低点买。

    第三介入点出现上面的2个买点后，如果错过，等回踩EXPMA）再买，或者加仓。
    '''
    #macd
    today_p = ((C - REF(C, 1))/REF(C, 1)) 
    today_p = round (today_p.value, 4)

    yes_p = ((REF(C, 1) - REF(C, 2))/REF(C, 2)) 
    yes_p = round (yes_p.value, 4)

    cond_1 = C > O and today_p > 0.01 and ( REF(C, 1) <  REF(EMA(C,12), 1) and C > EMA(C,12)) # C cross EMA12
    cond_2 = macd_cross(dif, dea) # macd gold cross
    cond_3 = (O < middleband[-1] and C > middleband[-1] ) or (O < C and O > middleband[-1] )  

    if cond_1 and (cond_2 == 1) and cond_3:
        draw_flag = True
        #insert into database
        dataframe_cols=['record_date','stock_code', 'stock_name', 'open', 'close', 'high', 'low', 'volume', 'p_change']
        # row=['2019-07-02',  '300750', 70.28,  71.04,  72.38,  69.96,  162519.0, 1.02]
        row=[nowdate, nowcode, nowname, O, C, H, L, V, today_p * 100]
        # print("row=%s" % (row))
        df=pd.DataFrame([row], columns=dataframe_cols)
        df=df.set_index('record_date')
        #insert to DB
        #sdata.insert_perstock_hdatadate(nowcode, df)
        print("[tao_yuan_san_jie_yi] peach and macd golden cross: code:%s, name:%s" % (nowcode, nowname ))
    #############################################################################


    ##############################################################################
    '''
    #cross
    cond_1 = CROSS(MA(C,5), MA(C, 13))
    cond_2 = CROSS(MA(C,13), MA(C, 21))
    cond_3 = C > MA(C, 5)
    cond_4 = V > MA(V, 50)
    cond_5 = ((C - REF(C, 1))/REF(C, 1)) > 0.03
    
    if cond_1 and cond_2 and cond_3 and cond_4 and cond_5:
        draw_flag = True
        print("cross: code:%s, name:%s" % (nowcode, nowname ))
    '''
    ##############################################################################



    
    #################################################################
    '''
    # 程序交易 （K线图数据，分钟/）
    # 使用程序的判断依据来模拟MACD指标交易情况，买入、卖出
    # 以200天为例，从第一天到第200天每天进行判断
    for i in range(1, dif.size):

        # 进行交易判断
        # DIF差离值和DEM讯号线的交替状态

        # 考虑买入的信号
        # 买入：金叉信号：昨天：DIF差离值 < DEA讯号线 and DIF差离值 > DEA讯号线 
        if dif[i-1] - dea[i-1] < 0 and dif[i] - dea[i] > 0:
            print("在第%d天:%s：买入了某某股票多少量的股票:%d" % (i, detail_info.index[i],detail_info['close'][i]))

        # 考虑卖出的信号
        # 买入：死叉信号：昨天：DIF差离值 > DEA讯号线 and DIF差离值 <  DEA讯号线
        if dif[i-1] - dea[i-1] > 0 and dif[i] - dea[i] < 0:
            print("在第%d天:%s：卖出了某某股票多少量的股票:%d" %  (i, detail_info.index[i],detail_info['close'][i]))



    plt.style.use('bmh')
    fig = plt.figure(figsize=(24, 30),dpi=160)
    '''
    ################################################################


    ################################################################
    #check need to generate png 
    if draw_flag == False:
        continue
    

    save_dir = 'stock_data'
    sub_name = '-peach'
    plot_picture(nowdate, nowcode, nowname, detail_info, save_dir, fig, sub_name) 
    ################################################################


shell_cmd='cp -rf stock_data/' + nowdate.strftime("%Y-%m-%d") +'*'  + ' /var/www/html/stock_data' +'/'
os.system(shell_cmd)
if debug:
    print('shell_cmd: %s' % shell_cmd)

plt.close('all')

hdata.db_disconnect()
sdata.db_disconnect()
last_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
print("start_time: %s, last_time: %s" % (start_time, last_time))
