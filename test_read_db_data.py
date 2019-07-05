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



# basic
import numpy as np
import pandas as pd

# get data
import pandas_datareader as pdr

# visual
import matplotlib.pyplot as plt
import mpl_finance as mpf
#%matplotlib inline
import seaborn as sns

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

nowdate=datetime.datetime.now().date()
#nowdate=nowdate-datetime.timedelta(1)

codestock_local=stocks.get_codestock_local()
#print(codestock_local)

hdata.db_connect()#由于每次连接数据库都要耗时0.0几秒，故获取历史数据时统一连接
sdata.db_connect()#由于每次连接数据库都要耗时0.0几秒，故获取历史数据时统一连接

start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
#all_info = hdata.my2_get_all_hdata_of_stock()
end_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
print("start_time: %s, end_time: %s" % (start_time, end_time))


#debug switch
debug = False;

#define canvas out of loop
plt.style.use('bmh')
fig = plt.figure(figsize=(24, 30),dpi=160)

'''
ax0  = fig.add_axes([0, 0.7, 1, 0.3])
ax0.grid()
ax  = fig.add_axes([0, 0.4, 1, 0.3])
ax2 = fig.add_axes([0, 0.3, 1, 0.1])
ax3 = fig.add_axes([0, 0.2, 1, 0.1])
ax4 = fig.add_axes([0, 0,   1, 0.2])
'''


for i in range(0,len(codestock_local)):
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
    if ('ST' in nowname):
        #log.debug("ST: code:%s, name:%s" % (nowcode, nowname ))
        if debug:
            print("skip code: code:%s, name:%s" % (nowcode, nowname ))
        continue

    detail_info = hdata.get_limit_hdata_of_stock(nowcode,100)
    #detail_info = hdata.get_limit_hdata_of_stock('000029',100) # test 'Exception: inputs are all NaN'
    #detail_info = all_info[all_info['stock_code'].isin([nowcode])]  #get date if nowcode == all_info['stock_code']
    #detail_info = detail_info.tail(100)
    if debug:
        print(detail_info)
    
    #fix NaN bug
    if len(detail_info) == 0 or (detail_info is None):
    	print('NaN: code:%s, name:%s' % (nowcode, nowname ))
    	continue
    
    #funcat call
    T(str(nowdate))
    S(nowcode)
    print(str(nowdate), nowcode, nowname, O, H, L, C)

    #continue
    detail_info.index = detail_info.index.format(formatter=lambda x: x.strftime('%Y-%m-%d'))
    #print(detail_info.index[2])
    
    detail_info['close'].fillna(value=0, inplace=True)   
    
    ma_5  = talib.MA(np.array(detail_info['close'], dtype=float), 5)
    ma_13 = talib.MA(np.array(detail_info['close'], dtype=float), 13)
    ma_21 = talib.MA(np.array(detail_info['close'], dtype=float), 21)
    if debug:
        print("ma_5.size:%d, ma_13.size:%d, ma_21.size:%d" % (ma_5.size, ma_13.size, ma_21.size))
    
    detail_info['k'], detail_info['d'] = talib.STOCH(detail_info['high'], detail_info['low'], detail_info['close'])
    detail_info['k'].fillna(value=0, inplace=True)
    detail_info['d'].fillna(value=0, inplace=True)
    
    #ma_vol
    ma_vol_50 = talib.MA(np.array(detail_info['volume'], dtype=float), 50)

     # 调用talib计算MACD指标
    detail_info['MACD'],detail_info['MACDsignal'],detail_info['MACDhist'] = talib.MACD(np.array(detail_info['close']),
                                        fastperiod=6, slowperiod=12, signalperiod=9)   

    # dif: 12， 与26日的差别
    # dea:dif的9日以移动平均线
    # 计算MACD指标
    dif, dea, macd_hist = talib.MACD(np.array(detail_info['close'], dtype=float), fastperiod=12, slowperiod=26, signalperiod=9)



    ##############################################################################
    #yitoujing
    today_p = ((C - REF(C, 1))/REF(C, 1)) 
    today_p = round (today_p.value, 4)

    yes_p = ((REF(C, 1) - REF(C, 2))/REF(C, 2)) 
    #yes_p = round (yes_p.value, 4)

    cond_1 = today_p > 0.03 and yes_p > 0.03
    cond_2 = C > MA(C, 21)
    cond_3 = MA(C, 21) > REF(MA(C, 21), 1)
    cond_4 = today_p > 0.095 and yes_p > 0.095
    if cond_1 and cond_2 and cond_3 and (cond_4 != True):
        draw_flag = True
        #insert into database
        dataframe_cols=['record_date','stock_code', 'stock_name', 'open', 'close', 'high', 'low', 'volume', 'p_change']
        # row=['2019-07-02',  '300750', 70.28,  71.04,  72.38,  69.96,  162519.0, 1.02]
        row=[nowdate, nowcode, nowname, O, C, H, L, V, today_p * 100]
        # print("row=%s" % (row))
        df=pd.DataFrame([row], columns=dataframe_cols)
        df=df.set_index('record_date')
        sdata.insert_perstock_hdatadate(nowcode, df)
        print("two day p > 0.03 : code:%s, name:%s" % (nowcode, nowname ))
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
	
    ################################################################

    plt.title(nowcode + ': ' + nowname) 
    
    ax0  = fig.add_axes([0, 0.7, 1, 0.3])
    ax0.grid()
    ax  = fig.add_axes([0, 0.4, 1, 0.3])
    ax2 = fig.add_axes([0, 0.3, 1, 0.1])
    ax3 = fig.add_axes([0, 0.2, 1, 0.1])
    ax4 = fig.add_axes([0, 0,   1, 0.2])


    #candles
    ax0.set_xticks(range(0, len(detail_info.index), 10))
    ax0.set_xticklabels(detail_info.index[::10])
    mpf.candlestick2_ochl(ax0, detail_info['open'], detail_info['close'], detail_info['high'],
                                  detail_info['low'], width=0.6, colorup='r', colordown='g', alpha=0.75)
    #boll
    upperband, middleband, lowerband = talib.BBANDS(np.array(detail_info['close']),timeperiod=20, nbdevdn=2, nbdevup=2)
    ax0.plot( upperband, label="上轨线")
    ax0.plot( middleband, label="中轨线")
    ax0.plot( lowerband, label="下轨线")

    #candles
    ax.set_xticks(range(0, len(detail_info.index), 10))
    ax.set_xticklabels(detail_info.index[::10])
    mpf.candlestick2_ochl(ax, detail_info['open'], detail_info['close'], detail_info['high'],
                                  detail_info['low'], width=0.6, colorup='r', colordown='g', alpha=0.75)
    #plt.rcParams['font.sans-serif']=['Microsoft JhengHei'] 

    #k-line
    ax.plot(ma_5, label='5日均線')
    ax.plot(ma_13, label='13日均線')
    ax.plot(ma_21, label='21日均線')

    #kd
    ax2.plot(detail_info['k'], label='K值')
    ax2.plot(detail_info['d'], label='D值')
    ax2.set_xticks(range(0, len(detail_info.index), 10))
    ax2.set_xticklabels(detail_info.index[::10])

    #macd
    ax3.plot(dif, color="y", label="差离值 dif")
    ax3.plot(dea, color="b", label="讯号线 dea")
    red_hist = np.where(macd_hist > 0 , macd_hist, 0)
    green_hist = np.where(macd_hist < 0 , macd_hist, 0)

    ax3.bar(detail_info.index, red_hist, label="红色MACD值", color='r')
    ax3.bar(detail_info.index, green_hist, label="绿色MACD值", color='g')

    ax3.set_xticks(range(0, len(detail_info.index), 10))
    ax3.set_xticklabels(detail_info.index[::10])


    #volume
    mpf.volume_overlay(ax4, detail_info['open'], detail_info['close'], detail_info['volume'], colorup='r', colordown='g', width=0.5, alpha=0.8)
    ax4.set_xticks(range(0, len(detail_info.index), 10))
    ax4.set_xticklabels(detail_info.index[::10])
    ax4.plot(ma_vol_50, label='50日均線')

    ax.legend();
    ax2.legend();
    ax3.legend();
    save_name = nowdate.strftime("%Y-%m-%d")
    figure_name = save_name + '-' +  nowcode + '-' + nowname + '.png'
    fig.savefig(figure_name)

    exec_command = "mkdir -p " + save_name
    os.system(exec_command)
    exec_command = "mv " + save_name + '-' +  nowcode + '*' + " " + save_name
    os.system(exec_command)
    
    plt.clf()
    plt.cla()


    #print(hdata.get_all_hdata_of_stock(nowcode))

    #print(i,nowcode,codestock_local[i][1])
plt.close('all')

hdata.db_disconnect()
sdata.db_disconnect()
last_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
print("start_time: %s, last_time: %s" % (start_time, last_time))
