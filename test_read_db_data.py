#!/usr/bin/env python  
# -*- coding: utf-8 -*-
# 2019-05-24, aaron


import psycopg2 #使用的是PostgreSQL数据库
import tushare as ts
from Stocks import *
from HData import *
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

#talib
import talib

#delete runtimer warning
import warnings
warnings.simplefilter(action = "ignore", category = RuntimeWarning)


today_date = time.strftime("%Y-%m-%d", time.localtime())
start = datetime.datetime(2018,10,1)


stocks=Stocks("usr","usr")
hdata=HData("usr","usr")

# stocks.db_stocks_create()#如果还没有表则需要创建
#print(stocks.db_stocks_update())#根据todayall的情况更新stocks表

#hdata.db_hdata_date_create()

nowdate=datetime.datetime.now().date()

codestock_local=stocks.get_codestock_local()
#print(codestock_local)

hdata.db_connect()#由于每次连接数据库都要耗时0.0几秒，故获取历史数据时统一连接
for i in range(0,len(codestock_local)):
#if (True):
    #i = 0
    nowcode=codestock_local[i][0]
    nowname=codestock_local[i][1]
    print("code:%s, name:%s" % (nowcode, nowname ))

    detail_info = hdata.get_all_hdata_of_stock(nowcode)
    detail_info = detail_info.tail(100)
    #print(detail_info)
    
    detail_info.index = detail_info.index.format(formatter=lambda x: x.strftime('%Y-%m-%d'))
    #print(detail_info.index[2])
    sma_5 = talib.SMA(np.array(detail_info['close']), 5)
    sma_13 = talib.SMA(np.array(detail_info['close']), 13)
    sma_21 = talib.SMA(np.array(detail_info['close']), 21)
    detail_info['k'], detail_info['d'] = talib.STOCH(detail_info['high'], detail_info['low'], detail_info['close'])
    detail_info['k'].fillna(value=0, inplace=True)
    detail_info['d'].fillna(value=0, inplace=True)

     # 调用talib计算MACD指标
    detail_info['MACD'],detail_info['MACDsignal'],detail_info['MACDhist'] = talib.MACD(np.array(detail_info['close']),
                                        fastperiod=6, slowperiod=12, signalperiod=9)   

    # dif: 12， 与26日的差别
    # dea:dif的9日以移动平均线
    # 计算MACD指标
    dif, dea, macd_hist = talib.MACD(np.array(detail_info['close']), fastperiod=12, slowperiod=26, signalperiod=9)


    #ma5 cross
    for i in range(1, sma_21.size):
        
        if (sma_5[i-1] < sma_13[i-1] and sma_5[i] > sma_13[i] and detail_info['close'][i] >  sma_5[i]):
            print(u"在第%d天:%s：ma5 cross ma13 :%d" % (i, detail_info.index[i],detail_info['close'][i]))


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
    ax.plot(sma_5, label='5日均線')
    ax.plot(sma_13, label='13日均線')
    ax.plot(sma_21, label='21日均線')

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

    ax.legend();
    ax2.legend();
    ax3.legend();
    figure_name = today_date + '-' +  nowcode + '-' + nowname + '.png'
    fig.savefig(figure_name)

    exec_command = "mkdir -p " + today_date
    os.system(exec_command)
    exec_command = "mv " + figure_name + " " + today_date
    os.system(exec_command)
    
    plt.cla()
    plt.clf()
    plt.close('all')


    #print(hdata.get_all_hdata_of_stock(nowcode))

    #print(i,nowcode,codestock_local[i][1])

hdata.db_disconnect()
