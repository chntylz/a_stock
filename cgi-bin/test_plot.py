#!/usr/bin/env python  
# -*- coding: utf-8 -*-
# 2019-05-24, aaron


import tushare as ts
import  datetime

import matplotlib
matplotlib.use('Agg')


from zig import *


# basic
import numpy as np
import pandas as pd


# visual
import matplotlib.pyplot as plt
import mplfinance as mpf
#%matplotlib inline

from mplfinance.original_flavor import candlestick_ohlc, candlestick2_ochl, volume_overlay


#time
import datetime as datetime
import time
import os
import sys

#talib
import talib

#from Algorithm import *

#delete runtimer warning
import warnings
warnings.simplefilter(action = "ignore", category = RuntimeWarning)


#funcat
from funcat import *
from funcat.data.aaron_backend import AaronDataBackend
set_data_backend(AaronDataBackend())



#debug switch
debug = False;

def plot_picture(nowdate, nowcode, nowname, detail_info, save_dir, fig, sub_name):
    if debug:
        print("code:%s, name:%s" % (nowcode, nowname ))


    #skip ST
    if ('ST' in nowname):
        if debug:
            print("skip code: code:%s, name:%s" % (nowcode, nowname ))
        
        return

    if debug:
        print(detail_info)
    
    #fix NaN bug
    if len(detail_info) < 3  or (detail_info is None):
        # print('NaN: code:%s, name:%s' % (nowcode, nowname ))
        return
    
    #funcat call
    T(str(nowdate))
    S(nowcode)
    #print(str(nowdate), nowcode, nowname, O, H, L, C)
    today_p = ((C - REF(C, 1))/REF(C, 1))
    today_p = round (today_p.value, 4)


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
    # detail_info['MACD'],detail_info['MACDsignal'],detail_info['MACDhist'] = talib.MACD(np.array(detail_info['close']),
    #                                    fastperiod=6, slowperiod=12, signalperiod=9)   

    # dif: 12， 与26日的差别
    # dea:dif的9日以移动平均线
    # 计算MACD指标
    dif, dea, macd_hist = talib.MACD(np.array(detail_info['close'], dtype=float), fastperiod=12, slowperiod=26, signalperiod=9)

  
    plt.title(nowcode + ': ' + nowname)
    
    ax05  = fig.add_axes([0, 0.8, 1, 0.1])
    ax05.grid()
    ax04  = fig.add_axes([0, 0.55, 1, 0.25])
    ax03  = fig.add_axes([0, 0.3, 1, 0.25])
    ax02 = fig.add_axes([0, 0.2, 1, 0.1])
    ax01 = fig.add_axes([0, 0.1, 1, 0.1])
    ax00 = fig.add_axes([0, 0,   1, 0.1])

    #zig
    ax05.set_xticks(range(0, len(detail_info.index), 10))
    ax05.set_xticklabels(detail_info.index[::10])

    #add label and vlines for zig
    z_df, z_peers, z_d, z_k, z_buy_state =zig(detail_info)
    ax05.plot(z_df)
    z_len = len(z_peers)
    for i in range(z_len): 
        #print("i%d"%i)
        x1 = z_peers[i]
        y1 = z_df[z_peers[i]]

        text1=z_d[x1] + '-' + str(z_k[x1])
        ax05.annotate(text1, xy=(x1, y1 ), xytext=(x1+2 , y1), color="b",arrowprops=dict(facecolor='red', shrink=0.05))

        if i is 0 or i is (z_len - 1):
            #skip plot.vlines for first and last
            continue

        print("y1:%s" % y1)
        if z_buy_state[i] is 1:
            ax05.vlines(x1, 0, y1, colors='red')
        else:
            ax05.vlines(x1, 0, y1, colors='green')
        

    

    #boll, candles
    ax04.set_title(nowcode + '-' + nowname)
    ax04.set_xticks(range(0, len(detail_info.index), 10))
    ax04.set_xticklabels(detail_info.index[::10])
    candlestick2_ochl(ax04, detail_info['open'], detail_info['close'], detail_info['high'],
                                  detail_info['low'], width=0.6, colorup='r', colordown='g', alpha=0.75)
    #boll
    upperband, middleband, lowerband = talib.BBANDS(np.array(detail_info['close']),timeperiod=20, nbdevdn=2, nbdevup=2)
    ax04.plot(upperband, label="upper")
    ax04.plot(middleband, label="middle")
    ax04.plot(lowerband, label="bottom")

    #candles
    ax03.set_xticks(range(0, len(detail_info.index), 10))
    ax03.set_xticklabels(detail_info.index[::10])
    candlestick2_ochl(ax03, detail_info['open'], detail_info['close'], detail_info['high'],
                                  detail_info['low'], width=0.6, colorup='r', colordown='g', alpha=0.75)
    #plt.rcParams['font.sans-serif']=['Microsoft JhengHei'] 

    #k-line
    ax03.plot(ma_5, label='MA5')
    ax03.plot(ma_13, label='MA13')
    ax03.plot(ma_21, label='MA21')

    #kd
    ax02.plot(detail_info['k'], label='K-Value')
    ax02.plot(detail_info['d'], label='D-Value')
    ax02.set_xticks(range(0, len(detail_info.index), 10))
    ax02.set_xticklabels(detail_info.index[::10])

    #macd
    ax01.plot(dif, color="y", label="dif")
    ax01.plot(dea, color="b", label="dea")
    red_hist = np.where(macd_hist > 0 , macd_hist, 0)
    green_hist = np.where(macd_hist < 0 , macd_hist, 0)

    ax01.bar(detail_info.index, red_hist, label="Red-MACD", color='r')
    ax01.bar(detail_info.index, green_hist, label="Green-MACD", color='g')

    ax01.set_xticks(range(0, len(detail_info.index), 10))
    ax01.set_xticklabels(detail_info.index[::10])


    #volume
    volume_overlay(ax00, detail_info['open'], detail_info['close'], detail_info['volume'], colorup='r', colordown='g', width=0.5, alpha=0.8)
    ax00.set_xticks(range(0, len(detail_info.index), 10))
    ax00.set_xticklabels(detail_info.index[::10])
    ax00.plot(ma_vol_50, label='MA50')

    ax03.legend();
    ax02.legend();
    ax01.legend();
    save_name = nowdate.strftime("%Y-%m-%d-%w")
    figure_name = save_name + '-' +  nowcode +  \
                    '-' + str(int(round(O.value *100, 4))) + \
                    '-' + str(int(round(C.value *100, 4))) + \
                    '-' + str(int(round(H.value *100, 4))) + \
                    '-' + str(int(round(L.value *100, 4))) + \
                    '-' + str(int(today_p * 10000)) + '.png'
    fig.savefig(figure_name)

    exec_command = "mkdir -p " + save_dir
    os.system(exec_command)

    save_dir = save_dir + "/" + save_name + sub_name
    exec_command_1 = "mkdir -p " + save_dir
    os.system(exec_command_1)

    exec_command_2 = "mv " + save_name + '-' +  nowcode + '*' + " " + save_dir + "/"
    #print("%s"%(exec_command_2))
    os.system(exec_command_2)
    
    plt.clf()
    plt.cla()


    exec_command_3 = "cp -f " + save_dir + "/" + figure_name + " " + "/var/www/html/test.png" 
    #print("%s"%(exec_command_3))
    os.system(exec_command_3)
