#!/usr/bin/env python  
# -*- coding: utf-8 -*-
# 2019-05-24, aaron
#console.cloud.tencent.com

import time
import sys
import os
import talib

import numpy as np
import pandas as pd

from HData_day import *

#funcat
from funcat import *
from funcat.data.aaron_backend import AaronDataBackend
set_data_backend(AaronDataBackend())


hdata=HData_day('usr', 'usr')
hdata.db_connect()


nowdate = '2019-07-12'
nowcode = '300750'
detail_info = hdata.get_all_hdata_of_stock(nowcode)
#print(detail_info.head(10))

detail_date = detail_info['record_date']
#print(detail_date.head(10))

detail_date_list = list(detail_date.apply(lambda x: str(x)))
#print(detail_date_list)


loop = 0
buy_price=0
sell_price=0
buy_flag=False
red_times=0
green_times=0
for curr_day in detail_date_list:
    loop = loop + 1
    
    if loop < 20:
        continue

    # print(curr_day)
    T(curr_day)
    S(nowcode)


    K,D,J = KDJ()
    K=round(K.value, 2)
    D=round(D.value, 2)
    J=round(J.value, 2)
    #print( O, C, H, L)
    #print( K, D, J)


    df = hdata.get_limit_hdata_of_stock(nowcode, curr_day, 100)
    #print(df.head(10))
    df['k'], df['d'] = talib.STOCH(df['high'], df['low'], df['close'],
            fastk_period=9,
            slowk_period=3,
            slowk_matype=0,
            slowd_period=3,
            slowd_matype=0)
    df['k'].fillna(value=0, inplace=True)
    df['d'].fillna(value=0, inplace=True)
    k_value = df['k']
    d_value = df['d']

    kd_gold_cross = k_value[-1] > d_value[-1] and k_value[-2] < d_value[-2]
    kd_dead_cross = k_value[-1] < d_value[-1] and k_value[-2] > d_value[-2]

    # dif: 12， 与26日的差别
    # dea:dif的9日以移动平均线
    # 计算MACD指标
    dif, dea, macd_hist = talib.MACD(np.array(df['close'], dtype=float), fastperiod=12, slowperiod=26, signalperiod=9)
    macd = dif - dea
    cond_macd = macd[-1] > 0



    '''
    if kd_gold_cross and buy_flag is False: 
        buy_flag = True
        buy_price = df['close'][-1]
        print("%s gold: k cross d, k=%d, d=%d, j=%d" % (curr_day, K, D, J))
        print( O, C, H, L)
        #print( K, D, J)

    if kd_dead_cross and buy_flag :
        buy_flag = False
        sell_price = df['close'][-1]
        print("%s dead: d cross k, k=%d, d=%d, j=%d" % (curr_day, K, D, J))
        print( O, C, H, L)
        #print( K, D, J)
        print( buy_price, sell_price)
        if sell_price > buy_price :
            red_times += 1
            print('+++++++++++++++++++')
        else:
            green_times += 1
            print('-------------------')
    '''

    if D > 50 and kd_gold_cross and (J > D + 15) and cond_macd and V > MA(V, 50):
        print ("%s buy, price is %f" % (curr_day, df['close'][-1]))

print('red_times=%d, green_times=%d' % (red_times, green_times ))

