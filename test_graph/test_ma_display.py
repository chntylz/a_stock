#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 30 21:40:25 2018
https://blog.csdn.net/luoganttcc/article/details/80152807
Python 量化(四)计算股票的移动平均线
@author: luogan
"""
import tushare as ts
import talib
from matplotlib import pyplot as plt
#通过tushare获取股票信息
df=ts.get_k_data('002230',start='2018-01-12',end='2019-04-30')
#提取收盘价
closed=df['close'].values
#获取均线的数据，通过timeperiod参数来分别获取 5,10,20 日均线的数据。
ma5=talib.SMA(closed,timeperiod=30)
ma10=talib.SMA(closed,timeperiod=60)
ma20=talib.SMA(closed,timeperiod=250)

#打印出来每一个数据
print (closed)
print (ma5)
print (ma10)
print (ma20)


plt.figure(figsize=(12,8))
#通过plog函数可以很方便的绘制出每一条均线
plt.plot(closed)
plt.plot(ma5)
plt.plot(ma10)
plt.plot(ma20)
#添加网格，可有可无，只是让图像好看点
plt.grid()
#记得加这一句，不然不会显示图像
plt.show()
