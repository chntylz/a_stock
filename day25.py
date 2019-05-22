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

#talib
import talib

#delete runtimer warning
import warnings
warnings.simplefilter(action = "ignore", category = RuntimeWarning)

start = datetime.datetime(2018,10,1)
df_2330 = pdr.DataReader('2330.TW', 'yahoo', start=start)
#print(df_2330)
df_2330.index = df_2330.index.format(formatter=lambda x: x.strftime('%Y-%m-%d'))
#print(df_2330.index[2])
sma_5 = talib.SMA(np.array(df_2330['Close']), 5)
sma_13 = talib.SMA(np.array(df_2330['Close']), 13)
sma_21 = talib.SMA(np.array(df_2330['Close']), 21)
df_2330['k'], df_2330['d'] = talib.STOCH(df_2330['High'], df_2330['Low'], df_2330['Close'])
df_2330['k'].fillna(value=0, inplace=True)
df_2330['d'].fillna(value=0, inplace=True)

 # 调用talib计算MACD指标
df_2330['MACD'],df_2330['MACDsignal'],df_2330['MACDhist'] = talib.MACD(np.array(df_2330['Close']),
                                    fastperiod=6, slowperiod=12, signalperiod=9)   

# dif: 12， 与26日的差别
# dea:dif的9日以移动平均线
# 计算MACD指标
dif, dea, macd_hist = talib.MACD(np.array(df_2330['Close']), fastperiod=12, slowperiod=26, signalperiod=9)


#ma5 cross
for i in range(1, sma_21.size):
        if sma_5[i-1] < sma_13[i-1] and sma_5[i] > sma_13[i] and df_2330['Close'][i] >  sma_5[i]:
                print("在第%d天:%s：ma5 cross ma13 :%d" % (i, df_2330.index[i],df_2330['Close'][i]))


# 程序交易 （K线图数据，分钟/）
# 使用程序的判断依据来模拟MACD指标交易情况，买入、卖出
# 以200天为例，从第一天到第200天每天进行判断
for i in range(1, dif.size):

    # 进行交易判断
    # DIF差离值和DEM讯号线的交替状态

    # 考虑买入的信号
    # 买入：金叉信号：昨天：DIF差离值 < DEA讯号线 and DIF差离值 > DEA讯号线 
    if dif[i-1] - dea[i-1] < 0 and dif[i] - dea[i] > 0:
        print("在第%d天:%s：买入了某某股票多少量的股票:%d" % (i, df_2330.index[i],df_2330['Close'][i]))

    # 考虑卖出的信号
    # 买入：死叉信号：昨天：DIF差离值 > DEA讯号线 and DIF差离值 <  DEA讯号线
    if dif[i-1] - dea[i-1] > 0 and dif[i] - dea[i] < 0:
        print("在第%d天:%s：卖出了某某股票多少量的股票:%d" %  (i, df_2330.index[i],df_2330['Close'][i]))




plt.style.use('bmh')
fig = plt.figure(figsize=(24, 20))

ax0  = fig.add_axes([0, 0.8, 1, 0.4])
ax0.grid()
ax  = fig.add_axes([0, 0.4, 1, 0.4])
ax2 = fig.add_axes([0, 0.3, 1, 0.1])
ax3 = fig.add_axes([0, 0.2, 1, 0.1])
ax4 = fig.add_axes([0, 0,   1, 0.2])


#candles
ax0.set_xticks(range(0, len(df_2330.index), 10))
ax0.set_xticklabels(df_2330.index[::10])
mpf.candlestick2_ochl(ax0, df_2330['Open'], df_2330['Close'], df_2330['High'],
                              df_2330['Low'], width=0.6, colorup='r', colordown='g', alpha=0.75)
#boll
upperband, middleband, lowerband = talib.BBANDS(np.array(df_2330['Close']),timeperiod=20, nbdevdn=2, nbdevup=2)
ax0.plot( upperband, label="上轨线")
ax0.plot( middleband, label="中轨线")
ax0.plot( lowerband, label="下轨线")

#candles
ax.set_xticks(range(0, len(df_2330.index), 10))
ax.set_xticklabels(df_2330.index[::10])
mpf.candlestick2_ochl(ax, df_2330['Open'], df_2330['Close'], df_2330['High'],
                              df_2330['Low'], width=0.6, colorup='r', colordown='g', alpha=0.75)
#plt.rcParams['font.sans-serif']=['Microsoft JhengHei'] 

#k-line
ax.plot(sma_5, label='5日均線')
ax.plot(sma_13, label='13日均線')
ax.plot(sma_21, label='21日均線')

#kd
ax2.plot(df_2330['k'], label='K值')
ax2.plot(df_2330['d'], label='D值')
ax2.set_xticks(range(0, len(df_2330.index), 10))
ax2.set_xticklabels(df_2330.index[::10])

#macd
ax3.plot(dif, color="y", label="差离值 dif")
ax3.plot(dea, color="b", label="讯号线 dea")
red_hist = np.where(macd_hist > 0 , macd_hist, 0)
green_hist = np.where(macd_hist < 0 , macd_hist, 0)

ax3.bar(df_2330.index, red_hist, label="红色MACD值", color='r')
ax3.bar(df_2330.index, green_hist, label="绿色MACD值", color='g')

ax3.set_xticks(range(0, len(df_2330.index), 10))
ax3.set_xticklabels(df_2330.index[::10])


#volume
mpf.volume_overlay(ax4, df_2330['Open'], df_2330['Close'], df_2330['Volume'], colorup='r', colordown='g', width=0.5, alpha=0.8)
ax4.set_xticks(range(0, len(df_2330.index), 10))
ax4.set_xticklabels(df_2330.index[::10])

ax.legend();
ax2.legend();
ax3.legend();

fig.savefig('day25_01.png')
