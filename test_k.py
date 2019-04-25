#!/usr/bin/env python
# -*- coding: utf-8 -*-

#tushare 数据规整画图
import tushare as ts

#后台保存图片
import matplotlib as mpl
mpl.use('Agg')

#import matplotlib.pyplot as plt
import matplotlib.finance as mpf
from matplotlib.pylab import date2num
import datetime


import sys
reload(sys)
#sys.setdefaultencoding('utf-8')
sys.setdefaultencoding("ISO-8859-1") 

"""a demo of matplotlib"""
import matplotlib as  mpl
from matplotlib  import pyplot as plt
mpl.rcParams[u'font.sans-serif'] = ['simhei']
mpl.rcParams['axes.unicode_minus'] = False



# 对tushare获取到的数据转换成candlestick_ohlc()方法可读取的格式
data_list = []
hist_data = ts.get_hist_data('600199')
#hist_data = hist_data.head(5)

for dates,row in hist_data.iterrows():
    # 将时间转换为数字
    date_time = datetime.datetime.strptime(dates,'%Y-%m-%d')
    t = date2num(date_time)
    open,high,low,close = row[:4]
    datas = (t,open,high,low,close)
    data_list.append(datas)
 
# 创建子图
fig, ax = plt.subplots()
fig.subplots_adjust(bottom=0.2)
# 设置X轴刻度为日期时间
ax.xaxis_date()
plt.xticks(rotation=45)
plt.yticks()
#plt.title(u'股票代码：601558两年K线图')
#plt.xlabel(u'时间')
#plt.ylabel(u'股价（元）’股价（元）')
plt.title("stock number:601558")
plt.xlabel("time")
plt.ylabel("price yuan")
mpf.candlestick_ohlc(ax,data_list,width=1.5,colorup='r',colordown='green')
plt.grid()

plt.savefig("./test1.jpg")
plt.show()
plt.draw()
