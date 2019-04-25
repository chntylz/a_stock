#!/usr/bin/env python
# -*- coding: utf-8 -*-

#tushare ���ݹ�����ͼ
import tushare as ts

#��̨����ͼƬ
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



# ��tushare��ȡ��������ת����candlestick_ohlc()�����ɶ�ȡ�ĸ�ʽ
data_list = []
hist_data = ts.get_hist_data('600199')
#hist_data = hist_data.head(5)

for dates,row in hist_data.iterrows():
    # ��ʱ��ת��Ϊ����
    date_time = datetime.datetime.strptime(dates,'%Y-%m-%d')
    t = date2num(date_time)
    open,high,low,close = row[:4]
    datas = (t,open,high,low,close)
    data_list.append(datas)
 
# ������ͼ
fig, ax = plt.subplots()
fig.subplots_adjust(bottom=0.2)
# ����X��̶�Ϊ����ʱ��
ax.xaxis_date()
plt.xticks(rotation=45)
plt.yticks()
#plt.title(u'��Ʊ���룺601558����K��ͼ')
#plt.xlabel(u'ʱ��')
#plt.ylabel(u'�ɼۣ�Ԫ�����ɼۣ�Ԫ��')
plt.title("stock number:601558")
plt.xlabel("time")
plt.ylabel("price yuan")
mpf.candlestick_ohlc(ax,data_list,width=1.5,colorup='r',colordown='green')
plt.grid()

plt.savefig("./test1.jpg")
plt.show()
plt.draw()
