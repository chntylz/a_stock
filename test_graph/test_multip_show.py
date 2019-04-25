#!/usr/bin/env python  
# -*- coding: utf-8 -*-

import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
import numpy as np


#先引入后面分析、可视化等可能用到的库
import tushare as ts
import pandas as pd  
import matplotlib.pyplot as plt

#正常显示画图时出现的中文和负号
from pylab import mpl
mpl.rcParams['font.sans-serif']=['SimHei']
mpl.rcParams['axes.unicode_minus']=False

#设置token
token='21dddafc47513ea46b89057b2c4edf7b44882b3e92274b431f199552'
#ts.set_token(token)
pro = ts.pro_api(token)


#获取当前上市的股票代码、简称、注册地、行业、上市时间等数据
basic=pro.stock_basic(list_status='L')
#查看前五行数据
#basic.head(5)

#获取平安银行日行情数据
pa=pro.daily(ts_code='000001.SZ', start_date='20180101',
               end_date='20190106')
#pa.head()

#K线图可视化
from pyecharts import Kline
pa.index=pd.to_datetime(pa.trade_date)
pa=pa.sort_index()
v1=list(pa.loc[:,['open','close','low','high']].values)
t=pa.index
v0=list(t.strftime('%Y%m%d'))
kline = Kline("平安银行K线图",title_text_size=15)
kline.add("", v0, v1,is_datazoom_show=True,
         mark_line=["average"],
         mark_point=["max", "min"],
         mark_point_symbolsize=60,
         mark_line_valuedim=['highest', 'lowest'] )
kline.render("上证指数图.html")
kline

my_path='/home/aaron/aaron/test_graph/stock_data/'

#定义获取多只股票函数：
def get_stocks_data(stocklist,start,end):
    all_data={}
    for code in stocklist:
        all_data[code]=pro.daily(ts_code=code,
                 start_date=start, end_date=end)
    return all_data

#保存本地
def save_data(all_data):
    for code,data in all_data.items():
        data.to_csv(my_path+code+'.csv',
                     header=True, index=False)

stocklist=list(basic.ts_code)[:15]
start=''
end=''
all_data=get_stocks_data(stocklist,start,end)

all_data['000002.SZ'].tail()

#将数据保存到本地
save_data(all_data)

#读取本地文件夹里所有文件
import os
#文件存储路径
file=my_path
g=os.walk(file)
filenames=[]
for path,d,filelist in g:
    for filename in filelist:
        filenames.append(os.path.join(filename))
print(filenames)

#将读取的数据文件放入一个字典中
df={}
#从文件名中分离出股票代码
code=[name.split('.')[0] for name in filenames]
for i in range(len(filenames)):
    filename=file+filenames[i]
    df[code[i]]=pd.read_csv(filename)

#查看第一只股票前五行数据
#df[code[0]].head()


def get_index_data(indexs):
    '''indexs是字典格式'''
    index_data={}
    for name,code in indexs.items():
        df=pro.index_daily(ts_code=code)
        df.index=pd.to_datetime(df.trade_date)   
        index_data[name]=df.sort_index()
    return index_data

#获取常见股票指数行情
indexs={'shindex': '000001.SH','szindex': '399001.SZ',
         'hs300': '000300.SH','cyb': '399006.SZ',
          'sh50': '000016.SH', 'zz500': '000905.SH',
         'zxb': '399005.SZ','sh180': '000010.SH'}
index_data=get_index_data(indexs)
#index_data['shindex'].head()

#对股价走势进行可视化分析
subjects =list(index_data.keys())
print(subjects)
#每个子图的title
plot_pos = [421,422,423,424,425,426,427,428] # 每个子图的位置
new_colors = ['#1f77b4','#ff7f0e', '#2ca02c', '#d62728',
             '#9467bd','#8c564b', '#e377c2', 
             '#7f7f7f','#bcbd22','#17becf']

fig = plt.figure(figsize=(16,18))
#fig.suptitle('A股股指走势',fontsize=18)
fig.suptitle('A stock trend',fontsize=18)
for pos in np.arange(len(plot_pos)):       
    ax = fig.add_subplot(plot_pos[pos]) 
    y_data =index_data[subjects[pos]]['close']    
    b = ax.plot(y_data,color=new_colors[pos])   
    print(subjects[pos])
    ax.set_title(subjects[pos])    
    # 将右上边的两条边颜色设置为空，相当于抹掉这两条边
    ax = plt.gca()  
    ax.spines['right'].set_color('none') 
    ax.spines['top'].set_color('none')    
	
plt.show()  


plt.savefig("/home/aaron/aaron/test_graph/test_multip.jpg") 
