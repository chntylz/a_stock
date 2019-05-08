#!/usr/bin/env python  
# -*- coding: utf-8 -*-


from jqdatasdk import *

auth("13661401357", "820820")


#pyecharts文档请见http://pyecharts.org
from pyecharts import Kline, online,Overlap,Scatter,Style,Line,Grid,Bar

import talib
class Kshow:
    def __init__(self,stock_name,jsPath='https://cdn.bootcss.com/echarts/4.1.0.rc2'):
        #使用最新的echarts js文件
        online(jsPath)
        #K线
        self.kline=Kline(stock_name)
        #macd line，即dif、dea
        self.macd_line=Line('')
        self.macd_bar=Bar('')
    
    def chart_init(self,data_df,show_BI=True,show_Line=True,show_Center=True):
        #获取K线数据
        def get_K_data(df):
            kdata=df.loc[:,['open','close','low','high']].to_dict('split')['data']
            xaxis=list(df.index.tolist())
            return xaxis,kdata
        #获取MACD数据
        def get_macd_data(df):
            dif=[l for l in df.dif.iteritems()]
            dea=[l for l in df.dea.iteritems()]
            macd_z=[l for l in df[df.macd>=0].macd.iteritems()]
            macd_f=[l for l in df[df.macd<0].macd.iteritems()]
            return dif,dea,macd_z,macd_f
        
        kx,ky=get_K_data(data_df)
        #添加K线，is_datazoom_show显示时间轴，datazoom_range时间轴范围
        self.kline.add('K',kx,ky,is_datazoom_show=True,datazoom_range=[80,100])
        #修改K线颜色
        self.kline._option['series'][0]['itemStyle']={'normal':{'color':'#ef232a', 'color0': '#14b143','borderColor': '#ef232a', 'borderColor0': '#14b143'}}
        
        dif,dea,macd_z,macd_f=get_macd_data(data_df)
        self.macd_bar.add('macd_z',[t for t,v in macd_z],macd_z,is_datazoom_show=True)
        self.macd_bar.add('macd_f',[t for t,v in macd_f],macd_f,is_datazoom_show=True)
        self.macd_line.add('dif',kx,dif,is_datazoom_show=True, xaxis_type='category',line_width =1,
                           is_symbol_show=False,is_fill=False)
        self.macd_line.add('dea',kx,dea,is_datazoom_show=True, xaxis_type='category',line_width =1,
                           is_symbol_show=False,is_fill=False)
        
        #这个应该是目前版本pyechart的bug，导致区域填充，删除后只显示线
        del chart.macd_line._option['series'][0]['areaStyle']
        del chart.macd_line._option['series'][1]['areaStyle']
    
    #主图
    def get_main_chart(self,height=600,width=1000):
        overlap = Overlap(height=height,width=height)
        overlap.add(self.kline)
        
        return overlap
    
    #辅图
    def get_macd_chart(self,height=200,width=1000):
        overlap = Overlap(height=height,width=height)
        overlap.add(self.macd_line)
        overlap.add(self.macd_bar)
        return overlap
    
    #获取图，并显示
    def show_chart(self,zoom_start=50,height=600,width=1000):
        grid = Grid(height=height,width =width)
        
        main_ov=self.get_main_chart(height-200,width)
        macd_ov=self.get_macd_chart(200,width)
        
        grid.add(main_ov,grid_top=0,grid_bottom=220)
        grid.add(macd_ov,grid_top=height-200)
        
        grid._option['dataZoom'][0]['xAxisIndex']=[0, 1]
        grid._option['dataZoom'][0]['start']=zoom_start
        grid._option['xAxis'][1]['show']=False
        grid._option['legend'][1]['show']=False
        grid._option['color']=['#145b7d','#e0861a','#ef232a','#14b143'] #DIF，DEA，MACD正，MACD负
        return grid
    
	
	
	
#股票代码
stock='000001.XSHG'
#名称
stock_name=get_security_info(stock).display_name


#获取dataframe
df=get_price(stock,end_date='2018-06-10',fields=['open','close','high','low'],frequency='1d',count=600)

#计算macd
df['dif'],df['dea'],df['macd']=talib.MACD(df.close.values, 
                                        fastperiod=12 , 
                                        slowperiod=26, 
                                        signalperiod=9 )
#macd最开始一段数据不准，移除
df=df.iloc[100:,:]
#初始化画图函数
chart=Kshow(stock[:6]+' '+stock_name)
#初始化数据
chart.chart_init(df)
#显示图表
chart.show_chart(height=600,width=1000)	

#以下代码可以在研究中生成图表大图 render.html
chart.show_chart(height=600,width=1000).render()