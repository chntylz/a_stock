#!/usr/bin/env python  
# -*- coding: utf-8 -*-
# 2019-05-24, aaron
#time
import datetime as datetime
import time
import sys
import os
sys.path.append("..")


import psycopg2 #使用的是PostgreSQL数据库
from Stocks import *
from HData_xq_day import *
import  datetime

from zig import *
from file_interface import *

from test_cross_quad import get_cross_info

# basic
import numpy as np
import pandas as pd

#talib
import talib

from funcat import *

#delete runtimer warning
import warnings
warnings.simplefilter(action = "ignore", category = RuntimeWarning)

#funcat
from funcat import *
from funcat.data.aaron_backend import AaronDataBackend
set_data_backend(AaronDataBackend())

from test_get_basic_data import *
from file_interface import *
from comm_interface import *


stocks=Stocks("usr","usr")
hdata=HData_xq_day("usr","usr")

# stocks.db_stocks_create()#如果还没有表则需要创建
#print(stocks.db_stocks_update())#根据todayall的情况更新stocks表



#debug switch
debug = 0
#debug = 1

def calculate_peach_zig_quad(nowdate):

    start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    codestock_local=get_stock_list()
    stock_len=len(codestock_local)
    update_list=[]  #for update is_peach, is_zig, is_quad in database table

    for i in range(0,stock_len):
        #for i in range(0,5):
        #if (True):
        #i = 0
        #if i > 2:
        #    continue

        draw_flag = False
        is_peach = 0
        is_zig = 0
        is_quad = 0
        is_boll_cross = False
        is_macd_cross = False
        is_ema_cross = False
        is_up_days  = 0

        nowcode=codestock_local[i][0]
        nowname=codestock_local[i][1]

        if nowcode[0:1] == '6':
            nowcode_new= 'SH' + nowcode
        else:
            nowcode_new= 'SZ' + nowcode

        if debug:
            print("code:%s, name:%s" % (nowcode, nowname ))


        if 0:
            #skip ST
            #if ('ST' in nowname or '300' in nowcode):
            if ('ST' in nowname or '68' in nowcode):
                if debug:
                    print("skip code: code:%s, name:%s" % (nowcode, nowname ))
                continue
        else:
            pass


        '''
        #database table item list
        record_date | stock_code |  open   |  close  |  high   |   low   | volume |  amount  | p_change 
        | is_zig | is_quad | is_macd_cross | is_ema_cross | is_boll_cross | up_days
        '''
        detail_info = hdata.get_data_from_hdata(stock_code=nowcode_new,
                end_date=nowdate.strftime("%Y-%m-%d"),
                limit=700)
        if debug:
            print(detail_info)
           
        #fix NaN bug
        # if len(detail_info) == 0 or (detail_info is None):
        if len(detail_info) < 6  or (detail_info is None):
            # print('NaN: code:%s, name:%s' % (nowcode, nowname ))
            update_list.append([nowdate.strftime("%Y-%m-%d"), nowcode_new, is_peach, is_zig, is_quad])
            continue
         
        db_max_date = detail_info['record_date'][len(detail_info)-1]
        if debug:
            print('type(db_max_date)=%s' % type(db_max_date))

        #format transfer '2021-01-01' -> '20210101' 
        if time_is_equal(db_max_date.replace('-',''), nowdate.strftime("%Y%m%d")):
            if debug:
                print('date is ok')
            else:
                pass
        else:
            #invalid data, skip this
            print('###error###: nowcode:%s, database max date:%s, nowdate:%s' % \
                    (nowcode, db_max_date, nowdate.strftime("%Y%m%d")))
            
            continue

           
        #funcat call
        T(str(nowdate))
        S(nowcode)
        # print(str(nowdate), nowcode, nowname, O, H, L, C)

        # dif: 12， 与26日的差别
        # dea:dif的9日以移动平均线
        # 计算MACD指标
        dif, dea, macd_hist = talib.MACD(np.array(detail_info['close'], dtype=float),\
                fastperiod=12, slowperiod=26, signalperiod=9)
           
        upperband, middleband, lowerband = talib.BBANDS(np.array(detail_info['close']),\
                timeperiod=20, nbdevdn=2, nbdevup=2)

           
        ##############################################################################
        '''
        桃园三结义的技术要点：

        1 股价要以阳线的形式上冲EXPMA（线 股价一定要站上这条线 光头阳线比带着影线的阳线强 涨停的光头阳线是最好的阳线

        2. MACD出现金叉 MACD一般有4种金叉，0轴之下的金叉 0轴之下的双次金叉 0轴之上的金叉 0轴之上的2次金叉。 一般是来说二次金叉>0轴上的金叉>0轴下的双次金叉>0轴之下的金叉

        3 BOLL突破中轨 一定是在中轨上方 有2中情况 第一种 从下面穿上来 第二种站在上方

        满足这3个条件 我们就叫桃园三结义。 。买入这种形态之后 就一路上涨。

        尤其是通过长期横盘之后的出现桃园三结义。这样横有多长，竖有多高。很容易出现翻翻行情。

        桃园三结义的买点：

        第一介入点条件达成当天尾盘

        第二介入点第二天开盘价如果开盘价过高。盘中低点买。

        第三介入点出现上面的2个买点后，如果错过，等回踩EXPMA）再买，或者加仓。
        '''

         



        today_p = ((C - REF(C, 1))/REF(C, 1)) 
        today_p = round (today_p.value, 4)

        yes_p = ((REF(C, 1) - REF(C, 2))/REF(C, 2)) 
        yes_p = round (yes_p.value, 4)

        if ( False):
            #is_peach
            cond_5 = peach_exist(nowdate, nowcode, 2, detail_info)
            if cond_5 and today_p > 0.01:
                is_peach = 1 
                print("[tao_yuan_san_jie_yi] peach and macd golden cross: code:%s, name:%s" % \
                    (nowcode, nowname ))
        else:

            # C cross EMA12
            cond_1 = C > O and today_p > 0.01 and \
                    ( REF(C, 1) <  REF(EMA(C,12), 1) and C > EMA(C,12))
            if debug:
                print( REF(C, 1) ,  REF(EMA(C,12), 1) ,  C , EMA(C,12))

            #C cross boll-mid
            cond_2 = (O < middleband[-1] and C > middleband[-1])

            #dif > dea
            #cond_3 = dif[-1] >  dea[-1] # macd gold cross

            #dif dea become big
            cond_3 = dif[-1] > dif[-2] or dea[-1] > dea[-2]
            if debug:
                print(dif[-1] , dif[-2] , dea[-1] , dea[-2])

            #C cross ma5 and ma10
            low=min(REF(C,1), O)
            cond_4 = low < MA(C, 5)  and C > MA(C, 5)

            cond_5 = low < MA(C, 10) and C > MA(C, 10)

            #volume not big
            cond_6 = V < (1.2 * REF(V, 1)) and V > (0.8 * REF(V, 1))
            cond_6 = True


            if debug:
                print(cond_1, cond_2, cond_3, cond_4, cond_5 , cond_6)

            if cond_1 and cond_2 and cond_3 and cond_4 and cond_5 and cond_6:
                is_peach = 1 

                print("[tao_yuan_san_jie_yi_adv] peach and macd golden cross: code:%s, name:%s" % \
                    (nowcode, nowname ))
                if debug:
                    print('is_peach %s' % is_peach)

        ################################################################################################

        #is_zig
        #zig condition
        z_df, z_peers, z_d, z_k, z_buy_state=zig(detail_info)

        if debug:
            print('zig info: z_peers=%s' %(z_peers))
            for k in range(0,len(z_peers)):
                print('zig info: z_peers_date=%s' %(z_d[z_peers[k]]))

            print('zig info: z_d=%s' %(z_d))
            print('zig info: z_k=%s' %(z_k))
            print('zig info: z_buy_state=%s' %(z_buy_state))

        z_len = len(z_peers)
        #calculate buy or sell
        if z_len >= 3:  # it should have one valid zig data at least
            delta_day =  z_peers[-1] - z_peers[-2]
            if z_buy_state[-2] is 1:  #valid zig must 1, that means valley
                is_zig = delta_day
            else:
                is_zig = delta_day * (-1)

        if debug:
            print('is_zig=%s' % is_zig)
            

        ###############################################################################################
        #is_quad
        #cross
        MA5=MA(C,5)
        MA10=MA(C,10)
        MA30=MA(C,30)
        MA60=MA(C,60)
        P1=CROSS(MA5,MA30)
        P2=CROSS(MA5,MA60)
        P3=CROSS(MA10,MA30)
        P4=CROSS(MA10,MA60)
        if debug:
            print('P1=%s, P2=%s, P3=%s, P4=%s'% (P1, P2, P3, P4))

        p1_pos, p1_cross =  get_cross_info(P1)
        p2_pos, p2_cross =  get_cross_info(P2)
        p3_pos, p3_cross =  get_cross_info(P3)
        p4_pos, p4_cross =  get_cross_info(P4)

        if debug:
            print('p1_pos:%s, p1_cross:%s' %(p1_pos, p1_cross))
            print('p2_pos:%s, p2_cross:%s' %(p2_pos, p2_cross))
            print('p3_pos:%s, p3_cross:%s' %(p3_pos, p3_cross))
            print('p4_pos:%s, p4_cross:%s' %(p4_pos, p4_cross))

        # P1 P2 P3 P4 all are true during withdays
        if p1_cross and  p2_cross and  p3_cross and  p4_cross :
            if debug:
                print('!!! %s, %s, %s' %(str(nowdate), nowcode, nowname))

            the_min = min(O, C)

            #cond-1
            c_less_ma5 = False
            s_day = min(p1_pos, p2_pos)
            e_day = max(p1_pos, p2_pos)
            if s_day == e_day:
                if REF(the_min, s_day) >= REF(MA5, s_day):
                    c_less_ma5 = True
                    if debug:
                        print("ma5: s_day(%d) is equal e_day(%d)" %( s_day, e_day))
                    else:
                        pass
                else:
                    pass
            else:
                for ps in range(s_day, e_day + 1):
                    if REF(the_min, ps) >= REF(MA5, ps):
                    #if REF(C, ps) >= REF(MA5, ps):
                        c_less_ma5 = True
                        if debug:
                            print('MA5 condition ok')
                        else:
                            pass
                    else:
                        c_less_ma5 = False
                        if debug:
                            print('MA5 condition not ok')
                        else:
                            pass
                        break


            #cond-2
            c_less_ma60 = False
            s_day = min(p3_pos, p4_pos)
            e_day = max(p1_pos, p2_pos)
            if s_day == e_day:
                if REF(the_min, s_day) >= REF(MA60, s_day):
                    c_less_ma60 = True
                    if debug:
                        print("ma60: s_day(%d) is equal e_day(%d)" %( s_day, e_day))
                    else:
                        pass
            else:
                for ps in range(s_day, e_day + 1):
                    if REF(the_min, ps) >= max(REF(MA60, ps), REF(MA30, ps)):  #the min can not be allowed to enter the quadrilateral
                    #if REF(C, ps) >= REF(MA60, ps):
                        c_less_ma60 = True
                        if debug:
                            print('MA60 condition ok')
                        else:
                            pass
                    else:
                        c_less_ma60 = False
                        if debug:
                            print('MA60 condition not ok')
                        else:
                            pass
                        break

            if c_less_ma5 and c_less_ma60:
               print('### %s, %s, %s' %(str(nowdate), nowcode, nowname))
               is_quad = 1

        if debug:
            print('is_quad=%s' % is_quad)


        ###############################################################################################
        
        update_list.append([nowdate.strftime("%Y-%m-%d"), nowcode_new, is_peach, is_zig, is_quad])

        if debug:
            print('final nowdate=%s, code:%s, name:%s,is_peach=%s, is_zig=%s,is_quad=%s'% \
                    (nowdate.strftime("%Y-%m-%d"), nowcode, nowname, is_peach, is_zig, is_quad))
        
        if (is_peach or is_quad) and (is_zig > 0):
            print('final real code:%s, name:%s,is_peach=%s, is_zig=%s,is_quad=%s'% \
                    (nowcode, nowname, is_peach, is_zig, is_quad))
        
        if debug:
            print('#############################################################################')



    if debug:
        print('update_list:%s'% update_list)

    data_column=['record_date', 'stock_code', 'is_peach', 'is_zig', 'is_quad']
    update_df=pd.DataFrame(update_list, columns=data_column)
    if debug:
        print(update_df)
    #hdata.update_allstock_hdatadate(update_df)

    last_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print("start_time: %s, last_time: %s" % (start_time, last_time))

    return update_df

def update_peach_zig_quad(nowdate, df, df1):

    tmp_df = df.sort_values('stock_code', ascending=0)
    tmp_df = tmp_df.reset_index(drop=True)

    tmp_df1 = df1.sort_values('stock_code', ascending=0)
    tmp_df1 = tmp_df1.reset_index(drop=True)

    tmp_df['is_peach'] = tmp_df1['is_peach']
    tmp_df['is_zig']   = tmp_df1['is_zig']
    tmp_df['is_quad']  = tmp_df1['is_quad']

    if debug:
        print(tmp_df)

    #delete first, then insert
    hdata.delete_data_from_hdata(\
            start_date=nowdate.strftime("%Y-%m-%d"), \
            end_date=nowdate.strftime("%Y-%m-%d")\
            )
    hdata.copy_from_stringio(tmp_df)



if __name__ == '__main__':

    cript_name, para1 = check_input_parameter()

    t1 = time.time()

    nowdate=datetime.datetime.now().date()
    nowdate=nowdate-datetime.timedelta(int(para1))
    print("nowdate is %s"%(nowdate.strftime("%Y-%m-%d"))) 

    handle_df = calculate_peach_zig_quad(nowdate)

    nowdate_df = hdata.get_data_from_hdata(\
            start_date=nowdate.strftime("%Y-%m-%d"), \
            end_date=nowdate.strftime("%Y-%m-%d")\
            )
    update_peach_zig_quad(nowdate, nowdate_df, handle_df) 

    t2 = time.time()

    
    print("t2-t1=%s"%(t2-t1)) 
