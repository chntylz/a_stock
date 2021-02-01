#!/usr/bin/env python  
# -*- coding: utf-8 -*-
# 2019-05-24, aaron

import datetime as datetime
import time
import os
import sys
sys.path.append('pwsnow_ball')

import psycopg2 #使用的是PostgreSQL数据库
from Stocks import *
from HData_day import *
from HData_select import *
from pysnow_ball.HData_xq_day import *
import  datetime

import matplotlib
matplotlib.use('Agg')


from zig import *
from test_plot import *
from file_interface import *




# basic
import numpy as np
import pandas as pd


# visual
import matplotlib.pyplot as plt
import mplfinance as mpf
#%matplotlib inline

#time

#talib
import talib


from funcat import *

from Algorithm import *

#delete runtimer warning
import warnings
warnings.simplefilter(action = "ignore", category = RuntimeWarning)

#log
from common import Log
log = Log(__name__).getlog()

#funcat
from funcat import *
from funcat.data.aaron_backend import AaronDataBackend
set_data_backend(AaronDataBackend())

from sys import argv

################################################################
stocks=Stocks("usr","usr")
hdata=HData_xq_day("usr","usr")
sdata=HData_select("usr","usr")

# stocks.db_stocks_create()#如果还没有表则需要创建
#print(stocks.db_stocks_update())#根据todayall的情况更新stocks表

#hdata.db_hdata_date_create()

#print("line number: " + str(sys._getframe().f_lineno) )
#sdata.db_hdata_date_create()
#print("line number: " + str(sys._getframe().f_lineno) )
######################################################################

#debug switch
debug = 0
debug = 0
within_days = 8
clean_flag = True


#return the day(j) and cross_flag(true or false) if P is true during with_days, P is cross(5, 30), etc
def get_cross_info(P):
    
    for j in range(0, within_days):
        cross = REF(P, j)
        if debug:
            print('P%d=%s type(cross)=%s' % (j, cross, type(cross)))
        if cross:
            if debug:
                print('j=%d: condition is OK'% j)
            return j, cross
        else:
            if debug:
                print('j=%d: condition is NG'% j)

    return j, cross


def check_input_parameter():
    # 如果执行的方式错误输出使用方法
    USAGE = '''
    用法错误，正确方式如下：
    python demo.py 1
    '''
    if len(argv) > 2:
        print(USAGE)  # 如果传入的参数不足，输出正确用法
        exit(1) # 异常退出(下面的代码将不会被执行)

    script_name, para1 = argv  # 将传入的参数赋值进行使用
    print("%s, %d"%(script_name, int(para1)))

    return script_name, para1

def quadrilateral_algorythm(codestock_local, nowdate, para1):
    stock_len=len(codestock_local)
    for i in range(0,stock_len):
    #for i in range(0,2):
    #if (True):
        #i = 0
        draw_flag = False
        nowcode=codestock_local[i][0]
        nowname=codestock_local[i][1]

        if debug:
            print("code:%s, name:%s" % (nowcode, nowname ))

        '''
        if '002641' in nowcode:
            pass
        else:
            continue
        '''

        #skip ST
        #if ('ST' in nowname or '300' in nowcode):
        if ('ST' in nowname or '68' in nowcode):
            #log.debug("ST: code:%s, name:%s" % (nowcode, nowname ))
            if debug:
                print("skip code: code:%s, name:%s" % (nowcode, nowname ))
            continue

        if debug:
            print("skip code: code:%s, name:%s" % (nowcode, nowname ))
            #continue


        if nowcode[0:1] == '6':
            stock_code_new= 'SH' + nowcode
        else:
            stock_code_new= 'SZ' + nowcode
                    

        detail_info = hdata.get_data_from_hdata(stock_code=stock_code_new, \
                end_date=nowdate.strftime("%Y-%m-%d"), limit=600)
        if debug:
            print(detail_info)
            #fix NaN bug
            # if len(detail_info) == 0 or (detail_info is None):
            if len(detail_info) < (int(para1) + 60)  or (detail_info is None):
                if debug:
                    print('NaN: code:%s, name:%s' % (nowcode, nowname ))
                continue
            

        db_max_date = detail_info['record_date'][len(detail_info)-1]
        db_max_date = db_max_date.replace('-','')

        if time_is_equal(db_max_date, nowdate.strftime("%Y%m%d")):
            if debug:
                print('date is ok')
        else:
            #invalid data, skip this
            print('###error###: nowcode:%s, database max date:%s, nowdate:%s' % (nowcode, db_max_date, nowdate.strftime("%Y%m%d")))
            continue

        #funcat call
        T(str(nowdate))
        S(nowcode)
        if debug:
            print(str(nowdate), nowcode, nowname, O, H, L, C)


       
        ##############################################################################
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
                for ps in range(s_day, e_day + 1):
                    if REF(the_min, ps) >= REF(MA5, ps):
                    #if REF(C, ps) >= REF(MA5, ps):
                        c_less_ma5 = True
                        if debug:
                           print('MA5 condition ok')
                    else:
                        c_less_ma5 = False
                        if debug:
                            print('MA5 condition not ok')
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
                for ps in range(s_day, e_day + 1):
                    if REF(the_min, ps) >= max(REF(MA60, ps), REF(MA30, ps)):  #the min can not be allowed to enter the quadrilateral
                    #if REF(C, ps) >= REF(MA60, ps):
                        c_less_ma60 = True
                        if debug:
                            print('MA60 condition ok')
                    else:
                        c_less_ma60 = False
                        if debug:
                            print('MA60 condition not ok')
                        break
                
            if c_less_ma5 and c_less_ma60:
                print('### %s, %s, %s' %(str(nowdate), nowcode, nowname))
                draw_flag = True




        '''
        cond_1 = CROSS(MA, MA)
        cond_2 = CROSS(MA(C,13), MA(C, 21))
        cond_3 = C > MA(C, 5)
        cond_4 = V > MA(V, 50)
        cond_5 = ((C - REF(C, 1))/REF(C, 1)) > 0.03
        
        if cond_1 and cond_2 and cond_3 and cond_4 and cond_5:
            draw_flag = True
            print("cross: code:%s, name:%s" % (nowcode, nowname ))
        '''
        ##############################################################################



        

        ################################################################
        #continue
        
        #check need to generate png 
        if draw_flag == False:
            continue
        

        save_dir = 'stock_data'
        sub_name = '-quad'

        #################### delete begin ##################
        global clean_flag
        if clean_flag:
            clean_flag = False
            remove_dir(nowdate, save_dir, sub_name)
        #################### delete end ##################


        plot_picture(nowdate, nowcode, nowname, detail_info, save_dir, fig, sub_name) 
        ################################################################

    shell_cmd='cp -rf stock_data/' + nowdate.strftime("%Y-%m-%d") +'*'  + ' /var/www/html/stock_data' +'/'
    os.system(shell_cmd)
    print('shell_cmd: %s' % shell_cmd)



if __name__ == '__main__':

    script_name, para1 = check_input_parameter()

    nowdate=datetime.datetime.now().date()
    nowdate=nowdate-datetime.timedelta(int(para1))
    print("nowdate is %s"%(nowdate.strftime("%Y-%m-%d"))) 

    codestock_local=stocks.get_codestock_local()
    if debug:
        print(codestock_local)

    sdata.db_connect()#由于每次连接数据库都要耗时0.0几秒，故获取历史数据时统一连接
    sdata.delete_data_of_day_stock(nowdate.strftime("%Y-%m-%d")) #delete first


    start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    #all_info = hdata.my2_get_all_hdata_of_stock()
    end_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print("start_time: %s, end_time: %s" % (start_time, end_time))

    #define canvas out of loop
    plt.style.use('bmh')
    fig = plt.figure(figsize=(24, 30),dpi=80)


    quadrilateral_algorythm(codestock_local, nowdate, para1)

    plt.close('all')

    sdata.db_disconnect()
    last_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print("start_time: %s, last_time: %s" % (start_time, last_time))
