#!/usr/bin/env python
#coding:utf-8
import os,sys,gzip
import json

import sys
sys.path.append("pysnow_ball")
sys.path.append("eastmoney")

from file_interface import *


import psycopg2 #使用的是PostgreSQL数据库
import tushare as ts

from HData_hsgt import *
from HData_xq_day import *

from HData_xq_fina import *
from HData_xq_holder import *

from get_daily_fund import *

import  datetime


#funcat
from funcat import *
from funcat.data.aaron_backend import AaronDataBackend
set_data_backend(AaronDataBackend())


debug=0
#debug=1

#file_path='/home/ubuntu/tmp/a_stock/hkexnews_scrapy/hkexnews_scrapy/json/20190823.json.gz'
hdata_day=HData_xq_day("usr","usr")

hdata_hsgt=HData_hsgt("usr","usr")
hdata_fina=HData_xq_fina("usr","usr")
hdata_holder=HData_xq_holder("usr","usr")
#hdata_hsgt.db_hdata_date_create()
hdata_hsgt.db_connect()




def hsgt_get_day_item_from_json(file_path):
    line_num=0
    list_tmp=[]
    line_count=len(gzip.open(file_path).readlines())

    #get fund from eastmoney database
    real_fund_df = get_daily_fund()
    real_fund_df = handle_raw_data(real_fund_df)
    
    ####get zlje start####
    fund_df   = get_zlje_data_from_db(url='url'     )
    fund_3_df = get_zlje_data_from_db(url='url_3'   )
    fund_5_df = get_zlje_data_from_db(url='url_5'   )
    fund_10_df = get_zlje_data_from_db(url='url_10' )
    ####get zlje end####

    for line in gzip.open(file_path):
        line_num = line_num + 1   
        if line_num == 1 or line_num == line_count:
            continue
        if debug:
            print("line_num:%d, %s"%(line_num, line))
        
        #{"date": "2019-08-23", "stock_ename": "SHENZHEN MINDRAY BIO-MEDICAL ELECTRONICS CO., LTD. (A #300760)", "code": "77760", "share_holding": "19601172", "percent": "1.61%"}
        line=str(line)
        begin=line.rfind('{')
        end=line.rfind('}')
        s=line[begin:end+1]
        s=s.replace('\'', '-')
        s=s.replace('\\', '')
        s=s.replace('XI"AN', 'XI-AN')

        if debug:
            print('s---->%s'%(s))
        d=json.loads(s, strict=False)
        line=d

        #get date
        shgt_date=line['date']

        #get stock_ename
        shgt_ename=line['stock_ename']
        position=shgt_ename.rfind('#')
        shgt_code=shgt_ename[position+1: -1]
        if len(shgt_code) < 6:
            print('error data! shgt_code:%s'%(shgt_code))
            shgt_code='0'.join(shgt_code)

        #### zlje start ####
        zlje = get_zlje(fund_df,     shgt_code, curr_date=shgt_date)
        if zlje:
            zlje = float(zlje[: zlje.find('<br>')])

        zlje_3 = get_zlje(fund_3_df,   shgt_code, curr_date=shgt_date)
        if zlje_3:
            zlje_3 = float(zlje_3[: zlje_3.find('<br>')])

        zlje_5 = get_zlje(fund_5_df,   shgt_code, curr_date=shgt_date)
        if zlje_5:
            zlje_5 = float(zlje_5[: zlje_5.find('<br>')])

        zlje_10 = get_zlje(fund_10_df,  shgt_code, curr_date=shgt_date)
        if zlje_10:
            zlje_10 = float(zlje_10[: zlje_10.find('<br>')])
        #### zlje end ####


        #### fina start ####
        if shgt_code[0:1] == '6':
            shgt_code_new= 'SH' + shgt_code 
        else:
            shgt_code_new= 'SZ' + shgt_code 


        
        #### fina start ####
        fina_df = hdata_fina.get_data_from_hdata(stock_code = shgt_code_new)
        
        fina_df = fina_df.sort_values('record_date', ascending=0)
        fina_df = fina_df.reset_index(drop=True)
        
        op_yoy = net_yoy = 0
        if len(fina_df):
            op_yoy = fina_df['operating_income_yoy'][0]
            net_yoy = fina_df['net_profit_atsopc_yoy'][0]
            
            if debug:
                print(fina_df)
        #### fina end ####
 
 
        #### holder start ####
        holder_df = hdata_holder.get_data_from_hdata(stock_code = shgt_code_new)
        holder_df = holder_df.sort_values('record_date', ascending=0)
        holder_df = holder_df.reset_index(drop=True)
        h0 = h1 = h2 = 0
        if len(holder_df) > 0:
            h0 = holder_df['chg'][0]
        if len(holder_df) > 1:
            h1 = holder_df['chg'][1]
        if len(holder_df) > 2:
            h2 = holder_df['chg'][2]
        #### holder start ####


        
        #get stock_cname
        shgt_cname=symbol(shgt_code)
        pos_s=shgt_cname.rfind('[')
        pos_e=shgt_cname.rfind(']')
        shgt_cname=shgt_cname[pos_s+1: pos_e]
        #print(shgt_cname)

        #get share_holding
        shgt_holding=float(line['share_holding'])
        
        #get percent
        shgt_percent=line['percent']
        position=shgt_ename.rfind('%')
        shgt_percent=float(shgt_percent[:position])

        if shgt_code[0:1] == '6':
            stock_code_new= 'SH' + shgt_code
        else:
            stock_code_new= 'SZ' + shgt_code

        #get open, close, high, low, and volume
        day_df=hdata_day.get_data_from_hdata(stock_code=stock_code_new, 
                start_date=shgt_date, end_date=shgt_date)
        if debug:
            print("line_num:%d, shgt_date:%s, shgt_code:%s, shgt_holding:%s, shgt_percent:%s,\
                    shgt_ename:%s, shgt_cname:%s"% \
                 (line_num, shgt_date, shgt_code, shgt_holding, shgt_percent,\
                     shgt_ename, shgt_cname))
            print('print day_df:')
            print(day_df)
        if len(day_df) > 0:
            day_dict=day_df.to_dict()

            shgt_open=day_dict['open'][0]
            shgt_close=day_dict['close'][0]
            shgt_high=day_dict['high'][0]
            shgt_low=day_dict['low'][0]
            shgt_volume=day_dict['volume'][0]
            shgt_is_zig=int(day_dict['is_zig'][0])
            shgt_is_quad=int(day_dict['is_quad'][0])
            shgt_is_peach=int(day_dict['is_peach'][0])



            list_tmp.append([shgt_date, shgt_code, shgt_cname, shgt_holding, shgt_percent, \
                    shgt_open, shgt_close, shgt_high, shgt_low, shgt_volume, \
                    shgt_is_zig, shgt_is_quad, shgt_is_peach, \
                    op_yoy, net_yoy,\
                    zlje, zlje_3, zlje_5, zlje_10,\
                    h0, h1, h2 ])
        else:
            print('############## code:%s, name=%s, daily data is null!!! ##############' % (shgt_code, shgt_cname))

    if debug:
        print(list_tmp)

    dataframe_cols = ['record_date', 'stock_code','shgt_cname', 'share_holding', 'hk_pct', \
            'open', 'close', 'high', 'low', 'volume', \
            'is_zig', 'is_quad', 'is_peach',\
            'op_yoy', 'net_yoy',\
            'zlje', 'zlje_3', 'zlje_5', 'zlje_10', \
            'holder_0', 'holder_1', 'holder_2' ]

    df = pd.DataFrame(list_tmp, columns=dataframe_cols)
    
    index =  df["record_date"]

    df = pd.DataFrame(list_tmp, index=index, columns=dataframe_cols)
    del df["record_date"]
    
    if debug:
        print(df)

    df.to_csv('./hsgt_debug.csv', encoding='utf-8')

    hdata_hsgt.insert_optimize_stock_hdatadate(df)


    return


def hsgt_get_all_data():

    latest_date = hdata_hsgt.db_get_latest_date_of_stock()
    if debug:
        print(type(latest_date))
    latest_date = str(latest_date)
    latest_date = latest_date.replace('-', '')

    print('latest_date:%s'%(latest_date))

    if latest_date is None :
        latest_date='20180101'

    #latest_date='20180101'

    curr_dir=cur_file_dir()#获取当前.py脚本文件的文件路径
    json_dir=curr_dir+'/hkexnews_scrapy/hkexnews_scrapy/json'
    all_files=getAllFiles(json_dir)
    if debug:
        print(all_files)

    for tmp_file in all_files:
        file_size=get_FileSize(tmp_file)
        if debug:
            print("%s size is:%f"%(tmp_file, file_size))
       
        if file_size < 1 :
            if debug:
                print(tmp_file)
            continue

        position=tmp_file.rfind('.json.gz')
        curr_date=tmp_file[position-8:position]
        result=compare_time(curr_date, latest_date)
        if debug:
            print("result=%s, curr_date %s < latest_date:%s"%(result, curr_date, latest_date))

        if result is False: 
            #insert data into hdata_hsgt_table
            hsgt_get_day_item_from_json(tmp_file)

    return



hsgt_get_all_data()

hdata_hsgt.db_disconnect()
