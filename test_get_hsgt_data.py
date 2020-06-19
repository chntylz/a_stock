#!/usr/bin/env python
#coding:utf-8
import os,sys,gzip
import json


from file_interface import *


import psycopg2 #使用的是PostgreSQL数据库
import tushare as ts

from HData_hsgt import *
from HData_day import *

import  datetime


#funcat
from funcat import *
from funcat.data.aaron_backend import AaronDataBackend
set_data_backend(AaronDataBackend())


debug=0
#debug=1

#file_path='/home/ubuntu/tmp/a_stock/hkexnews_scrapy/hkexnews_scrapy/json/20190823.json.gz'
hdata_day=HData_day("usr","usr")
hdata_day.db_connect()

hdata_hsgt=HData_hsgt("usr","usr")
#hdata_hsgt.db_hdata_date_create()
hdata_hsgt.db_connect()




def hsgt_get_day_item_from_json(file_path):
    line_num=0
    list_tmp=[]
    line_count=len(gzip.open(file_path).readlines())

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

        #get open, close, high, low, and volume
        day_df=hdata_day.get_data_accord_code_and_date(shgt_code, shgt_date)
        if debug:
            print("line_num:%d, shgt_date:%s, shgt_code:%s, shgt_holding:%s, shgt_percent:%s,shgt_ename:%s, shgt_cname:%s"% \
                 (line_num, shgt_date, shgt_code, shgt_holding, shgt_percent, shgt_ename, shgt_cname))
            print('print day_df:')
            print(day_df)
        if len(day_df) > 0:
            day_dict=day_df.to_dict()

            shgt_open=day_dict['open'][0]
            shgt_close=day_dict['close'][0]
            shgt_high=day_dict['high'][0]
            shgt_low=day_dict['low'][0]
            shgt_volume=day_dict['volume'][0]


            list_tmp.append([shgt_date, shgt_code, shgt_cname, shgt_holding, shgt_percent, shgt_open, shgt_close, shgt_high, shgt_low, shgt_volume])
        else:
            print('############## code:%s, name=%s, daily data is null!!! ##############' % (shgt_code, shgt_cname))

    if debug:
        print(list_tmp)

    dataframe_cols = ['record_date', 'stock_code','shgt_cname', 'share_holding', 'hk_pct', 'open', 'close', 'high', 'low', 'volume']

    df = pd.DataFrame(list_tmp, columns=dataframe_cols)
    index =  df["record_date"]
    df = pd.DataFrame(list_tmp, index=index, columns=dataframe_cols)
    del df["record_date"]
    
    if debug:
        print(df)

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
        if debug:
            print("curr_date %s < latest_date:%s"%(curr_date, latest_date))
        result=compare_time(curr_date, latest_date)

        if result is False: 
            #insert data into hdata_hsgt_table
            hsgt_get_day_item_from_json(tmp_file)

    return



hsgt_get_all_data()

hdata_hsgt.db_disconnect()
hdata_day.db_disconnect()
