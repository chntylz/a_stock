#!/#!/usr/bin/env python  
# -*- coding: utf-8 -*-


import pandas as pd
import json
import requests
import re

import time
import datetime


from HData_eastmoney_realfund import *

debug = 0
debug = 1


hdata_realfund = HData_eastmoney_realfund('usr', 'usr')

date_list = ['03-31', '06-30', '09-30', '12-31']


def get_real_fund(date=None):

    data_df = pd.DataFrame()
    nowdate=datetime.datetime.now().date()

    if date is None:
        date = nowdate.strftime('%Y-%m-%d')

    if date[5:] in date_list:
        url ='https://data.eastmoney.com/dataapi/zlsj/list?tkn=eastmoney&ReportDate='
        url += date
        url += '&code=&type=1&zjc=0&sortField=Count&sortDirec=1&pageNum=1&pageSize=5000&cfg=jjsjtj'

        if debug:
            print(url)

        response = requests.get(url)
        api_param = json.loads(response.text)
        rawdata = api_param['data']
        data_df = pd.DataFrame(rawdata)
        #replace '' with nan
        data_df.replace(to_replace=r'^\s*$',value=np.nan,regex=True,inplace=True)
        #replace nan with 0
        data_df = data_df.fillna(0)


    return data_df

def handle_raw_data(df):

    if len(df) < 0:
        return

    '''
    #unit: Y
    df['f7']  = df['f7'].apply(lambda x: float(x)/1000/1000/100)
    df['f8']  = df['f8'].apply(lambda x: float(x)/1000/1000/100)
    df['f11']  = df['f11'].apply(lambda x: float(x)/1000/1000/100)

    df['f9']  = df['f9'].apply(lambda x: round(float(x),2))
    df['f10']  = df['f10'].apply(lambda x: round(float(x),2))
    df['f12']  = df['f12'].apply(lambda x: round(float(x),2))
    '''

    df = round(df, 2)

    return df

def check_table():
    table_exist = hdata_realfund.table_is_exist()
    print('table_exist=%d' % table_exist)
    if table_exist:
        #hdata_realfund.db_hdata_xq_create()
        print('table already exist')
    else:
        hdata_realfund.db_hdata_xq_create()
        print('table not exist, create')
    pass

if __name__ == '__main__':
    
    nowdate=datetime.datetime.now().date()
    date_string = nowdate.strftime('%Y-%m-%d')

    check_table()
    
    df = get_real_fund('2019-12-31')
    df.to_csv('test_realfund.csv', encoding='gbk')
    if len(df):
        df = handle_raw_data(df)
        hdata_realfund.copy_from_stringio(df)


