#!/#!/usr/bin/env python  
# -*- coding: utf-8 -*-


import pandas as pd
import json
import requests
import re

import time
import datetime


from HData_eastmoney_fund import *
from HData_eastmoney_fund_3 import *
from HData_eastmoney_fund_5 import *
from HData_eastmoney_fund_10 import *

debug = 0
#debug = 1


hdata_fund = HData_eastmoney_fund('usr', 'usr')
hdata_fund_3 = HData_eastmoney_fund_3('usr', 'usr')
hdata_fund_5 = HData_eastmoney_fund_5('usr', 'usr')
hdata_fund_10 = HData_eastmoney_fund_10('usr', 'usr')

url_1 = 'http://push2.eastmoney.com/api/qt/clist/get?cb=jQuery112308511204703039876_1610688047489&fid=f62 &po=1&pz=10000&pn=1&np=1&fltt=2&invt=2&ut=b2884a393a59ad64002292a3e90d46a5&fs=m%3A0%2Bt%3A6%2Bf%3A!2%2Cm%3A0%2Bt%3A13%2Bf%3A!2%2Cm%3A0%2Bt%3A80%2Bf%3A!2%2Cm%3A1%2Bt%3A2%2Bf%3A!2%2Cm%3A1%2Bt%3A23%2Bf%3A!2%2Cm%3A0%2Bt%3A7%2Bf%3A!2%2Cm%3A1%2Bt%3A3%2Bf%3A!2&fields=f12%2Cf14%2Cf2%2Cf3%2Cf62%2Cf184%2Cf66%2Cf69%2Cf72%2Cf75%2Cf78%2Cf81%2Cf84%2Cf87%2Cf204%2Cf205%2Cf124'
url_3 = 'http://push2.eastmoney.com/api/qt/clist/get?cb=jQuery112309724568186220448_1610691602607&fid=f267&po=1&pz=10000&pn=1&np=1&fltt=2&invt=2&ut=b2884a393a59ad64002292a3e90d46a5&fs=m%3A0%2Bt%3A6%2Bf%3A!2%2Cm%3A0%2Bt%3A13%2Bf%3A!2%2Cm%3A0%2Bt%3A80%2Bf%3A!2%2Cm%3A1%2Bt%3A2%2Bf%3A!2%2Cm%3A1%2Bt%3A23%2Bf%3A!2%2Cm%3A0%2Bt%3A7%2Bf%3A!2%2Cm%3A1%2Bt%3A3%2Bf%3A!2&fields=f12%2Cf14%2Cf2%2Cf127%2Cf267%2Cf268%2Cf269%2Cf270%2Cf271%2Cf272%2Cf273%2Cf274%2Cf275%2Cf276%2Cf257%2Cf258%2Cf124'
url_5 = 'http://push2.eastmoney.com/api/qt/clist/get?cb=jQuery112309724568186220448_1610691602607&fid=f164&po=1&pz=10000&pn=1&np=1&fltt=2&invt=2&ut=b2884a393a59ad64002292a3e90d46a5&fs=m%3A0%2Bt%3A6%2Bf%3A!2%2Cm%3A0%2Bt%3A13%2Bf%3A!2%2Cm%3A0%2Bt%3A80%2Bf%3A!2%2Cm%3A1%2Bt%3A2%2Bf%3A!2%2Cm%3A1%2Bt%3A23%2Bf%3A!2%2Cm%3A0%2Bt%3A7%2Bf%3A!2%2Cm%3A1%2Bt%3A3%2Bf%3A!2&fields=f12%2Cf14%2Cf2%2Cf109%2Cf164%2Cf165%2Cf166%2Cf167%2Cf168%2Cf169%2Cf170%2Cf171%2Cf172%2Cf173%2Cf257%2Cf258%2Cf124'
url_10= 'http://push2.eastmoney.com/api/qt/clist/get?cb=jQuery112309724568186220448_1610691602607&fid=f174&po=1&pz=10000&pn=1&np=1&fltt=2&invt=2&ut=b2884a393a59ad64002292a3e90d46a5&fs=m%3A0%2Bt%3A6%2Bf%3A!2%2Cm%3A0%2Bt%3A13%2Bf%3A!2%2Cm%3A0%2Bt%3A80%2Bf%3A!2%2Cm%3A1%2Bt%3A2%2Bf%3A!2%2Cm%3A1%2Bt%3A23%2Bf%3A!2%2Cm%3A0%2Bt%3A7%2Bf%3A!2%2Cm%3A1%2Bt%3A3%2Bf%3A!2&fields=f12%2Cf14%2Cf2%2Cf160%2Cf174%2Cf175%2Cf176%2Cf177%2Cf178%2Cf179%2Cf180%2Cf181%2Cf182%2Cf183%2Cf260%2Cf261%2Cf124'

mapping = {
    "f2": "zxj",
    "f3": "zdf",
    "f127": "zdf",
    "f109": "zdf",
    "f160": "zdf",
    "f12": "code",
    "f14": "name",
    "f62": "zlje",
    "f184": "zljzb",
    "f66": "cddje",
    "f69": "cddjzb",
    "f72": "ddje",
    "f75": "ddjzb",
    "f78": "zdje",
    "f81": "zdjzb",
    "f84": "xdje",
    "f87": "xdjzb",
    "f267": "zlje",
    "f268": "zljzb",
    "f269": "cddje",
    "f270": "cddjzb",
    "f271": "ddje",
    "f272": "ddjzb",
    "f273": "zdje",
    "f274": "zdjzb",
    "f275": "xdje",
    "f276": "xdjzb",
    "f164": "zlje",
    "f165": "zljzb",
    "f166": "cddje",
    "f167": "cddjzb",
    "f168": "ddje",
    "f169": "ddjzb",
    "f170": "zdje",
    "f171": "zdjzb",
    "f172": "xdje",
    "f173": "xdjzb",
    "f174": "zlje",
    "f175": "zljzb",
    "f176": "cddje",
    "f177": "cddjzb",
    "f178": "ddje",
    "f179": "ddjzb",
    "f180": "zdje",
    "f181": "zdjzb",
    "f182": "xdje",
    "f183": "xdjzb",
    "f205": "zdcode",
    "f204": "zdname",
    "f258": "zdcode",
    "f257": "zdname",
    "f261": "zdcode",
    "f260": "zdname",
    "f225": "zlpm1",
    "f263": "zlpm5",
    "f264": "zlpm10",
    "f124": "record_date"
};

def get_date_from_timestamp(timestamp):
    d = datetime.datetime.fromtimestamp(timestamp / 1000, None)  
    my_date = d.strftime("%Y-%m-%d %H:%M:%S.%f")
    my_date = my_date[:10]
    return my_date


def get_daily_fund(url=None):
    timestamp=str(round(time.time() * 1000))
    #url = 'http://push2.eastmoney.com/api/qt/clist/get?cb=jQuery112309724568186220448_1610691602607&fid=f62&po=1&pz=10000&pn=1&np=1&fltt=2&invt=2&ut=b2884a393a59ad64002292a3e90d46a5&fs=m%3A0%2Bt%3A6%2Bf%3A!2%2Cm%3A0%2Bt%3A13%2Bf%3A!2%2Cm%3A0%2Bt%3A80%2Bf%3A!2%2Cm%3A1%2Bt%3A2%2Bf%3A!2%2Cm%3A1%2Bt%3A23%2Bf%3A!2%2Cm%3A0%2Bt%3A7%2Bf%3A!2%2Cm%3A1%2Bt%3A3%2Bf%3A!2&fields=f12%2Cf14%2Cf2%2Cf3%2Cf62%2Cf184%2Cf66%2Cf69%2Cf72%2Cf75%2Cf78%2Cf81%2Cf84%2Cf87%2Cf204%2Cf205%2Cf124'
    if url == 'url_3':
        url = 'http://push2.eastmoney.com/api/qt/clist/get?cb=jQuery112309724568186220448_'\
                + timestamp + '&fid=f267&po=1&pz=10000&pn=1&np=1&fltt=2&invt=2&ut=b2884a393a59ad64002292a3e90d46a5&fs=m%3A0%2Bt%3A6%2Bf%3A!2%2Cm%3A0%2Bt%3A13%2Bf%3A!2%2Cm%3A0%2Bt%3A80%2Bf%3A!2%2Cm%3A1%2Bt%3A2%2Bf%3A!2%2Cm%3A1%2Bt%3A23%2Bf%3A!2%2Cm%3A0%2Bt%3A7%2Bf%3A!2%2Cm%3A1%2Bt%3A3%2Bf%3A!2&fields=f12%2Cf14%2Cf2%2Cf127%2Cf267%2Cf268%2Cf269%2Cf270%2Cf271%2Cf272%2Cf273%2Cf274%2Cf275%2Cf276%2Cf257%2Cf258%2Cf124'
    elif url == 'url_5':
        url = 'http://push2.eastmoney.com/api/qt/clist/get?cb=jQuery112309724568186220448_'\
                + timestamp + '&fid=f164&po=1&pz=10000&pn=1&np=1&fltt=2&invt=2&ut=b2884a393a59ad64002292a3e90d46a5&fs=m%3A0%2Bt%3A6%2Bf%3A!2%2Cm%3A0%2Bt%3A13%2Bf%3A!2%2Cm%3A0%2Bt%3A80%2Bf%3A!2%2Cm%3A1%2Bt%3A2%2Bf%3A!2%2Cm%3A1%2Bt%3A23%2Bf%3A!2%2Cm%3A0%2Bt%3A7%2Bf%3A!2%2Cm%3A1%2Bt%3A3%2Bf%3A!2&fields=f12%2Cf14%2Cf2%2Cf109%2Cf164%2Cf165%2Cf166%2Cf167%2Cf168%2Cf169%2Cf170%2Cf171%2Cf172%2Cf173%2Cf257%2Cf258%2Cf124'
    elif url == 'url_10':
        url = 'http://push2.eastmoney.com/api/qt/clist/get?cb=jQuery112309724568186220448_'\
                + timestamp + '&fid=f174&po=1&pz=10000&pn=1&np=1&fltt=2&invt=2&ut=b2884a393a59ad64002292a3e90d46a5&fs=m%3A0%2Bt%3A6%2Bf%3A!2%2Cm%3A0%2Bt%3A13%2Bf%3A!2%2Cm%3A0%2Bt%3A80%2Bf%3A!2%2Cm%3A1%2Bt%3A2%2Bf%3A!2%2Cm%3A1%2Bt%3A23%2Bf%3A!2%2Cm%3A0%2Bt%3A7%2Bf%3A!2%2Cm%3A1%2Bt%3A3%2Bf%3A!2&fields=f12%2Cf14%2Cf2%2Cf160%2Cf174%2Cf175%2Cf176%2Cf177%2Cf178%2Cf179%2Cf180%2Cf181%2Cf182%2Cf183%2Cf260%2Cf261%2Cf124'
    else:
        url = 'http://push2.eastmoney.com/api/qt/clist/get?cb=jQuery112309724568186220448_'\
                + timestamp +'&fid=f62&po=1&pz=10000&pn=1&np=1&fltt=2&invt=2&ut=b2884a393a59ad64002292a3e90d46a5&fs=m%3A0%2Bt%3A6%2Bf%3A!2%2Cm%3A0%2Bt%3A13%2Bf%3A!2%2Cm%3A0%2Bt%3A80%2Bf%3A!2%2Cm%3A1%2Bt%3A2%2Bf%3A!2%2Cm%3A1%2Bt%3A23%2Bf%3A!2%2Cm%3A0%2Bt%3A7%2Bf%3A!2%2Cm%3A1%2Bt%3A3%2Bf%3A!2&fields=f12%2Cf14%2Cf2%2Cf3%2Cf62%2Cf184%2Cf66%2Cf69%2Cf72%2Cf75%2Cf78%2Cf81%2Cf84%2Cf87%2Cf204%2Cf205%2Cf124'

    if debug:
        print(url)

   
    response = requests.get(url)
    p1 = re.compile(r'[(](.*?)[)]', re.S)
    response_array = re.findall(p1, response.text)
    api_param = json.loads(response_array[0])
    rawdata = api_param['data']['diff']
    data_df = pd.DataFrame(rawdata)
    


    return data_df

def del_column(df, name=None):
    if name in df.columns:
        del df[name]

def handle_raw_data(df):
    nowdate=datetime.datetime.now().date()

   
    del_column(df,name='f204')
    del_column(df,name='f205')
    del_column(df,name='f206')
    del_column(df,name='f257')
    del_column(df,name='f258')
    del_column(df,name='f259')
    del_column(df,name='f260')
    del_column(df,name='f261')
    del_column(df,name='f262')


    if debug:
        print(df)

    df_list = list(df)
    len_df = len(df_list)
    for i in range(0, len_df):
        if debug:
            print(i, len_df,df_list[i])
        new_col_name = mapping[df_list[i]]
        df = df.rename(columns={df_list[i]:new_col_name})

    
    #timestamp -> date
    df['record_date'] = nowdate.strftime("%Y-%m-%d")
    #df['record_date'] = '2021-01-22'

    #delete suspend code
    df = df[df['zxj'] != '-']
    df = df.reset_index(drop=True)


    df['record_date'] = nowdate.strftime("%Y-%m-%d")
    #unit: Y
    df['zlje']  = df['zlje'].apply(lambda x: x/1000/1000/100)
    df['cddje'] = df['cddje'].apply(lambda x: x/1000/1000/100)
    df['ddje']  = df['ddje'].apply(lambda x: x/1000/1000/100)
    df['zdje']  = df['zdje'].apply(lambda x: x/1000/1000/100)
    df['xdje']  = df['xdje'].apply(lambda x: x/1000/1000/100)

    df = round(df, 2)

    new_col = ['zxj', 'zdf', 'code', 'name', 'record_date', 'zlje', 'zljzb', 'cddje', 'cddjzb', 'ddje', 'ddjzb', 'zdje', 'zdjzb', 'xdje', 'xdjzb']
    df = df[new_col]
    return df

def check_table():
    table_exist = hdata_fund.table_is_exist()
    print('table_exist=%d' % table_exist)
    if table_exist:
        #hdata_fund.db_hdata_xq_create()
        print('table already exist')
    else:
        hdata_fund.db_hdata_xq_create()
        print('table not exist, create')

    table_3_exist = hdata_fund_3.table_is_exist()
    print('table_3_exist=%d' % table_3_exist)
    if table_3_exist:
        #hdata_fund_3.db_hdata_xq_create()
        print('table_3 already exist')
    else:
        hdata_fund_3.db_hdata_xq_create()
        print('table_3 not exist, create')

    table_5_exist = hdata_fund_5.table_is_exist()
    print('table_5_exist=%d' % table_5_exist)
    if table_5_exist:
        #hdata_fund_5.db_hdata_xq_create()
        print('table_5 already exist')
    else:
        hdata_fund_5.db_hdata_xq_create()
        print('table_5 not exist, create')

    table_10_exist = hdata_fund_10.table_is_exist()
    print('table_10_exist=%d' % table_10_exist)
    if table_10_exist:
        #hdata_fund_10.db_hdata_xq_create()
        print('table_10 already exist')
    else:
        hdata_fund_10.db_hdata_xq_create()
        print('table_10 not exist, create')



if __name__ == '__main__':
    
    nowdate=datetime.datetime.now().date()

    check_table()

    df = get_daily_fund()
    df = handle_raw_data(df)
    print(list(df))
    hdata_fund.copy_from_stringio(df)

    df_3 = get_daily_fund(url='url_3')
    df_3 = handle_raw_data(df_3)
    hdata_fund_3.copy_from_stringio(df_3)
    print(list(df_3))

    df_5 = get_daily_fund(url='url_5')
    df_5 = handle_raw_data(df_5)
    hdata_fund_5.copy_from_stringio(df_5)
    print(list(df_5))

    df_10 = get_daily_fund(url='url_10')
    df_10 = handle_raw_data(df_10)
    hdata_fund_10.copy_from_stringio(df_10)
    print(list(df_10))

    print(list(df))
    print(list(df_3))
    print(list(df_5))
    print(list(df_10))



