#!/usr/bin/env python
#coding:utf-8
import os,sys,gzip
import json


from file_interface import *


import psycopg2 #使用的是PostgreSQL数据库
import tushare as ts
import numpy as np

from HData_hsgt import *

import  datetime

#keep 0.01 accrucy
pd.set_option('display.float_format',lambda x : '%.2f' % x)

###################################################################################


debug=0
#debug=1


hdata_hsgt=HData_hsgt("usr","usr")
hdata_hsgt.db_connect()



###################################################################################

def hsgt_get_stock_list():
    df=hdata_hsgt.get_all_list_of_stock()
    if debug:
        print("df size is %d"% (len(df)))
    
    return df


def hsgt_get_all_data():
    df=hdata_hsgt.get_all_hdata_of_stock()
    if debug:
        print("df size is %d"% (len(df)))
    
    return df

def hsgt_analysis_data():
    all_df=hsgt_get_all_data()
    all_df["record_date"]=all_df["record_date"].apply(lambda x: str(x))
    latest_date=all_df.loc[0,'record_date']
    print(latest_date)

    all_df['delta1']  =all_df.groupby('stock_code')['percent'].apply(lambda i:i.diff(-1))
    all_df['delta2']  =all_df.groupby('stock_code')['percent'].apply(lambda i:i.diff(-2))
    all_df['delta3']  =all_df.groupby('stock_code')['percent'].apply(lambda i:i.diff(-3))
    all_df['delta5']  =all_df.groupby('stock_code')['percent'].apply(lambda i:i.diff(-5))
    all_df['delta10'] =all_df.groupby('stock_code')['percent'].apply(lambda i:i.diff(-10))
    all_df['delta21'] =all_df.groupby('stock_code')['percent'].apply(lambda i:i.diff(-21))
    all_df['delta120']=all_df.groupby('stock_code')['percent'].apply(lambda i:i.diff(-120))
    all_df=all_df.round(2)
    return all_df

    pass
            
def hsgt_get_daily_data(all_df):
    latest_date=all_df.loc[0,'record_date']
    daily_df=all_df[all_df['record_date'] == latest_date]
    return daily_df


def hsgt_daily_sort(daily_df, orderby='delta1'):
    sort_df=daily_df.sort_values(orderby, ascending=0)
    return sort_df;

def hsgt_write_to_file(f, df):
        f.write('<table class="gridtable">\n')


        f.write('    <tr>\n')

        a_len=len(list(df))
        for j in range(0, a_len): 
            f.write('        <td>\n')
            f.write('           <a> %s</a>\n'%(list(df)[j]))
            f.write('        </td>\n')
        f.write('    </tr>\n')

       #f.write('%s\n'%(list(df)))
        for i in range(0, 10):
            f.write('    <tr>\n')
            a_array=df[i:i+1].values
            a_len=len(a_array[0])
            for j in range(0, a_len): 
                f.write('        <td>\n')
                f.write('           <a> %s</a>\n'%(a_array[0][j]))
                f.write('        </td>\n')

            f.write('    </tr>\n')

        f.write('</table>\n')

###################################################################################
if __name__ == '__main__':
    all_df =  hsgt_analysis_data()
    daily_df = hsgt_get_daily_data(all_df)
    delta1_df = hsgt_daily_sort(daily_df, 'delta1')
    delta2_df = hsgt_daily_sort(daily_df, 'delta2')

    print('day1')
    print("%s"%(delta1_df.head(10)))
    print('day2')
    print("%s"%(delta2_df.head(10)))

    #df = pd.DataFrame(np.random.randint(0,100,size=[10,10]))
    newfile='hsgt.html'
    with open(newfile,'w') as f:

        f.write('<!DOCTYPE html>\n')
        f.write('<html>\n')
        f.write('<head>\n')
        f.write('<meta http-equiv="Content-Type" content="text/html; charset=utf-8" />\n')
        f.write('<title> index </title>\n')
        f.write('\n')
        f.write('\n')
        f.write('<style type="text/css">a {text-decoration: none}\n')
        f.write('\n')
        f.write('\n')

        f.write('/* gridtable */\n')
        f.write('table.gridtable {\n')
        f.write('    font-size:15px;\n')
        f.write('    color:#000;\n')
        f.write('    border-width: 1px;\n')
        f.write('    border-color: #333333;\n')
        f.write('    border-collapse: collapse;\n')
        f.write('}\n')
        f.write('table.gridtable th {\n')
        f.write('    border-width: 1px;\n')
        f.write('    padding: 8px;\n')
        f.write('    border-style: solid;\n')
        f.write('    border-color: #333333;\n')
        f.write('    background-color: #dedede;\n')
        f.write('}\n')
        f.write('table.gridtable td {\n')
        f.write('    border-width: 1px;\n')
        f.write('    padding: 8px;\n')
        f.write('    border-style: solid;\n')
        f.write('    border-color: #333333;\n')
        f.write('    background-color: #ffffff;\n')
        f.write('}\n')
        f.write('/* /gridtable */\n')

        f.write('\n')
        f.write('\n')
        f.write('</style>\n')

        f.write('</head>\n')
        f.write('\n')
        f.write('\n')
 
        f.write('<body>\n')
###################################################################################
        #hsgt_write_to_file(f, df)
        hsgt_write_to_file(f, delta1_df.head(10))
        f.write('        <td>\n')
        f.write('           <a>################################################################################### </a>\n')
        f.write('        </td>\n')
        hsgt_write_to_file(f, delta2_df.head(10))
###################################################################################

        f.write('</body>\n')
        f.write('\n')
        f.write('\n')
        f.write('</html>\n')
        f.write('\n')


hdata_hsgt.db_disconnect()
