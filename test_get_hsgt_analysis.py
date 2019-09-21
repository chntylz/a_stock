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

def hsgt_get_delta_m_of_day(df, days):
    delta_dict={2:'delta2_m',  3:'delta3_m', 4:'delta4_m', 5:'delta5_m', 10:'delta10_m', 21:'delta21_m', 120:'delta120_m'}
    target_column=delta_dict[days]
    df[target_column] = df['delta1_m']
    for i in range(1, days):
        if debug:
            print('i=%d, days=%d'%(i, days))
        src_column='money_sft_'+ str(i)
        df[target_column] = df[target_column] + df[src_column]

    return df


def hsgt_handle_all_data(df):
    all_df=df
    latest_date=all_df.loc[0,'record_date']
    print(latest_date)

    del all_df['open']
    del all_df['high']
    del all_df['low']
    del all_df['volume']
    
    all_df['percent_tmp'] = all_df['percent']
    del all_df['percent']
    all_df['percent'] = all_df['percent_tmp']
    del all_df['percent_tmp']

    all_df['delta1']  = all_df.groupby('stock_code')['percent'].apply(lambda i:i.diff(-1))
    all_df['delta2']  =all_df.groupby('stock_code')['percent'].apply(lambda i:i.diff(-2))
    all_df['delta3']  =all_df.groupby('stock_code')['percent'].apply(lambda i:i.diff(-3))
    all_df['delta4']  =all_df.groupby('stock_code')['percent'].apply(lambda i:i.diff(-4))
    all_df['delta5']  =all_df.groupby('stock_code')['percent'].apply(lambda i:i.diff(-5))
    all_df['delta10'] =all_df.groupby('stock_code')['percent'].apply(lambda i:i.diff(-10))
    all_df['delta21'] =all_df.groupby('stock_code')['percent'].apply(lambda i:i.diff(-21))
    all_df['delta120']=all_df.groupby('stock_code')['percent'].apply(lambda i:i.diff(-120))
    
    all_df['delta1_share'] = all_df.groupby('stock_code')['share_holding'].apply(lambda i:i.diff(-1))
    all_df['delta1_m'] = all_df['close'] * all_df['delta1_share'] / 10000;
    del all_df['delta1_share']
    

    max_number=21
    #temp column added
    for index in range(1, max_number):
        column='money_sft_'+ str(index)
        all_df[column] = all_df.groupby('stock_code')['delta1_m'].shift(index*(-1))

    all_df=all_df.fillna(0)

    all_df=hsgt_get_delta_m_of_day(all_df, 2)
    all_df=hsgt_get_delta_m_of_day(all_df, 3)
    all_df=hsgt_get_delta_m_of_day(all_df, 4)
    all_df=hsgt_get_delta_m_of_day(all_df, 5)
    all_df=hsgt_get_delta_m_of_day(all_df, 10)
    all_df=hsgt_get_delta_m_of_day(all_df, 21)
    #all_df=hsgt_get_delta_m_of_day(all_df, 120)

    all_df=all_df.round(2)

    #temp column delete
    for index in range(1, max_number):
        column='money_sft_'+ str(index)
        del all_df[column]


    if debug:
        print(all_df.head(10))    

    return all_df, latest_date

    pass
            
def hsgt_get_daily_data(all_df):
    latest_date=all_df.loc[0,'record_date']
    daily_df=all_df[all_df['record_date'] == latest_date]
    return daily_df


def hsgt_daily_sort(daily_df, orderby='delta1'):
    sort_df=daily_df.sort_values(orderby, ascending=0)
    return sort_df;

def hsgt_write_to_file(f, k, df):
        f.write('<table class="gridtable">\n')


        f.write('    <tr>\n')

        #headline
        col_len=len(list(df))
        for j in range(0, col_len): 
            f.write('        <td>\n')
            if (j == 0):
                f.write('           <a> record__date</a>\n') #align
            else:
                f.write('           <a> %s</a>\n'%(list(df)[j]))
            f.write('        </td>\n')
        f.write('    </tr>\n')

        #dataline
        #f.write('%s\n'%(list(df)))
        df_len=len(df)
        for i in range(0, df_len):
            f.write('    <tr>\n')
            a_array=df[i:i+1].values
            tmp_stock_code=a_array[0][1] 
            if tmp_stock_code[0:2] == '60':
                stock_code_new='SH'+tmp_stock_code
            else:
                stock_code_new='SZ'+tmp_stock_code
            xueqiu_url='https://xueqiu.com/S/' + stock_code_new
            hsgt_url='../../cgi-bin/hsgt-search.cgi?name=' + tmp_stock_code


            for j in range(0, col_len): 
                f.write('        <td>\n')

                #set color to delta column, 5 is the position of percent
                #record_date  stock_code  stock_cname share_holding   close  percent  delta1  delta2  delta3  delta4  delta5  delta10 delta21 delta120    delta1_m    delta2_m    delta3_m    delta4_m    delta5_m    delta10_m   delta21_m
                if (j == k + 5):
                        f.write('           <a style="color: #FF0000"> %s</a>\n'%(a_array[0][j]))
                else:
                    if(j == 1): 
                        f.write('           <a href="%s" target="_blank"> %s[hsgt]</a>\n'%(hsgt_url, a_array[0][j]))
                    elif(j == 2):
                        f.write('           <a href="%s" target="_blank"> %s[xueqiu]</a>\n'%(xueqiu_url, a_array[0][j]))
                    else:
                        f.write('           <a> %s</a>\n'%(a_array[0][j]))
                
                f.write('        </td>\n')

            f.write('    </tr>\n')

        f.write('</table>\n')

###################################################################################

if __name__ == '__main__':
    df=hsgt_get_all_data()
    all_df, latest_date = hsgt_handle_all_data(df)

##################### html generation start ##############################################################
    save_dir = "hsgt"
    exec_command = "mkdir -p " + (save_dir)
    print(exec_command)
    os.system(exec_command)

    file_name=save_dir + '-' + latest_date
    newfile=save_dir + '/' + file_name + '.html'
    with open(newfile,'w') as f:

        f.write('<!DOCTYPE html>\n')
        f.write('<html>\n')
        f.write('<head>\n')
        f.write('<meta http-equiv="Content-Type" content="text/html; charset=utf-8" />\n')
        f.write('<title> hsgt-%s </title>\n'%(latest_date))
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
        f.write('<p style="color: #FF0000"> delta1: delta percent of 1 day </p>\n')
        f.write('<p style="color: #FF0000"> delta1_m: delta money of 1 day, delta share_holding * close </p>\n')
####################### data handle start ############################################################
        daily_df  = hsgt_get_daily_data(all_df)
        daily_net = daily_df['delta1_m'].sum()
        f.write('<p style="color: #FF0000"> delta1_m sum is: %.2fw rmb </p>\n'%(daily_net))

        delta_list = ['percent', 'delta1', 'delta2', 'delta3', 'delta4',  'delta5', 'delta10', 'delta21', 'delta120', 'delta1_m', 'delta2_m',    'delta3_m',   'delta4_m', 'delta5_m', 'delta10_m', 'delta21_m']
        lst_len = len(delta_list)
        for k in range(0, lst_len):
            f.write('           <p style="color: #FF0000">------------------------------------top10 order by %s desc---------------------------------------------- </p>\n'%(delta_list[k]))
            delta_tmp = hsgt_daily_sort(daily_df, delta_list[k])
            delta_tmp = delta_tmp.head(10)
            hsgt_write_to_file(f, k, delta_tmp)
            f.write('        <td>\n')
            f.write('        </td>\n')
            
####################### data handle end ############################################################

        f.write('</body>\n')
        f.write('\n')
        f.write('\n')
        f.write('</html>\n')
        f.write('\n')

##################### html generation end ##############################################################
    #copy to /var/www/html/hsgt
    newfile=save_dir + '/' + file_name + '.html'
    exec_command = 'cp -f ' + newfile + ' /var/www/html/hsgt/'
    os.system(exec_command)

     

hdata_hsgt.db_disconnect()
