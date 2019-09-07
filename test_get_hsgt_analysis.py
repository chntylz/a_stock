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

def hsgt_handle_all_data(df):
    all_df=df
    latest_date=all_df.loc[0,'record_date']
    print(latest_date)

    del all_df['open']
    del all_df['high']
    del all_df['low']
    del all_df['volume']
    

    all_df['delta1']  =all_df.groupby('stock_code')['percent'].apply(lambda i:i.diff(-1))
    all_df['delta2']  =all_df.groupby('stock_code')['percent'].apply(lambda i:i.diff(-2))
    all_df['delta3']  =all_df.groupby('stock_code')['percent'].apply(lambda i:i.diff(-3))
    all_df['delta5']  =all_df.groupby('stock_code')['percent'].apply(lambda i:i.diff(-5))
    all_df['delta10'] =all_df.groupby('stock_code')['percent'].apply(lambda i:i.diff(-10))
    all_df['delta21'] =all_df.groupby('stock_code')['percent'].apply(lambda i:i.diff(-21))
    all_df['delta120']=all_df.groupby('stock_code')['percent'].apply(lambda i:i.diff(-120))
    all_df=all_df.round(2)
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

        col_len=len(list(df))
        for j in range(0, col_len): 
            f.write('        <td>\n')
            f.write('           <a> %s</a>\n'%(list(df)[j]))
            f.write('        </td>\n')
        f.write('    </tr>\n')

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

                #set color to delta column, 5 is the position of first delta1
                #record_date stock_code  stock_cname share_holding   percent delta1  delta2  delta3  delta5  delta10 delta21 delta120
                if (j == k + 6):
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
####################### data handle start ############################################################
        daily_df  = hsgt_get_daily_data(all_df)
        delta_list = ['delta1', 'delta2', 'delta3', 'delta5', 'delta10', 'delta21', 'delta120', 'percent']
        lst_len = len(delta_list)
        for k in range(0, lst_len):
            f.write('           <a style="color: #FF0000">------------------------------------order by %s desc---------------------------------------------- </a>\n'%(delta_list[k]))
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
