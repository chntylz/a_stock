#!/usr/bin/env python  
# -*- coding: utf-8 -*-

import psycopg2 #使用的是PostgreSQL数据库
import tushare as ts
from Stocks import *
from HData_fina import *
import  datetime

from time import clock


import sys
import os
import time

#funcat
from funcat import *
from funcat.data.aaron_backend import AaronDataBackend
set_data_backend(AaronDataBackend())



stocks=Stocks("usr","usr")
hdata_fina=HData_fina("usr","usr")
debug = False
   

def get_fina_data():
    nowdate = datetime.datetime.now().date()    
    lastdate = nowdate - datetime.timedelta(365 * 2) #two years ago

    print('nowdate:%s, lastdate:%s' % (nowdate, lastdate))


    fina_data  =  hdata_fina.get_all_hdata_of_stock_accord_time( lastdate.strftime("%Y%m%d"), nowdate.strftime("%Y%m%d"))

    return fina_data

def fina_get_continuous_info(df, select='or_yoy', net_percent=20):
    all_df = df
    data_list = []
    group_by_stock_code_df=all_df.groupby('ts_code')
    for stock_code, group_df in group_by_stock_code_df:
        if debug:
            print(stock_code)
            print(group_df.head(1))

   
        
        #get stock_cname
        stock_name = symbol(stock_code)
        pos_s=stock_name.rfind('[')
        pos_e=stock_name.rfind(']')
        stock_name=stock_name[pos_s+1: pos_e]
        if debug:
            print(stock_name)
        #skip ST
        if ('ST' in stock_name):
            continue
 
        
        group_df=group_df.reset_index(drop=True) #reset index
        max_date=group_df.loc[0, 'ann_date']
        or_yoy=group_df.loc[0, 'or_yoy']
        netprofit_yoy=group_df.loc[0, 'netprofit_yoy']

        length=len(group_df)
        money_flag = 0
        for i in range(length):
            or_item = group_df.ix[i]['or_yoy']
            netprofit_item = group_df.ix[i]['netprofit_yoy']
            if debug:
                print('netprofit_item =%f'%(netprofit_item))

            if or_item >= net_percent and netprofit_item >= net_percent:
                pass
            else:
                break

        #algorithm
        if(i > 1):
             if group_df.ix[0]['or_yoy'] < group_df.ix[1]['or_yoy']:  #decline, skip
                continue
        else:
             continue


        if debug:
            print(max_date, stock_code, stock_name, or_yoy,  netprofit_yoy, i)

        data_list.append([max_date, stock_code, stock_name, or_yoy, netprofit_yoy,  i])  #i  is p_count

    data_column=['record_date', 'stock_code', 'stock_name', 'or_yoy', 'netprofit_yoy', 'p_count']

    ret_df = pd.DataFrame(data_list, columns=data_column)
    if select is 'or_yoy':
        ret_df = ret_df.sort_values('or_yoy', ascending=0)
    elif select is 'netprofit_yoy':
        ret_df = ret_df.sort_values('netprofit_yoy', ascending=0)


    return ret_df
    
def fina_handle_html_head(filename):
    with open(filename,'w') as f:
        f.write('<!DOCTYPE html>\n')
        f.write('<html>\n')
        f.write('<head>\n')
        f.write('<meta http-equiv="Content-Type" content="text/html; charset=utf-8" />\n')
        f.write('<title> fina-%s </title>\n' % (datetime.datetime.now().date()))
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
    pass

def fina_write_headline_column(f, df):

    f.write('    <tr>\n')
    #headline
    col_len=len(list(df))
    for j in range(0, col_len): 
        f.write('        <td>\n')
        f.write('        <a> %s</a>\n'%(list(df)[j]))
        f.write('        </td>\n')


    f.write('    </tr>\n')


def fina_handle_link(stock_code):

    tmp_stock_code=stock_code
    if tmp_stock_code[0:1] == '6':
        stock_code_new='SH'+tmp_stock_code
    else:
        stock_code_new='SZ'+tmp_stock_code
        
    xueqiu_url='https://xueqiu.com/S/' + stock_code_new
    hsgt_url='../../cgi-bin/hsgt-search.cgi?name=' + tmp_stock_code
    fina_url = xueqiu_url + '/detail#/ZYCWZB'    
    return xueqiu_url, hsgt_url, fina_url
    
    
def fina_write_to_file(f, df):
    f.write('<table class="gridtable">\n')

    #headline
    fina_write_headline_column(f, df)

    #dataline
    #f.write('%s\n'%(list(df)))
    df_len=len(df)
    for i in range(0, df_len): #loop line

        f.write('    <tr>\n')
        a_array=df[i:i+1].values  #get line of df
        tmp_stock_code=a_array[0][1] 
        xueqiu_url, hsgt_url, fina_url = fina_handle_link(tmp_stock_code)

        col_len=len(list(df))
        for j in range(0, col_len): #loop column
            f.write('        <td>\n')
            element_value = a_array[0][j] #get a[i][j] element
            #df_fina_column=['record_date', 'stock_code', 'stock_name', 'or_yoy', 'netprofit_yoy', 'p_count']
            if(j == 0): 
                f.write('           <a href="%s" target="_blank"> %s[fina]</a>\n'%(fina_url, element_value))
            elif(j == 1): 
                f.write('           <a href="%s" target="_blank"> %s[hsgt]</a>\n'%(hsgt_url, element_value))
            elif(j == 2):
                f.write('           <a href="%s" target="_blank"> %s</a>\n'%(xueqiu_url, element_value))
            elif(j == 3 or j == 4):
                f.write('           <a> %.2f%s</a>\n'%(element_value, '%'))
            else:
                f.write('           <a> %s</a>\n'%(element_value))
                     
                                
            f.write('        </td>\n')
        f.write('    </tr>\n')

    f.write('</table>\n')

    pass
    
    
def fina_handle_html_body(filename, df):
    with open(filename,'a') as f:
        #select condition
        fina_write_to_file(f, df)
        
    pass

def fina_handle_html_end(filename):
    with open(filename,'a') as f:
        f.write('        <td>\n')
        f.write('        </td>\n')
        f.write('</body>\n')
        f.write('\n')
        f.write('\n')
        f.write('</html>\n')
        f.write('\n')

    #copy to /var/www/html/fina
    os.system('mkdir -p /var/www/html/fina')
    exec_command = 'cp -f ' + filename + ' /var/www/html/fina/'
    os.system(exec_command)

    pass


    
    
    
def fina_generate_html(df):
    save_dir = "fina"
    exec_command = "mkdir -p " + (save_dir)
    print(exec_command)
    os.system(exec_command)

    file_name='finance'
    newfile=save_dir + '/' + file_name + '.html'

    fina_handle_html_head(newfile)
    fina_handle_html_body(newfile, df)
    fina_handle_html_end(newfile)
    
    
    
def get_example_data():

    hdata_fina.db_connect()#由于每次连接数据库都要耗时0.0几秒，故获取历史数据时统一连接

    df = get_fina_data()
    
    df_fina =  fina_get_continuous_info(df, 'or_yoy')

    hdata_fina.db_disconnect()

    return df, df_fina



if __name__ == '__main__':

    t1 = clock()
    df, df_fina = get_example_data()
    fina_generate_html(df_fina)
    t2 = clock()
    print("t1:%s, t2:%s, delta=%s"%(t1, t2, t2-t1))

           

