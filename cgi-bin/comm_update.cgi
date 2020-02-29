#!/usr/bin/env python
# -*- coding: utf-8 -*- 
import os,sys,time, datetime
import cgi

import psycopg2 #使用的是PostgreSQL数据库
import tushare as ts
import numpy as np
import pandas as pd

from HData_hsgt import *
hdata_hsgt=HData_hsgt("usr","usr")

debug=0

nowdate=datetime.datetime.now().date()
str_date= nowdate.strftime("%Y-%m-%d")

def get_stock_info(file_name):
    stock_list = []
    with open(file_name) as f:
        for line in f:
            if debug:
                print (line, len(line))
            if len(line) < 6 or '#' in line:
                if debug:
                    print('unvalid line data, skip!')
                continue
            space_pos = line.rfind(' ')
            stock_list.append([line[0:space_pos], line[space_pos+1: -1]])

    return stock_list



def show_realdata():
    #my_list=['300750','300552', '000401', '300458','300014', '601958', '601117', '600588', '002230']
    #my_list_cn=['ningdeshidai','wanjikeji', 'jidongshuini', 'quanzhikeji', 'yiweilineng', 'jinmugufen', 'zhongguohuaxue', 'yongyouwangluo', 'kedaxunfei']

    data_list = []

    file_name = 'my_optional.txt'
    my_list = get_stock_info(file_name)
    if debug:
        print(my_list)

    for i in range(len(my_list)):

        new_date        = str_date
        new_code        = my_list[i][0]
        new_name        = my_list[i][1]
        if debug:
            print("new_code:%s" % new_code)
        
        df = ts.get_realtime_quotes(new_code)
        new_pre_price   = df['pre_close'][0]
        new_price       = df['price'][0]
       
        hsgt_df = hdata_hsgt.get_limit_hdata_of_stock_code(new_code, new_date, 2)
        if debug:
            print(hsgt_df)
        hsgt_df_len = len(hsgt_df)
        if hsgt_df_len > 1: 
            new_hsgt_date           = hsgt_df['record_date'][1]
            new_hsgt_share_holding  = hsgt_df['share_holding'][1]
            new_hsgt_percent        = hsgt_df['percent'][1]
            new_hsgt_delta1         = hsgt_df['percent'][1] - hsgt_df['percent'][0]
            new_hsgt_deltam         = (hsgt_df['share_holding'][1] - hsgt_df['share_holding'][0]) * float(new_pre_price)/10000.0
        elif hsgt_df_len > 0: 
            new_hsgt_date           = hsgt_df['record_date'][0]
            new_hsgt_share_holding  = hsgt_df['share_holding'][0]
            new_hsgt_percent        = hsgt_df['percent'][0]
            new_hsgt_delta1         = hsgt_df['percent'][0] 
            new_hsgt_deltam         = hsgt_df['share_holding'][0] * float(new_pre_price)/10000.0
        else:
            new_hsgt_date           = ''
            new_hsgt_share_holding  = 0
            new_hsgt_percent        = 0
            new_hsgt_delta1         = 0
            new_hsgt_deltam         = 0
        
        data_list.append([new_date, new_code, new_name, new_pre_price, new_price, new_hsgt_date, new_hsgt_share_holding, new_hsgt_percent, new_hsgt_delta1, new_hsgt_deltam])


        #data_list.append([str_date, my_list[i], my_list_cn[i], df['pre_close'][0], df['price'][0] ])

    data_column = ['date', 'code', 'name', 'pre_price', 'price', 'hsgt_date', 'hsgt_share_holding', 'hsgt_percent', 'hsgt_delta1', 'hsgt_deltam' ]
    ret_df=pd.DataFrame(data_list, columns=data_column)
 
    return ret_df

def cgi_generate_html_1(df):
    '''
    print """Content-type: text/html\r\n\r\n


    <html lang="zh">
      <head>
        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
        <meta http-equiv="refresh" content="5">
        <title>comm_update</title>
      </head>
      <body>
       %s 
      </body>
    </html>
    """ % (df.to_html())
    '''






    print("Content-type: text/html")
    print("")


    print("<html lang='zh'> ")
    print("  <head>")
    print("    <meta http-equiv='Content-Type' content='text/html; charset=UTF-8'>")
    print("    <meta http-equiv='refresh' content='5'>")
    print("    <title>comm_update</title>")
    print("  </head>")
    print("  <body>")

    print("  <h2> I am cgi </h2>")
    print("  %s " % df.to_html())
    print("  </body>")
    print("</html>")
    print("")






   
   
def cgi_handle_html_head():
    print("Content-type: text/html")
    print("")

    print('<!DOCTYPE html>\n')
    print('<html>\n')
    print('<head>\n')
    print('<meta http-equiv="Content-Type" content="text/html; charset=utf-8" />\n')
    print('<meta http-equiv="refresh" content="5">\n')
    print('<title> fina-%s </title>\n' % (datetime.datetime.now().date()))
    print('\n')
    print('\n')
    print('<style type="text/css">a {text-decoration: none}\n')
    print('\n')
    print('\n')

    print('/* gridtable */\n')
    print('table.gridtable {\n')
    print('    font-size:15px;\n')
    print('    color:#000;\n')
    print('    border-width: 1px;\n')
    print('    border-color: #333333;\n')
    print('    border-collapse: collapse;\n')
    print('}\n')
    print('table.gridtable th {\n')
    print('    border-width: 1px;\n')
    print('    padding: 8px;\n')
    print('    border-style: solid;\n')
    print('    border-color: #333333;\n')
    print('    background-color: #dedede;\n')
    print('}\n')
    print('table.gridtable td {\n')
    print('    border-width: 1px;\n')
    print('    padding: 8px;\n')
    print('    border-style: solid;\n')
    print('    border-color: #333333;\n')
    print('    background-color: #ffffff;\n')
    print('}\n')
    print('/* /gridtable */\n')

    print('\n')
    print('\n')
    print('</style>\n')

    print('</head>\n')
    print('\n')
    print('\n')

    print('<body>\n')
    print('\n')
    print('\n')
    print('\n')
    '''
    print('<p>-----------------------------------我是分割线-----------------------------------</p>\n')
    print('<p  style="color:blue;"> or_yoy:        营业收入同比增长</p>')
    print('<p  style="color:blue;"> netprofit_yoy: 净利润同比增长</p>')
    print('<p  style="color:blue;"> p_count:       连续增长次数，并且or_yoy不低于上一次 </p>')
    print('<p>-----------------------------------我是分割线-----------------------------------</p>\n')
    '''
    print('<p>----------------------------------------------------------------------</p>\n')
    print('<p>----------------------------------------------------------------------</p>\n')
    print('\n')
    print('\n')

def cgi_write_headline_column(df):

    print('    <tr>\n')
    #headline
    col_len=len(list(df))
    for j in range(0, col_len): 
        print('        <td>\n')
        print('        <a> %s</a>\n'%(list(df)[j]))
        print('        </td>\n')

    print('    </tr>\n')


def cgi_handle_link(stock_code):

    tmp_stock_code=stock_code
    if tmp_stock_code[0:1] == '6':
        stock_code_new='SH'+tmp_stock_code
    else:
        stock_code_new='SZ'+tmp_stock_code
        
    xueqiu_url='https://xueqiu.com/S/' + stock_code_new
    hsgt_url='../../cgi-bin/hsgt-search.cgi?name=' + tmp_stock_code
    cgi_url = xueqiu_url + '/detail#/ZYCWZB'    
    return xueqiu_url, hsgt_url, cgi_url
    
    
def cgi_write_to_file( df):
    print('<table class="gridtable">\n')

    #headline
    cgi_write_headline_column(df)

    #dataline
    #print('%s\n'%(list(df)))
    df_len=len(df)
    for i in range(0, df_len): #loop line

        print('    <tr>\n')
        a_array=df[i:i+1].values  #get line of df
        tmp_stock_code=a_array[0][1] 
        xueqiu_url, hsgt_url, cgi_url = cgi_handle_link(tmp_stock_code)

        col_len=len(list(df))
        for j in range(0, col_len): #loop column
            print('        <td>\n')
            element_value = a_array[0][j] #get a[i][j] element
            #df_cgi_column=['record_date', 'stock_code', 'stock_name', 'or_yoy', 'netprofit_yoy', 'p_count']
            if(j == 0): 
                print('           <a href="%s" target="_blank"> %s[fina]</a>\n'%(cgi_url, element_value))
            elif(j == 1): 
                print('           <a href="%s" target="_blank"> %s[hsgt]</a>\n'%(hsgt_url, element_value))
            elif(j == 2):
                print('           <a href="%s" target="_blank"> %s</a>\n'%(xueqiu_url, element_value))
            elif(j == col_len - 1):
                print('           <a> %.2f</a>\n'%(element_value))
            else:
                print('           <a> %s</a>\n'%(element_value))
                     
                                
            print('        </td>\n')
        print('    </tr>\n')

    print('</table>\n')

    pass
    
    
def cgi_handle_html_body(df):
    #select condition
    cgi_write_to_file(df)
    pass

def cgi_handle_html_end():
    print('        <td>\n')
    print('        </td>\n')
    print('</body>\n')
    print('\n')
    print('\n')
    print('</html>\n')
    print('\n')

    pass


    
    
    
def cgi_generate_html(df):
    cgi_handle_html_head()
    cgi_handle_html_body(df)
    cgi_handle_html_end()
    
    
    

if __name__ == '__main__':

    df=show_realdata()
    if debug:
        print(df)

    cgi_generate_html(df)
