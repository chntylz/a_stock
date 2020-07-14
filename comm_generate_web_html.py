#!/usr/bin/env python
# -*- coding: utf-8 -*- 
import os,sys,time, datetime
import tushare as ts
import numpy as np
import pandas as pd


import time

#keep 0.01 accrucy
pd.set_option('display.float_format',lambda x : '%.2f' % x)



#funcat
from funcat import *
from funcat.data.aaron_backend import AaronDataBackend
set_data_backend(AaronDataBackend())


import psycopg2
from Stocks import *
from HData_hsgt import *
from HData_dailybasic import *
from HData_select import *

from file_interface import *

hsgtdata=HData_hsgt("usr","usr")
stocks=Stocks("usr","usr")
db_daily=HData_dailybasic("usr", "usr")
sdata=HData_select("usr","usr")

token='21dddafc47513ea46b89057b2c4edf7b44882b3e92274b431f199552'
pro = ts.pro_api(token)


#get basic stock info
#basic_df=ts.get_stock_basics()
basic_df=stocks.get_all_data()

debug=0
#debug=1

def comm_get_total_mv(stock_code):
    total_mv_df = db_daily.get_data_from_hdata(stock_code=stock_code, limit=1)
    #市值，单位：亿
    if len(total_mv_df) > 0:
        total_mv = total_mv_df['total_mv'][0]/10000 
    else:
        total_mv = 0

    return round(total_mv,2)



  
############################################################################################################

def hsgt_get_daily_data(all_df):
    latest_date=all_df.loc[0,'record_date']
    daily_df=all_df[all_df['record_date'] == latest_date]
    return daily_df


def hsgt_daily_sort(daily_df, orderby='delta1'):
    sort_df=daily_df.sort_values(orderby, ascending=0)
    return sort_df;


def hsgt_get_continuous_info(df, select):
    all_df = df
    data_list = []
    group_by_stock_code_df=all_df.groupby('stock_code')
    for stock_code, group_df in group_by_stock_code_df:
        if debug:
            print(stock_code)
            print(group_df.head(1))
        
        group_df    = group_df.reset_index(drop=True) #reset index
        max_date    = group_df.loc[0, 'record_date']
        stock_cname = group_df.loc[0, 'stock_cname']
        hk_pct      = group_df.loc[0, 'hk_pct']
        delta1      = group_df.loc[0, 'delta1']
        delta1_m    = group_df.loc[0, 'delta1_m']
        close       = group_df.loc[0, 'close']
        a_pct     = group_df.loc[0, 'a_pct']

        length=len(group_df)
        money_total = 0
        flag_m = group_df.loc[0]['delta1_m']
        if flag_m > 0:
            conti_flag = 1
        else:
            conti_flag = 0
        
        for i in range(length):
            delta_m = group_df.loc[i]['delta1_m']
            if debug:
                print('delta_m=%f'%(delta_m))

            if delta_m >= 0:
                tmp_flag = 1
            else:
                tmp_flag = 0

            if conti_flag == tmp_flag:
                money_total = money_total + delta_m
            else:
                break
                
        money_total = round(money_total,2)
        if debug:
            print(max_date, stock_code, stock_cname, hk_pct, close, a_pct, delta1, i, money_total)

        data_list.append([max_date, stock_code, stock_cname, hk_pct, close, a_pct, delta1, delta1_m, i, money_total])  #i  is conti_day

    data_column=['record_date', 'stock_code', 'stock_cname', 'hk_pct', 'close', 'a_pct', 'delta1', 'delta1_m', 'conti_day', 'money_total']

    ret_df = pd.DataFrame(data_list, columns=data_column)
    ret_df['m_per_day'] = ret_df.money_total / ret_df.conti_day
    ret_df=ret_df.round(2)
    #ret_df = ret_df.sort_values('money_total', ascending=0)
    #ret_df = ret_df.sort_values('conti_day', ascending=0)
    if select is 'p_money':
        ret_df = ret_df.sort_values('m_per_day', ascending=0)
    elif select is 'p_continous_day':
        ret_df = ret_df.sort_values('conti_day', ascending=0)

    return ret_df
############################################################################################################



def comm_write_headline_column(f, df):

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

    #add total_mv
    f.write('        <td>\n')
    f.write('           <a> total_mv </a>\n')
    f.write('        </td>\n')

    #add industry
    f.write('        <td>\n')
    f.write('           <a> _industry_ </a>\n')
    f.write('        </td>\n')

    f.write('    </tr>\n')

def comm_handle_link(stock_code):

    tmp_stock_code=stock_code
    if tmp_stock_code[0:1] == '6':
        stock_code_new='SH'+tmp_stock_code
    else:
        stock_code_new='SZ'+tmp_stock_code
        
    xueqiu_url='https://xueqiu.com/S/' + stock_code_new
    hsgt_url='../../cgi-bin/hsgt-search.cgi?name=' + tmp_stock_code

    fina_url = xueqiu_url + '/detail#/ZYCWZB'    
    return xueqiu_url, hsgt_url, fina_url

#image_name='2019-09-23-1-002436-兴森科技-814-878-891-796-840.png'
def get_conti_day_from_s_db(image_name):
    
    i = 1
    j = 1

    if len(image_name) < 19:
        return 0

    stock_code = image_name[13:19]
    curr_date = image_name[0:10]
    
    if debug:
        print('get_conti_day_from_s_db(): curr_date=%s, stock_code=%s' % (curr_date, stock_code) )

    now_date = datetime.datetime.strptime(curr_date,'%Y-%m-%d')

    while True:
        s_date = (now_date - datetime.timedelta(int(i))).strftime("%Y-%m-%d")
        e_date = (now_date - datetime.timedelta(int(i-1))).strftime("%Y-%m-%d")
        select_df = sdata.get_data_from_hdata(stock_code=stock_code, start_date=s_date, end_date=e_date, limit=1)
        if debug:
            print('i = %d,s_date=%s, e_date=%s select_df:' % (i, e_date, s_date))
            print('%s' % select_df)

        #todo
        if is_work_day(datetime.datetime.strptime(e_date,'%Y-%m-%d')):
            if len(select_df) > 0:
                j = j + 1
            else:
                return j  
        else:
            if debug:
                print("%s is holiday" % s_date)

        i = i+1
    pass
    
def comm_write_to_file(f, k, df, filename):
    f.write('<table class="gridtable">\n')

    #headline
    comm_write_headline_column(f, df)

    #dataline
    #f.write('%s\n'%(list(df)))
    df_len=len(df)
    for i in range(0, df_len): #loop line

        f.write('    <tr>\n')
        a_array=df[i:i+1].values  #get line of df
        tmp_stock_code=a_array[0][1] 
        xueqiu_url, hsgt_url, fina_url = comm_handle_link(tmp_stock_code)

        col_len=len(list(df))
        for j in range(0, col_len): #loop column
            f.write('        <td>\n')
            element_value = a_array[0][j] #get a[i][j] element
            if debug:
                print('element_value: %s' % element_value)
                                     
            if k is -1: # normal case
                #data_column=['record_date', 'stock_code', 'stock_cname', 'hk_pct', 'close', 'delta1', 'delta1_m', 'conti_day', 'money_total']
                if(j == 0): 
                    f.write('           <a href="%s" target="_blank"> %s[fina]</a>\n'%(fina_url, element_value))
                elif(j == 1): 
                    f.write('           <a href="%s" target="_blank"> %s[hsgt]</a>\n'%(hsgt_url, element_value))
                elif(j == 2):
                    f.write('           <a href="%s" target="_blank"> %s</a>\n'%(xueqiu_url, element_value))
                elif(j == 3):
                    f.write('           <a> %.2f%s</a>\n'%(element_value, '%'))
                elif(j == 8):
                    f.write('           <a> %.2f</a>\n'%(element_value))
                else:
                    #element_value=2019-09-23-1-002436-兴森科技-814-878-891-796-840.png
                    if 'png' in str(element_value):
                        conti_day = str(get_conti_day_from_s_db(str(element_value)))
                        if debug:
                            print('basic 2days up: conti_day=%s' % conti_day)

                        if 'buy' in str(element_value) and 'bottom' in str(element_value):#zig+bottom
                            f.write('           <a href="%s" target="_blank"> %s</a>\n'%( element_value, conti_day + 'zig+b'))
                        elif 'buy' in str(element_value):#zig
                            f.write('           <a href="%s" target="_blank"> %s</a>\n'%( element_value, conti_day + 'zig'))
                        elif 'bottom' in str(element_value):#bottom
                            f.write('           <a href="%s" target="_blank"> %s</a>\n'%( element_value, conti_day + 'bottom'))
                        else:
                            f.write('           <a href="%s" target="_blank"> %s</a>\n'%( element_value, conti_day ))
                    else:
                        f.write('           <a> %s</a>\n'%(element_value))
            
            else: #special case for get red color column
                #set color to delta column, 6 is the position of hk_pct
                #record_date,  stock_code,  stock_cname, share_holding,   close, a_pct,  hk_pct,  delta1,  delta2,  delta3,  delta4,  delta5,  delta10, delta21, delta120,    delta1_m,    delta2_m,  delta3_m, delta4_m, delta5_m,    delta10_m,   delta21_m
                if (j == k + 6):
                    f.write('           <a style="color: #FF0000"> %s</a>\n'%(element_value))
                else:
                    if(j == 0): 
                        f.write('           <a href="%s" target="_blank"> %s[fina]</a>\n'%(fina_url, element_value))
                    elif(j == 1): 
                        f.write('           <a href="%s" target="_blank"> %s[hsgt]</a>\n'%(hsgt_url, element_value))
                    elif(j == 2):
                        f.write('           <a href="%s" target="_blank"> %s</a>\n'%(xueqiu_url, element_value))
                    else:
                        f.write('           <a> %s</a>\n'%(element_value))
            
                                
            f.write('        </td>\n')
        #add total_mv
        f.write('        <td>\n')
        f.write('           <a> %s </a>\n' %  (comm_get_total_mv(tmp_stock_code)))
        f.write('        </td>\n')

        #add industry
        f.write('        <td>\n')
        f.write('           <a> %s </a>\n' % (basic_df.loc[tmp_stock_code]['industry']))
        f.write('        </td>\n')

        f.write('    </tr>\n')

    f.write('</table>\n')

    pass
    
         
def comm_handle_html_head(filename, title, latest_date):
    with open(filename,'w') as f:
        f.write('<!DOCTYPE html>\n')
        f.write('<html>\n')
        f.write('<head>\n')
        f.write('<meta http-equiv="Content-Type" content="text/html; charset=utf-8" />\n')
        f.write('<title> %s-%s </title>\n'%(title, latest_date))
        f.write('\n')
        f.write('\n')
        f.write('<style type="text/css">a {text-decoration: none}\n')
        f.write('\n')
        f.write('\n')

        f.write('/* gridtable */\n')
        f.write('table {\n')
        f.write('    font-size:18px;\n')
        f.write('    color:#000;\n')
        f.write('    border-width: 1px;\n')
        f.write('    border-color: #333333;\n')
        f.write('    border-collapse: collapse;\n')
        f.write('}\n')
        f.write('table tr {\n')
        f.write('    border-width: 1px;\n')
        f.write('    padding: 8px;\n')
        f.write('    border-style: solid;\n')
        f.write('    border-color: #333333;\n')
        f.write('}\n')
        f.write('table th {\n')
        f.write('    border-width: 1px;\n')
        f.write('    padding: 8px;\n')
        f.write('    border-style: solid;\n')
        f.write('    border-color: #333333;\n')
        f.write('}\n')
        f.write('table td {\n')
        f.write('    border-width: 1px;\n')
        f.write('    padding: 8px;\n')
        f.write('    border-style: solid;\n')
        f.write('    border-color: #333333;\n')
        f.write('}\n')
        
        '''
        f.write('    table tr:nth-child(odd){\n')
        f.write('    background-color: #eeeeee;\n')
        f.write('    }\n')

        f.write('    table tr:nth-child(even){\n')
        f.write('    background-color: #cccccc;\n')
        f.write('    }\n')
        '''

        f.write('/* /gridtable */\n')

        f.write('\n')
        f.write('\n')
        f.write('</style>\n')

        f.write('</head>\n')
        f.write('\n')
        f.write('\n')
 
        f.write('<body>\n')
       

    pass

#filename includes hsgt or fina
def comm_handle_html_body(filename, all_df, select='topy10'):
    if debug:
        print('filename: %s' % filename)
    with open(filename,'a') as f:
        if 'hsgt' in filename:
            daily_df  = hsgt_get_daily_data(all_df)
            daily_net = daily_df['delta1_m'].sum()
            f.write('<p style="color: #FF0000"> delta1_m sum is: %.2fw rmb </p>\n'%(daily_net))
            if select is 'top10':

                delta_list = ['hk_pct', 'delta1', 'delta1_m', 'delta2', 'delta3', 'delta4',  'delta5', 'delta10', 'delta21', 'delta120', 'delta2_m',    'delta3_m',   'delta4_m', 'delta5_m', 'delta10_m', 'delta21_m']
                lst_len = len(delta_list)
                for k in range(0, lst_len):
                    f.write('           <p style="color: #FF0000">------------------------------------top10 order by %s desc---------------------------------------------- </p>\n'%(delta_list[k]))
                    delta_tmp = hsgt_daily_sort(daily_df, delta_list[k])
                    delta_tmp = delta_tmp.head(20)
                    comm_write_to_file(f, k, delta_tmp, filename)

            elif select is 'p_money':
                conti_df = hsgt_get_continuous_info(all_df, 'p_money')
                #select condition
                conti_df = conti_df[ (conti_df.money_total / conti_df.conti_day > 1000) & (conti_df.money_total > 2000) &(conti_df.delta1_m > 1000)] 
                comm_write_to_file(f, -1, conti_df, filename)

            elif select is 'p_continous_day':
                conti_df = hsgt_get_continuous_info(all_df, 'p_continous_day')
                #select condition
                #conti_df = conti_df[ (conti_df.money_total / conti_df.conti_day > 1000) & (conti_df.money_total > 2000) &(conti_df.delta1_m > 1000)] 
                conti_df = conti_df[conti_df.money_total > 2000] 
                comm_write_to_file(f, -1, conti_df, filename)
        else:
            comm_write_to_file(f, -1, all_df, filename)
    pass

def comm_handle_html_end(filename, target_dir=''):
    with open(filename,'a') as f:
        f.write('        <td>\n')
        f.write('        </td>\n')
        f.write('</body>\n')
        f.write('\n')
        f.write('\n')
        f.write('</html>\n')
        f.write('\n')

    if 'hsgt' in filename:
        #copy to /var/www/html/hsgt
        os.system('mkdir -p /var/www/html/hsgt')
        exec_command = 'cp -rf ' + filename + ' /var/www/html/hsgt/'
        os.system(exec_command)
    elif 'fina' in filename:
        #copy to /var/www/html/fina
        os.system('mkdir -p /var/www/html/stock_data/finance')
        exec_command = 'cp -rf ' + filename + ' /var/www/html/stock_data/finance/'
        os.system(exec_command)
    else:
        exec_command = 'cp -rf ' + filename + ' /var/www/html/stock_data/' + target_dir + '/'
        os.system(exec_command)

    if debug:
        print(exec_command)
    pass




def comm_get_hsgt_continous_info(df):
    hsgt_df = df
    hsgt_df_len = len(hsgt_df)
    money_total = 0
    flag_m = hsgt_df.loc[0]['delta1_m']
    if flag_m > 0:
        conti_flag = 1
    else:
        conti_flag = 0

    for i in range(hsgt_df_len):
        delta_m = hsgt_df.loc[i]['delta1_m']
        if debug:
            print('delta_m=%f'%(delta_m))

        if delta_m >= 0:
            tmp_flag = 1
        else:
            tmp_flag = 0

        if conti_flag == tmp_flag:
            money_total = money_total + delta_m
        else:
            break
    
    money_total = round(money_total, 2)
    
    return i, money_total

 

def comm_handle_hsgt_data(df):

    all_df =df 

    del all_df['open']
    del all_df['high']
    del all_df['low']
    del all_df['volume']

    if len(all_df) > 0:
        #the_first_line - the_second_line
        all_df['delta1']  = all_df.groupby('stock_code')['percent'].apply(lambda i:i.diff(-1))
        all_df['delta1_share'] = all_df.groupby('stock_code')['share_holding'].apply(lambda i:i.diff(-1))
        all_df['delta1_m'] = all_df['close'] * all_df['delta1_share'] / 10000;
        del all_df['delta1_share']

        all_df=all_df[all_df['delta1_m'] != 0]
        all_df=all_df.reset_index(drop=True)

        if debug:
            print(type(all_df))
            print(all_df.head(2))

    hsgt_df = all_df

    hsgt_df_len = len(hsgt_df)
    if hsgt_df_len > 1:
        hsgt_date           = hsgt_df['record_date'][0]
        hsgt_share          = hsgt_df['share_holding'][0]
        hsgt_percent        = hsgt_df['percent'][0]
        hsgt_delta1         = hsgt_df['percent'][0] - hsgt_df['percent'][1]
        hsgt_delta1         = round(hsgt_delta1, 2)
        hsgt_deltam         = (hsgt_df['share_holding'][0] - hsgt_df['share_holding'][1]) * hsgt_df['close'][0] /10000.0
        hsgt_deltam         = round(hsgt_deltam, 2)
        conti_day, money_total= comm_get_hsgt_continous_info(hsgt_df)

    elif hsgt_df_len > 0:
        hsgt_date           = hsgt_df['record_date'][0]
        hsgt_share          = hsgt_df['share_holding'][0]
        hsgt_percent        = hsgt_df['percent'][0]
        hsgt_delta1         = hsgt_df['percent'][0]
        hsgt_deltam         = hsgt_df['share_holding'][0] * hsgt_df['close'][0]/10000.0
        hsgt_deltam         = round(hsgt_deltam, 2)
        conti_day           = 1
        money_total         = hsgt_deltam
    else:
        hsgt_date           = ''
        hsgt_share          = 0
        hsgt_percent        = 0
        hsgt_delta1         = 0
        hsgt_deltam         = 0
        conti_day           = 0
        money_total         = 0

    return hsgt_date, hsgt_share, hsgt_percent, hsgt_delta1, hsgt_deltam, conti_day, money_total   



def insert_industry(dict_name, key):
    if dict_name.get(key) is None :
       dict_name.setdefault(key, 1)
    else:
        dict_name[key]=dict_name[key] + 1


def comm_generate_web_dataframe(curr_dir, images, curr_day, dict_industry):
    
    txt_file = curr_dir + '/' + curr_day +'.txt'
    
    with open(txt_file,'w') as f:
        f.write('\n')

    data_list = []
    for image in images:

        #'2019-07-09-600095-哈高科-873-960-960-873-997.png'
        #2019-09-23-1-002436-兴森科技-814-878-891-796-840.png
        tmp_image=image[0:image.rfind('.')]
        if debug:
            print('tmp_image: %s' % (tmp_image))

        #stock_code=image[11:17]
        stock_code=image[13:19]
        stock_name = symbol(stock_code)
        pos_s=stock_name.rfind('[')
        pos_e=stock_name.rfind(']')
        stock_name=stock_name[pos_s+1: pos_e]
        
        #save stock_code to txt_file
        with open(txt_file,'a') as f:
            f.write('%s \n' % stock_code)

        #funcat call
        T(curr_day)
        S(stock_code)
        pre_close = REF(C, 1)
        open_p = (O - pre_close)/pre_close
        open_p = round (open_p.value, 4)
        open_jump=open_p - 0.02
        if debug:
            print(str(curr_day), stock_code, O, H, L, C, open_p)

        close_p = (C - pre_close)/pre_close
        close_p = round (close_p.value, 4) * 100

        all_df = hsgtdata.get_data_from_hdata(stock_code=stock_code, end_date=curr_day, limit=60)
        hsgt_date, hsgt_share, hsgt_percent, hsgt_delta1, hsgt_deltam, conti_day, money_total = comm_handle_hsgt_data(all_df)

        total_mv =  comm_get_total_mv(stock_code)

        industry_name = basic_df.loc[stock_code]['industry']
        insert_industry(dict_industry, industry_name)


        data_list.append([curr_day, stock_code, stock_name, close_p, C.value, image, hsgt_date, hsgt_share, hsgt_percent, hsgt_delta1, hsgt_deltam, conti_day, money_total, total_mv ])

    data_column = ['cur_date', 'code', 'name', 'a_pct', 'close', 'image_url', 'hk_date', 'hk_share', 'hk_pct', 'hk_delta1', 'hk_deltam', 'conti_day', 'hk_m_total', 'total_mv']
    ret_df=pd.DataFrame(data_list, columns=data_column)
    ret_df['m_per_day'] = ret_df.hk_m_total / ret_df.conti_day
    ret_df = ret_df.fillna(0)
    ret_df=ret_df.round(2)
    if debug:
        print(ret_df)

    data_column = ['cur_date', 'code', 'name', 'a_pct', 'close', 'image_url', 'hk_date', 'hk_share', 'hk_pct', 'hk_delta1', 'hk_deltam', 'conti_day', 'hk_m_total', 'm_per_day', 'total_mv']
    ret_df=ret_df.loc[:,data_column]

    ret_df = ret_df.sort_values('hk_m_total', ascending=0)
 
    return ret_df


def handle_divided(stock_code=None,
        curr_date=None):

    is_div = False
    div_value = 0

    if stock_code[0:1] == '6':
        stock_code_new= stock_code + '.SH'
    else:
        stock_code_new= stock_code + '.SZ'

    df = pro.dividend(ts_code=stock_code_new, fields='ts_code,div_proc,stk_div,record_date,ex_date')

    time.sleep(0.6)

    #delete row where value is equal to None
    df = df[~df.astype(str).eq('None').any(1)]

    #reset index
    df=df.reset_index(drop=True)

    length = len(df)

    if length == 0 :
        return is_div, div_value

    if debug:
        print(df.head(1))

    ex_date=df['ex_date'][0]
    div_value=df['stk_div'][0] 

    if debug:
        print('ex_date=%s'% (ex_date))

    
    if curr_date == ex_date:
        is_div = True
    else:
        is_div = False

    return is_div, div_value

