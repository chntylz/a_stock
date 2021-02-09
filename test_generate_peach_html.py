#!/usr/bin/env python
#coding:utf-8
import os,sys

import psycopg2 #使用的是PostgreSQL数据库
import tushare as ts
from HData_select import *
from HData_day import *
from HData_hsgt import *
import  datetime
import time
from pysnow_ball.HData_xq_day import *

from comm_generate_web_html import *

import numpy as np
import pandas as pd
#keep 0.01 accrucy
pd.set_option('display.float_format',lambda x : '%.2f' % x)


#funcat
from funcat import *
from funcat.data.aaron_backend import AaronDataBackend
set_data_backend(AaronDataBackend())

hdata=HData_xq_day("usr","usr")
sdata=HData_select("usr","usr")
hsgtdata=HData_hsgt("usr","usr")

dict_industry={}



debug=0
debug=1

from sys import argv
# 如果执行的方式错误输出使用方法
USAGE = '''
用法错误，正确方式如下：
python demo.py 1
'''
if len(argv) > 2:
    print(USAGE)  # 如果传入的参数不足，输出正确用法
    exit(1) # 异常退出(下面的代码将不会被执行)

script_name, para1 = argv  # 将传入的参数赋值进行使用
print("%s, %d"%(script_name, int(para1)))



nowdate=datetime.datetime.now().date()
#test
nowdate=nowdate-datetime.timedelta(int(para1))
lastdate=nowdate-datetime.timedelta(1)

curr_day=nowdate.strftime("%Y-%m-%d")
curr_day_w=nowdate.strftime("%Y-%m-%d-%w")
last_day=lastdate.strftime("%Y-%m-%d")
print("curr_day:%s, last_day:%s"%(curr_day, last_day))

stock_data_dir="stock_data"
curr_dir=curr_day_w+'-peach'


def peach_continue_handle_html_body_special(newfile, date):
    f = newfile
    curr_day = date
    with open(newfile,'a') as f:
        f.write('\n')
        f.write('\n')
        f.write('\n')
        f.write('\n')
        f.write('<p> 日期 %s </p>\n' %(curr_day))

        t1 = time.time()
        df = get_today_item(curr_day)
        print('delta time= %s ' % (time.time() - t1))

        # 找出上涨的股票
        df_up = df[df['percent'] > 0.00]
        # 走平股数
        df_even = df[df['percent'] == 0.00]
        # 找出下跌的股票
        df_down = df[df['percent'] < 0.00]

        # 找出涨停的股票
        limit_up = df[df['percent'] >= 9.70]
        limit_down = df[df['percent'] <= -9.70]

        s_debug= ('<p> A股上涨个数： %d,  A股下跌个数： %d,  A股走平个数:  %d</p>' % (df_up.shape[0], df_down.shape[0], df_even.shape[0]))
        print(s_debug)
        f.write('%s\n'%(s_debug))

        s_debug=('<p> 涨停数量：%d 个</p>' % (limit_up.shape[0]))
        print(s_debug)
        f.write('%s\n'%(s_debug))

        s_debug=('<p> 跌停数量：%d 个</p>' % (limit_down.shape[0]))
        print(s_debug)
        f.write('%s\n'%(s_debug))

        f.write('<p>-----------------------------------我是分割线-----------------------------------</p>\n')
    
        f.write('<p  style="color:green;">绿色: 当天跳空高开2个点以上 </p>')
        f.write('<p>-----------------------------------我是分割线-----------------------------------</p>\n')
        f.write('\n')

    pass


def peach_continue_handle_html_end_special(newfile, dict_industry):
    with open(newfile,'a') as f:
        f.write('\n')
        f.write('\n')
        f.write('\n')
        f.write('\n')
        f.write('<p>-----------------------------------我是分割线-----------------------------------</p>\n')
        f.write('\n')
        f.write('<p>industry %s</p>\n' % (sorted(dict_industry.items(),key=lambda x:x[1],reverse=True)))
        f.write('\n')
        f.write('<p>-----------------------------------我是分割线-----------------------------------</p>\n')

    pass
    
def is_exist_quad(stock_name, days=30): 
   
    if days < 0:
       return False

    tmp_date=nowdate-datetime.timedelta(int(days))

    tmp_day=tmp_date.strftime("%Y-%m-%d")
    quad_day_w = tmp_date.strftime("%Y-%m-%d-%w")
    quad_dir=quad_day_w+'-quad'

    #curr_path=cur_file_dir()#获取当前.py脚本文件的文件路径
    curr_path='/var/www/html'
    stock_data_path= curr_path + '/' + stock_data_dir 
    quad_path=stock_data_path + '/' + quad_dir
    quad_files=getAllFiles(quad_path)

    if debug:
    #if False:
        #print('quad_path = %s' % quad_path)
        #print('quad_files = %s' % quad_files)
        pass

    peach_quad_file=[f for f in quad_files if stock_name in f]
    
    if len(peach_quad_file) > 0:
        return True
    else:
        return  is_exist_quad(stock_name, days -1)

     
    
def showImageInHTML(imageTypes,savedir):
    curr_path= savedir+'/' + curr_dir
    files=getAllFiles(curr_path)
    if debug:
        print('')
        print('curr_path:%s' % curr_path)
        print("all file:%s" % (files))

    images=[f for f in files if f[f.rfind('.')+1:] in imageTypes]
    if debug:
        print('')
        print("png jpg gif file %s"%(images))

    '''
    #delete size judge
    images=[item for item in images if os.path.getsize(item)>5*1024]
    if debug:
        print('')
        print("size > 5 * 1024: %s"%(images))
    '''

    #images=[curr_dir+item[item.rfind('/'):] for item in images]
    images=[item[item.rfind('/')+1:] for item in images]
    if debug:
        print('')
        print("png name: %s"%(images))

    print('')
    newfile='%s/%s'%(savedir, curr_dir + '/' + curr_dir + '.html')
    newfile_2='%s/%s'%(savedir, curr_dir + '/' + curr_dir + '-new.html')
    if debug:
        print("%s"% newfile)
        print("%s"% newfile_2)
    
    #get continuous stock_code
    last_day = get_valid_last_day(lastdate)

    if debug:
        print("last_day:%s, curr_day:%s curr_dir:%s" % (last_day, curr_day, curr_dir))
    
    real_dir = savedir + '/' + curr_dir 
    if debug:
        print('real_dir:%s'% real_dir)

    if os.path.exists(real_dir) == False:
        print("%s not exist!!! return" % (real_dir ))
        return
    
    if debug:
        print('real_dir:%s'% real_dir)
        print('images:%s'% images)
        print('curr_day:%s'% curr_day)
        print('dict_industry:%s'% dict_industry)

    
    quad_peach_list = []
    for image in images:
        stock_name=image[13:19]
        if is_exist_quad(stock_name, 30): 
            #satisfy quad condition
            quad_peach_list.append(image)
            print('peach_quad stock_name:%s' % stock_name)
        else:
            print('!!! none peach_quad stock_name:%s' % stock_name)
            
    print('quad_peach_list=%s' % quad_peach_list)
   
    ret_df = comm_generate_web_dataframe(real_dir, images, curr_day, dict_industry)

    comm_handle_html_head(newfile, stock_data_dir, curr_day )
    peach_continue_handle_html_body_special(newfile, curr_day)
    comm_handle_html_body(newfile, ret_df)
    peach_continue_handle_html_end_special(newfile, dict_industry)
    comm_handle_html_end(newfile, curr_dir)


    ret_df = comm_generate_web_dataframe(real_dir, quad_peach_list, curr_day, dict_industry)
    comm_handle_html_head(newfile_2, stock_data_dir, curr_day )
    peach_continue_handle_html_body_special(newfile_2, curr_day)
    comm_handle_html_body(newfile_2, ret_df)
    peach_continue_handle_html_end_special(newfile_2, dict_industry)
    comm_handle_html_end(newfile_2, curr_dir)
	
	
def getAllFiles(directory):
    files=[]
    for dirpath, dirnames,filenames in os.walk(directory):
        if filenames!=[]:
            for file in filenames:
                files.append(dirpath+'/'+file)
    files.sort(key=len)
    return files

#获取脚本文件的当前路径
def cur_file_dir():
    #获取脚本路径
    path = sys.path[0]
    #判断为脚本文件还是py2exe编译后的文件，如果是脚本文件，则返回的是脚本的目录，如果是py2exe编译后的文件，则返回的是编译后的文件路径
    if os.path.isdir(path):
        return path
    elif os.path.isfile(path):
        return os.path.dirname(path)

def get_today_item(today):
    df=hdata.get_data_from_hdata(start_date=today, end_date=today)
    # print(len(df))
    return df


def get_valid_last_day(nowdate):
    poll_flag = True
    i = 1
    item_number = 10
    #test
    #nowdate=nowdate-datetime.timedelta(1)
    stopdate=nowdate-datetime.timedelta(item_number) #get 7 item from hdata_day(db)
    stop_day=stopdate.strftime("%Y-%m-%d")
    curr_day=nowdate.strftime("%Y-%m-%d")
    if debug:
        print('stop_day=%s, curr_day=%s' % (stop_day, curr_day))
    start_day=curr_day
    last_day=curr_day
    df=hdata.get_data_from_hdata(start_date=stop_day, \
            end_date=curr_day, limit=item_number)
    if debug:
        print(df)
    while poll_flag:
        lastdate=nowdate-datetime.timedelta(i)
        i = i+1        
        last_day=lastdate.strftime("%Y-%m-%d")
        list_df = list(df['record_date'].apply(lambda x: str(x)))
        print("last_day:%s" % (last_day))
        print("list_df:%s i=%d"%(list_df, i))
        if last_day in list_df:
            poll_flag = False;
            break
    print("last_day:%s" % (last_day))
    return last_day
    
    
if __name__ == '__main__':
    '''
    nowdate=datetime.datetime.now().date()
    yesterday=nowdate-datetime.timedelta(1)
    
    today = nowdate.strftime("%Y-%m-%d")
    yesterday = yesterday.strftime("%Y-%m-%d")
    today_df=get_today_item(today)
    
    last_day = get_valid_last_day(nowdate)
    print('last_day:%s' %  (last_day))
    '''

    #savedir=cur_file_dir()#获取当前.py脚本文件的文件路径
    savedir='/var/www/html'
    savedir= savedir + '/' + stock_data_dir 
    showImageInHTML(('jpg','png','gif'), savedir)#浏览所有jpg,png,gif文件
