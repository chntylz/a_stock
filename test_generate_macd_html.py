#!/usr/bin/env python
#coding:utf-8
import os,sys

import psycopg2 #使用的是PostgreSQL数据库
import tushare as ts
from HData_select import *
from HData_day import *
import  datetime

hdata=HData_day("usr","usr")
sdata=HData_select("usr","usr")

nowdate=datetime.datetime.now().date()
#test
#nowdate=nowdate-datetime.timedelta(1)
lastdate=nowdate-datetime.timedelta(1)

curr_day=nowdate.strftime("%Y-%m-%d")
last_day=lastdate.strftime("%Y-%m-%d")
curr_dir=curr_day+'-macd'

def showImageInHTML(imageTypes,savedir):
    files=getAllFiles(savedir+'/' + curr_dir)
    #print("file:%s" % (files))
    images=[f for f in files if f[f.rfind('.')+1:] in imageTypes]
    #print("%s"%(images))
    images=[item for item in images if os.path.getsize(item)>5*1024]
    #print("%s"%(images))
    #images=[curr_dir+item[item.rfind('/'):] for item in images]
    images=[item[item.rfind('/')+1:] for item in images]
    print("%s"%(images))
    newfile='%s/%s'%(savedir, curr_dir + '/' + curr_dir + '.html')
    
    #get continuous stock_code
    last_day = get_valid_last_day(nowdate)
    print("last_day:%s, curr_day:%s curr_dir:%s" % (last_day, curr_day, curr_dir))
    
    if os.path.exists(savedir + '/' + curr_dir ) == False:
        print("%s not exist!!! return" % (savedir + '/' + curr_dir ))
        return
    
    with open(newfile,'w') as f:

        f.write('<!DOCTYPE html>\n')
        f.write('<html>\n')
        f.write('<head>\n')
        f.write('<meta http-equiv="Content-Type" content="text/html; charset=utf-8" />\n')
        f.write('<title> %s </title>\n'%(curr_day))
        f.write('\n')
        f.write('\n')
        f.write('<style type="text/css">a {text-decoration: none}</style>\n')
        f.write('\n')

        f.write('</head>\n')
        f.write('<body>\n')
        f.write('\n')
        f.write('\n')
        f.write('\n')
        f.write('\n')
        f.write('<p> 日期 %s </p>\n' %(curr_day))

        df = get_today_item(curr_day)

        # 找出上涨的股票
        df_up = df[df['p_change'] > 0.00]
        # 走平股数
        df_even = df[df['p_change'] == 0.00]
        # 找出下跌的股票
        df_down = df[df['p_change'] < 0.00]

        # 找出涨停的股票
        limit_up = df[df['p_change'] >= 9.70]
        limit_down = df[df['p_change'] <= -9.70]

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
        f.write('\n')


        for image in images:

            #'2019-07-09-600095-哈高科-873-960-960-873-997.png' 
            tmp_image=image[0:image.rfind('.')]
            print('%s' % (tmp_image))
            
            stock_code=image[11:17]
            print('%s' % (stock_code))
            
            print('%s' % (stock_code[0:2]))
            if stock_code[0:2] == '60':
                stock_code_new='SH'+stock_code
            else:
                stock_code_new='SZ'+stock_code

            print('%s' % (stock_code_new))
            xueqiu_url='https://xueqiu.com/S/' + stock_code_new

            f.write('<p>\n')
            f.write('<a href="%s" target="_blank"> %s </a>' % (image, tmp_image))
            f.write('---->')
            f.write('<a href="%s" target="_blank">(%s) </a>\n' % (xueqiu_url , 'xueqiu:' + stock_code_new))
            f.write('</p>\n')
            
            
        f.write('\n')
        f.write('\n')
        f.write('\n')
        f.write('\n')
        f.write('\n')
        f.write('\n')
        f.write('<p>-----------------------------------我是分割线-----------------------------------</p>\n')
        f.write('</body>\n')
        f.write('</html>\n')
        f.write('\n')
    
    
    shell_cmd2='cp -rf ' + curr_dir + ' /var/www/html/'
    os.system(shell_cmd2)
    print ('success,images are wrapped up in %s' % (newfile))

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
    df=hdata.get_day_hdata_of_stock(today)
    # print(len(df))
    return df


def get_valid_last_day(nowdate):
    poll_flag = True
    i = 1
    item_number = 7
    #test
    #nowdate=nowdate-datetime.timedelta(1)
    stopdate=nowdate-datetime.timedelta(item_number) #get 7 item from hdata_day(db)
    stop_day=stopdate.strftime("%Y-%m-%d")
    curr_day=nowdate.strftime("%Y-%m-%d")
    start_day=curr_day
    last_day=curr_day
    df=hdata.my2_get_valid_last_day_hdata_of_stock(stop_day, curr_day, item_number)
    while poll_flag:
        lastdate=nowdate-datetime.timedelta(i)
        i = i+1        
        last_day=lastdate.strftime("%Y-%m-%d")
        list_df = list(df['record_date'].apply(lambda x: str(x)))
        print("list_df:%s"%(list_df))
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

    savedir=cur_file_dir()#获取当前.py脚本文件的文件路径
    showImageInHTML(('jpg','png','gif'), savedir)#浏览所有jpg,png,gif文件
