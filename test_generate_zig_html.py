#!/usr/bin/env python
#coding:utf-8
import os,sys

import psycopg2 #使用的是PostgreSQL数据库
import tushare as ts
from HData_select import *
from HData_day import *
from HData_hsgt import *
import  datetime

#funcat
from funcat import *
from funcat.data.aaron_backend import AaronDataBackend
set_data_backend(AaronDataBackend())

hdata=HData_day("usr","usr")
sdata=HData_select("usr","usr")
hsgtdata=HData_hsgt("usr","usr")

daily_df=ts.get_stock_basics()
dict_industry={}


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
curr_dir=curr_day_w+'-zig'
def insert_industry(dict_name, key):
    if dict_name.get(key) is None :
       dict_name.setdefault(key, 1)
    else:
        dict_name[key]=dict_name[key] + 1



def generate_head_html(file_f, day):
    f = file_f
    curr_day = day
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
    '''
    #f.write('<p>蓝色：连续两天涨幅3个点以上   红色:连续三天涨幅3个点以上    绿色: 连续两天涨幅3个点以上，并且当天跳空高开2个点以上 /p>\n')
    f.write('<p  style="color:blue;">蓝色: 连续两天涨幅3个点以上   </p>')
    f.write('<p  style="color:red;">红色: 连续三天涨幅3个点以上   </p>')
    f.write('<p  style="color:green;">绿色: 连续两天涨幅3个点以上，并且当天跳空高开2个点以上 </p>')
    '''
    f.write('<p  style="color:green;">绿色: 当天跳空高开2个点以上 </p>')
    f.write('<p>-----------------------------------我是分割线-----------------------------------</p>\n')
    f.write('\n')


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

        generate_head_html(f, curr_day)

        for image in images:

            #'2019-07-09-600095-哈高科-873-960-960-873-997.png' 
            tmp_image=image[0:image.rfind('.')]
            print('%s' % (tmp_image))
            
            stock_code=image[13:19]

            #funcat call
            T(curr_day)
            S(stock_code)
            open_p = ((O - REF(C, 1))/REF(C, 1)) 
            open_p = round (open_p.value, 4)
            open_jump=open_p - 0.02
            print(str(nowdate), stock_code, O, H, L, C, open_p)


            print('%s' % (stock_code))
            
            print('%s' % (stock_code[0:2]))
            if stock_code[0:1] == '6':
                stock_code_new='SH'+stock_code
            else:
                stock_code_new='SZ'+stock_code

            print('%s' % (stock_code_new))
            xueqiu_url='https://xueqiu.com/S/' + stock_code_new
            hsgt_url='../../cgi-bin/hsgt-search.cgi?name=' + stock_code
            hsgt_df = hsgtdata.get_all_hdata_of_stock_code(stock_code)

            f.write('<p>\n')
            if open_jump > 0 :
                f.write('<a href="%s" target="_blank" style="color: #32CD32"> %s </a>' % (image, tmp_image))
            else:
                f.write('<a href="%s" target="_blank"> %s </a>' % (image, tmp_image))
            f.write('---->')
            f.write('<a href="%s" target="_blank"> %s </a>\n' % (xueqiu_url , 'xueqiu:' + stock_code_new))
            if (len(hsgt_df) > 0):
                f.write('---->')
                f.write('<a href="%s" target="_blank"> %s</a>\n'%(hsgt_url, 'hsgt:' + stock_code_new))
            industry_name = daily_df.loc[stock_code]['industry']
            f.write('[%s]' % (industry_name))
            f.write('</p>\n')
            insert_industry(dict_industry, industry_name)
            
            
        f.write('\n')
        f.write('\n')
        f.write('\n')
        f.write('\n')
        f.write('\n')
        f.write('\n')
        f.write('<p>-----------------------------------我是分割线-----------------------------------</p>\n')
        f.write('\n')
        f.write('<p>industry %s</p>\n' % (sorted(dict_industry.items(),key=lambda x:x[1],reverse=True)))
        f.write('\n')
        f.write('<p>-----------------------------------我是分割线-----------------------------------</p>\n')
        f.write('</body>\n')
        f.write('</html>\n')
        f.write('\n')
    
    
    shell_cmd2='cp -rf ' + stock_data_dir + '/' + curr_dir + ' /var/www/html/'+stock_data_dir+'/'
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
    item_number = 10
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
    savedir= savedir + '/' + stock_data_dir 
    showImageInHTML(('jpg','png','gif'), savedir)#浏览所有jpg,png,gif文件