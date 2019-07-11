#!/usr/bin/env python
#coding:utf-8
import os,sys

import psycopg2 #使用的是PostgreSQL数据库
import tushare as ts
from HData_select import *
import  datetime

sdata=HData_select("usr","usr")

nowdate=datetime.datetime.now().date()
#nowdate=nowdate-datetime.timedelta(1)
src_dir=nowdate.strftime("%Y-%m-%d")

def showImageInHTML(imageTypes,savedir):
    files=getAllFiles(savedir+'/' + src_dir)
    #print("file:%s" % (files))
    images=[f for f in files if f[f.rfind('.')+1:] in imageTypes]
    #print("%s"%(images))
    images=[item for item in images if os.path.getsize(item)>5*1024]
    #print("%s"%(images))
    #images=[src_dir+item[item.rfind('/'):] for item in images]
    images=[item[item.rfind('/')+1:] for item in images]
    print("%s"%(images))
    newfile='%s/%s'%(savedir, src_dir + '/' + src_dir + '.html')
    with open(newfile,'w') as f:

        f.write('<!DOCTYPE html>\n')
        f.write('<html>\n')
        f.write('<head>\n')
        f.write('<meta http-equiv="Content-Type" content="text/html; charset=utf-8" />\n')
        f.write('<title> %s </title>\n'%(src_dir))
        f.write('<style>\n')
        f.write('.ShaShiDi{\n')
        f.write('width:500px;\n')
        f.write('height:400px;\n')
        f.write('display:flex;\n')
        f.write('        align-items:center;\n')
        f.write('        justify-content:center;\n')
        f.write('}\n')
        f.write('\n')
        f.write('.ShaShiDi img{\n')
        f.write('width:100%;\n')
        f.write('height:auto;\n')
        f.write('}\n')
        f.write('</style>\n')
        f.write('\n')
        f.write('\n')
        f.write('<style type="text/css">a {text-decoration: none}</style>\n')
        f.write('\n')

        f.write('</head>\n')
        f.write('<body>\n')
        f.write('    <meta http-equiv="Content-Type" content="text/html; charset=utf-8" />\n')
        f.write('\n')
        f.write('\n')
        f.write('\n')
        f.write('\n')
        f.write('\n')


        for image in images:
            '''
            f.write('<div class="ShaShiDi">\n')        
            f.write("<img src='%s'>\n"%image)
            f.write('</div>\n')
            '''

            #'2019-07-09-600095-哈高科-873-960-960-873-997.png' 
            tmp_image=image[0:image.rfind('.')]
            print('%s' % (tmp_image))
            
            stock_code=image[11:17]
            print('%s' % (stock_code))
            
            print('%s' % (stock_code[0:2]))
            if stock_code[0:2] == '60':
                stock_code='SH'+stock_code
            else:
                stock_code='SZ'+stock_code

            print('%s' % (stock_code))
            xueqiu_url='https://xueqiu.com/S/' + stock_code

            f.write('<p>\n')
            f.write('<a href="%s" target="_blank"> %s </a>' % (image, tmp_image))
            f.write('---->')
            f.write('<a href="%s" target="_blank">(%s) </a>\n' % (xueqiu_url , 'xueqiu:' + stock_code))
            f.write('</p>\n')
            
            
        f.write('\n')
        f.write('\n')
        f.write('\n')
        f.write('\n')
        f.write('\n')
        f.write('\n')
        f.write('</body>\n')
        f.write('</html>\n')
        f.write('\n')
    
	
    shell_cmd2='cp -rf ' + src_dir + ' /var/www/html/'
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
    
    df=sdata.my2_get_all_hdata_of_stock()
    # print("today:%s: %s" % (today, df.head(100)))
    df = df[df.record_date==today]
    # print("%s" % (df.head(10)))

    
    
    return df

        
if __name__ == '__main__':
    nowdate=datetime.datetime.now().date()
    nowdate=nowdate-datetime.timedelta(1)
    
    today = nowdate.strftime("%Y-%m-%d")
    df=get_today_item(today)


    savedir=cur_file_dir()#获取当前.py脚本文件的文件路径
    showImageInHTML(('jpg','png','gif'), savedir)#浏览所有jpg,png,gif文件
