#!/usr/bin/env python
#coding:utf-8
import os,sys

import psycopg2 #使用的是PostgreSQL数据库
import tushare as ts
from HData_select import *
import  datetime

sdata=HData_select("usr","usr")

def showImageInHTML(imageTypes,savedir):
    files=getAllFiles(savedir+'/pic')
    #print("file:%s" % (files))
    images=[f for f in files if f[f.rfind('.')+1:] in imageTypes]
    #print("%s"%(images))
    images=[item for item in images if os.path.getsize(item)>5*1024]
    #print("%s"%(images))
    images=['pic'+item[item.rfind('/'):] for item in images]
    print("%s"%(images))
    newfile='%s/%s'%(savedir,'images.html')
    with open(newfile,'w') as f:
        f.write('<div>')
        for image in images:
            f.write("<img src='%s'>\n"%image)
        f.write('</div>')
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
    print("today:%s: %s" % (today, df.head(100)))
    df = df[df.record_date==today]
    print("%s" % (df.head(10)))

    
    
    return df

        
if __name__ == '__main__':
    nowdate=datetime.datetime.now().date()
    nowdate=nowdate-datetime.timedelta(1)
    
    today = nowdate.strftime("%Y-%m-%d")
    df=get_today_item(today)


    savedir=cur_file_dir()#获取当前.py脚本文件的文件路径
    showImageInHTML(('jpg','png','gif'), savedir)#浏览所有jpg,png,gif文件