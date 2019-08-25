#!/usr/bin/env python
#coding:utf-8
import os,sys,gzip
import json


from file_interface import *



debug=False

#file_path='/home/ubuntu/tmp/a_stock/hkexnews_scrapy/hkexnews_scrapy/json/20190823.json.gz'


def getAllFiles(directory):
    files=[]
    for dirpath, dirnames,filenames in os.walk(directory):
        if filenames!=[]:
            for file in filenames:
                files.append(dirpath+'/'+file)
    #files.sort(key=len)
    files.sort()
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




def hsgt_get_day_item_from_json(file_path):
    line_count=len(gzip.open(file_path).readlines())
    line_num=0
    for line in gzip.open(file_path):
        line_num = line_num + 1   
        if line_num == 1 or line_num == line_count:
            continue
        if debug:
            print("line_num:%d, %s"%(line_num, line))
        
        #{"date": "2019-08-23", "stock_ename": "SHENZHEN MINDRAY BIO-MEDICAL ELECTRONICS CO., LTD. (A #300760)", "code": "77760", "share_holding": "19601172", "percent": "1.61%"}
        line=str(line)
        begin=line.rfind('{')
        end=line.rfind('}')
        s=line[begin:end+1]
        s=s.replace('\'', '-')
        s=s.replace('\\', '')
        s=s.replace('XI"AN', 'XI-AN')

        if debug:
            print('s---->%s'%(s))
        d=json.loads(s, strict=False)
        line=d

        #get date
        shgt_date=line['date']

        #get stock_ename
        shgt_ename=line['stock_ename']
        position=shgt_ename.rfind('#')
        shgt_code=shgt_ename[position+1: -1]


        #get share_holding
        shgt_holding=line['share_holding']
        
        #get percent
        shgt_percent=line['percent']
        position=shgt_ename.rfind('%')
        shgt_percent=shgt_percent[:position]

        print("line_num:%d, shgt_date:%s, shgt_code:%s, shgt_holding:%s, shgt_percent:%s, shgt_ename:%s"% \
             (line_num, shgt_date, shgt_code, shgt_holding, shgt_percent, shgt_ename))



def hsgt_get_all_data():
    curr_dir=cur_file_dir()#获取当前.py脚本文件的文件路径
    json_dir=curr_dir+'/hkexnews_scrapy/hkexnews_scrapy/json'
    all_files=getAllFiles(json_dir)
    if debug:
        print(all_file)

    for tmp_file in all_files:
        if debug:
            print(tmp_file)
        file_size=get_FileSize(tmp_file)
        if debug:
            print(file_size)
       
        if file_size < 1 :
            print(tmp_file)
            continue

        hsgt_get_day_item_from_json(tmp_file)


hsgt_get_all_data()
