#!/usr/bin/env python
#coding:utf-8
#https://www.cnblogs.com/shaosks/p/5614630.html

import time
import datetime

import os,sys,os.path
from sys import argv
import shutil

#把时间戳转化为时间: 1479264792 to 2016-11-16 10:53:12
def get_date_from_timestamp(timestamp):
    d = datetime.datetime.fromtimestamp(timestamp / 1000, None)  # 时间戳转换成字符串日期时间
    my_date = d.strftime("%Y-%m-%d %H:%M:%S.%f")
    my_date = my_date[:10]
    return my_date

def get_snowball_timestamp():
    timestamp = time.time() * 1000
    return timestamp 
