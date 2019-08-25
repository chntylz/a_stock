#!/usr/bin/env python
#coding:utf-8

import time
import datetime

import os

#把时间戳转化为时间: 1479264792 to 2016-11-16 10:53:12
def TimeStampToTime(timestamp):
    timeStruct = time.localtime(timestamp)
    return time.strftime('%Y-%m-%d %H:%M:%S',timeStruct)



#获取文件的大小,结果保留两位小数，单位为Byte
def get_FileSize(filePath):
    fsize = os.path.getsize(filePath)
    fsize = fsize/float(1024)
    return round(fsize,2)


#获取文件的访问时间
def get_FileAccessTime(filePath):
    t = os.path.getatime(filePath)
    return TimeStampToTime(t)


#获取文件的创建时间
def get_FileCreateTime(filePath):
    t = os.path.getctime(filePath)
    return TimeStampToTime(t)


#获取文件的修改时间
def get_FileModifyTime(filePath):
    t = os.path.getmtime(filePath)
    return TimeStampToTime(t)