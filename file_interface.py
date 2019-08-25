#!/usr/bin/env python
#coding:utf-8

import time
import datetime

import os

#��ʱ���ת��Ϊʱ��: 1479264792 to 2016-11-16 10:53:12
def TimeStampToTime(timestamp):
    timeStruct = time.localtime(timestamp)
    return time.strftime('%Y-%m-%d %H:%M:%S',timeStruct)



#��ȡ�ļ��Ĵ�С,���������λС������λΪByte
def get_FileSize(filePath):
    fsize = os.path.getsize(filePath)
    fsize = fsize/float(1024)
    return round(fsize,2)


#��ȡ�ļ��ķ���ʱ��
def get_FileAccessTime(filePath):
    t = os.path.getatime(filePath)
    return TimeStampToTime(t)


#��ȡ�ļ��Ĵ���ʱ��
def get_FileCreateTime(filePath):
    t = os.path.getctime(filePath)
    return TimeStampToTime(t)


#��ȡ�ļ����޸�ʱ��
def get_FileModifyTime(filePath):
    t = os.path.getmtime(filePath)
    return TimeStampToTime(t)