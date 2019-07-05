#!/usr/bin/env python
#coding:utf-8
import os,sys

def showImageInHTML(imageTypes,savedir):
    files=getAllFiles(savedir+'/pic')
    images=[f for f in files if f[f.rfind('.')+1:] in imageTypes]
    images=[item for item in images if os.path.getsize(item)>5*1024]
    images=['pic'+item[item.rfind('/'):] for item in images]
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
     
if __name__ == '__main__':
    savedir=cur_file_dir()#获取当前.py脚本文件的文件路径
    showImageInHTML(('jpg','png','gif'), savedir)#浏览所有jpg,png,gif文件
