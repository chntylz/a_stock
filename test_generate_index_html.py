#!/usr/bin/env python
#coding:utf-8
import os,sys
import datetime


nowdate=datetime.datetime.now().date()
nowdate=nowdate-datetime.timedelta(1)
src_dir=nowdate.strftime("%Y-%m-%d")
target_html='index.html'

def showImageInHTML(imageTypes,savedir):
    files=getAllFiles(savedir)
    print("p0 :%s" % (files))
    images=[f for f in files if f[f.rfind('.')+1:] in imageTypes]
    print("p1 :%s"%(images))
    images=[item[item.rfind('/'):] for item in images]
    print("p3 :%s"%(images))
    newfile='%s/%s'%(savedir, target_html)
    with open(newfile,'w') as f:

        f.write('<!DOCTYPE html>\n')
        f.write('<html>\n')
        f.write('<head>\n')
        f.write('<meta http-equiv="Content-Type" content="text/html; charset=utf-8" />\n')
        f.write('<title> %s </title>\n'%(target_html))
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
            tmp_image=image[image.rfind('/')+1:]
            tmp_image=tmp_image[tmp_image.rfind('.')-100:-5]
            
            '''
            if tmp_image not in ['index', 'zheli']:
                print("%s" %(tmp_image))
                image = tmp_image + image
            '''
            if tmp_image in ['index', 'zheli']:
                continue

            print("%s" % (tmp_image))
            image = tmp_image + image

            f.write('<p>\n')
            f.write('<a href="%s"> %s </a>\n' % (image, tmp_image))
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
    
    
    #shell_cmd='cp -rf ' + newfile + ' /var/www/html/'
    #os.system(shell_cmd)

    #shell_cmd2='cp -rf ' + src_dir + ' /var/www/html/'
    #os.system(shell_cmd2)
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
    showImageInHTML(('html'), savedir)#浏览所有jpg,png,gif文件
