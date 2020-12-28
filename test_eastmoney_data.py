
import requests

# 原文链接：https://blog.csdn.net/binosun/article/details/78697332

s_date='2020-11-10'
e_date='2020-11-11'
#url='http://datainterface3.eastmoney.com/EM_DataCenter_V3/api/GDZC/GetGDZC?tkn=eastmoney&cfg=gdzc&secucode=&fx=1&sharehdname=&pageSize=50&pageNum=1&sortFields=BDJZ&sortDirec=1&startDate=2017-11-29&endDate=2017-11-30'
url='http://datainterface3.eastmoney.com/EM_DataCenter_V3/api/GDZC/GetGDZC?tkn=eastmoney&cfg=gdzc&secucode=&fx=1&sharehdname=&pageSize=50&pageNum=1&sortFields=BDJZ&sortDirec=1&startDate='+ s_date +'&endDate='+e_date

#url='http://push2.eastmoney.com/api/qt/clist/get?pn=3&pz=50&po=1&np=1&ut=b2884a393a59ad64002292a3e90d46a5&fltt=2&invt=2&fid0=f4001&fid=f62&fs=m:0+t:6+f:!2,m:0+t:13+f:!2,m:0+t:80+f:!2,m:1+t:2+f:!2,m:1+t:23+f:!2,m:0+t:7+f:!2,m:1+t:3+f:!2&stat=1&fields=f12,f14,f2,f3,f62,f184,f66,f69,f72,f75,f78,f81,f84,f87,f204,f205,f124&rt=53504896&cb=jQuery18307123164692561571_1605146852328&_=1605146898252'

def get_data_from_eastmoney(url):
    print(url)
    response=requests.get(url)
    print(response.text)

    # dict = {'a': 1, 'b': 2, 'b': '3'}
    # print(dict['a'])
    items={}
    items=response.text
    print(type(items))
    items=eval(items)
    print(items["Message"])
    print(items["Status"])
    print(items["Data"])
    # for i in range(len(items)):
        # print(items(i))

    item=items["Data"][0]
    # print(item["Data"])
    for i in item["Data"]:
        print('*************************************************************')
        # print(i)
        # print('--------------')
        i=i.split('|')
        print(i)



    dict_0 = items['Data'][0]
    column=dict_0['FieldName'].split(',')
    print(len(column))
    data=dict_0['Data']
    for i in data:
        
        tmp=i.split('|')
        print(len(tmp))
        print(tmp)
        print('-------------------------------------------------------------')
    

get_data_from_eastmoney(url)

