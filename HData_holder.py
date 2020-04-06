#!/usr/bin/env python  
# -*- coding: utf-8 -*-

'''
https://tushare.pro/document/2?doc_id=166

输入参数

名称    类型    必选    描述
ts_code str N   TS股票代码
enddate str N   截止日期
start_date  str N   公告开始日期
end_date    str N   公告结束日期


输出参数

名称    类型    默认显示    描述
ts_code str Y   TS股票代码
ann_date    str Y   公告日期
end_date    str Y   截止日期
holder_num  int Y   股东户数


接口使用


pro = ts.pro_api()

    df = pro.stk_holdernumber(ts_code='300199.SZ', start_date='20160101', end_date='20181231')


    数据示例

          ts_code  ann_date  end_date  holder_num
          0   300199.SZ  20181025  20180930       25135
          1   300199.SZ  20180808  20180630       25785
          2   300199.SZ  20180426  20180331       23384
          3   300199.SZ  20180316  20180228       23490
          4   300199.SZ  20180316  20171231       24086
          5   300199.SZ  20171026  20170930       24121
          6   300199.SZ  20170817  20170630       26271
          7   300199.SZ  20170427  20170331       24531
          8   300199.SZ  20170427  20161231       22972
          9   300199.SZ  20161028  20161027       19787
          10  300199.SZ  20161027  20160930       19787
          11  300199.SZ  20160804  20160630       20050
          12  300199.SZ  20160428  20160331       23367

'''

import  psycopg2
import tushare as ts
import pandas as pd
from time import clock

class HData_holder(object):
    def __init__(self,user,password):
        # self.aaa = aaa
        self.hdata_holder=[]
        self.user=user
        self.password=password

        self.conn=None
        self.cur=None


    def db_connect(self):
        self.conn = psycopg2.connect(database="usr", user=self.user, password=self.password, host="127.0.0.1",
                                port="5432")
        self.cur = self.conn.cursor()

    def db_disconnect(self):

        self.conn.close()

    def db_hdata_date_create(self):
        conn = psycopg2.connect(database="usr", user=self.user, password=self.password, host="127.0.0.1",
                                port="5432")
        cur = conn.cursor()
        # 创建stocks表
        cur.execute('''
                drop table if exists hdata_holder;
                create table hdata_holder(
                    record_date     date,
                    stock_code      varchar,
                    ann_date        date,
                    end_date        date,
                    holder_num      float
                    );
                alter table hdata_holder add primary key(stock_code,record_date);
                ''')
        conn.commit()
        conn.close()
        print("db_hdata_holder_create finish")
        pass




    def db_get_maxdate_of_stock(self,stock_code):#获取某支股票的最晚日期
        self.cur.execute("select max(ann_date) from hdata_holder where stock_code="+"\'"+stock_code+"\'"+";")
        ans=self.cur.fetchall()
        if(len(ans)==0):
            return None
        return ans[0][0]
        self.conn.commit()
        pass

    def insert_perstock_hdatadate(self,stock_code,data):#插入一支股票的所有历史数据到数据库#如果有code和index相同的不重复插入
        t1=clock()

        #print(stock_code+" insert_perstock_hdatadate begin")
        if data is None:
            print("None")
        else:
            for i in range(0,len(data)):
                # print (i)
                str_temp=""

                #str_temp+="\'"+stock_code+"\'"+","
                #str_temp+="\'"+data.index[i]+"\'"
                str_temp+="\'"+data.index[i].strftime("%Y-%m-%d")+"\'"



                for j in range(0,data.shape[1]):
                    str_temp+=","+"\'"+str(data.iloc[i,j])+"\'"

                sql_temp="values"+"("+str_temp+")"
                #print(sql_temp)
                self.cur.execute("insert into hdata_holder "+sql_temp+";")
            self.conn.commit()

        #print(clock()-t1)

        #print(stock_code+" insert_perstock_hdatadate finish")


    def insert_allstock_hdatadate(self, data):#插入一支股票的所有历史数据到数据库#如果有code和index相同的不重复插入

        data_format = " record_date, stock_code, ann_date, end_date, holder_num"

        t1=clock()

        #print(stock_code+" insert_perstock_hdatadate begin")
        if data is None:
            print("None")
        else:
            length = len(data)
            sql_cmd = ""
            each_num = 1000
            for i in range(0,length):
                # print (i)

                str_temp=""
                #str_temp+="\'"+stock_code+"\'"+","
                #str_temp+="\'"+data.index[i]+"\'"
                str_temp+="\'"+data.index[i].strftime("%Y-%m-%d")+"\'"

                for j in range(0,data.shape[1]):
                    str_temp+=","+"\'"+str(data.iloc[i,j])+"\'"

                sql_cmd= sql_cmd + "("+str_temp+")"
                if i % each_num == 0 or i == (length -1):
                    pass
                else:
                    sql_cmd = sql_cmd+","

                if i % each_num == 0:
                    #print(sql_cmd)
                    if(sql_cmd != ""):
                        self.cur.execute("insert into hdata_holder (" + data_format + " ) values " + sql_cmd + ";")
                        self.conn.commit()
                        sql_cmd = ""

            #print(sql_cmd)
            if(sql_cmd != ""):
                self.cur.execute("insert into hdata_holder (" + data_format + " ) values " + sql_cmd + ";")
                self.conn.commit()

        #print(clock()-t1)

        #print(stock_code+" insert_perstock_hdatadate finish")


    def get_all_hdata_of_stock(self,stock_code):#将数据库中的数据读取并转为dataframe格式返回
        conn = psycopg2.connect(database="usr", user=self.user, password=self.password, host="127.0.0.1",
                                port="5432")
        cur = conn.cursor()

        #sql_temp="select * from (select * from hdata_holder where stock_code='000922' order by record_date desc LIMIT 5) as tbl order by record_date asc;"
        #sql_temp="select * from (select * from hdata_holder where stock_code="+"\'"+stock_code+"\' order by record_date desc LIMIT 100) as tbl order by record_date asc;"
        sql_temp="select * from hdata_holder where stock_code="+"\'"+stock_code+"\';"
        cur.execute(sql_temp)
        rows = cur.fetchall()

        conn.commit()
        conn.close()

        dataframe_cols=[tuple[0] for tuple in cur.description]#列名和数据库列一致
        df = pd.DataFrame(rows, columns=dataframe_cols)
        index =  df["record_date"]
        df = pd.DataFrame(rows, index=index, columns=dataframe_cols)
        
        return df
        pass
        
    def get_limit_hdata_of_stock(self,stock_code,end_day, limit_number):#将数据库中的数据读取并转为dataframe格式返回
        conn = psycopg2.connect(database="usr", user=self.user, password=self.password, host="127.0.0.1",
                                port="5432")
        cur = conn.cursor()

        #sql_temp="select * from (select * from hdata_holder where stock_code='000922' order by record_date desc LIMIT 5) as tbl order by record_date asc;"
        sql_temp="select * from (select * from hdata_holder where stock_code="+"\'"+stock_code+"\'  and record_date <= "+"\'"+ end_day +"\'  order by record_date desc LIMIT "+"\'"+str(limit_number)+"\' ) as tbl order by record_date asc;"
        #sql_temp="select * from hdata_holder where stock_code="+"\'"+stock_code+"\';"
        cur.execute(sql_temp)
        rows = cur.fetchall()

        conn.commit()
        conn.close()

        dataframe_cols=[tuple[0] for tuple in cur.description]#列名和数据库列一致
        df = pd.DataFrame(rows, columns=dataframe_cols)
        index =  df["record_date"]
        df = pd.DataFrame(rows, index=index, columns=dataframe_cols)
        
        return df
        pass
 
    def get_day_hdata_of_stock(self, day):#将数据库中的数据读取并转为dataframe格式返回
        conn = psycopg2.connect(database="usr", user=self.user, password=self.password, host="127.0.0.1",
                                port="5432")
        cur = conn.cursor()
        #select * from (select * from hdata_holder where stock_code='000922' order by record_date desc LIMIT 5) as tbl order by record_date asc;
        sql_temp="select * from hdata_holder where record_date = "+"\'"+day+"\';"
        cur.execute(sql_temp)
        rows = cur.fetchall()

        conn.commit()
        conn.close()

        dataframe_cols=[tuple[0] for tuple in cur.description]#列名和数据库列一致
        df = pd.DataFrame(rows, columns=dataframe_cols)
        index =  df["record_date"]
        df = pd.DataFrame(rows, index=index, columns=dataframe_cols)

        return df
        pass
 
       
    def my2_get_all_hdata_of_stock(self):#将数据库中的数据读取并转为dataframe格式返回
        conn = psycopg2.connect(database="usr", user=self.user, password=self.password, host="127.0.0.1",
                                port="5432")
        cur = conn.cursor()
        #select * from (select * from hdata_holder where stock_code='000922' order by record_date desc LIMIT 5) as tbl order by record_date asc;
        sql_temp="select * from hdata_holder;"
        cur.execute(sql_temp)
        rows = cur.fetchall()

        conn.commit()
        conn.close()

        dataframe_cols=[tuple[0] for tuple in cur.description]#列名和数据库列一致
        df = pd.DataFrame(rows, columns=dataframe_cols)
        index =  df["record_date"]
        df = pd.DataFrame(rows, index=index, columns=dataframe_cols)
        
        return df
        pass
        
    def my2_get_valid_last_day_hdata_of_stock(self, stop_day, curr_day, number):#将数据库中的数据读取并转为dataframe格式返回
        conn = psycopg2.connect(database="usr", user=self.user, password=self.password, host="127.0.0.1",
                                port="5432")
        cur = conn.cursor()
        #select * from (select * from hdata_holder where stock_code='000922' order by record_date desc LIMIT 5) as tbl order by record_date asc;
        sql_temp="select record_date from hdata_holder where record_date between " + "\'" + stop_day + "\'  and  " + "\'" + curr_day + "\'  group by record_date order by record_date desc limit " + "\'" + str(number) + "\' ;"
        cur.execute(sql_temp)
        rows = cur.fetchall()

        conn.commit()
        conn.close()

        dataframe_cols=[tuple[0] for tuple in cur.description]#列名和数据库列一致
        df = pd.DataFrame(rows, columns=dataframe_cols)
    
        return df
        pass
        
    def get_data_accord_code_and_date(self, stock_code, record_date):#将数据库中的数据读取并转为dataframe格式返回
        conn = psycopg2.connect(database="usr", user=self.user, password=self.password, host="127.0.0.1",
                                port="5432")
        cur = conn.cursor()
        sql_temp="select * from hdata_holder where stock_code = " + "\'" + stock_code + "\'  and record_date = " + "\'" + record_date + "\' ;"
        cur.execute(sql_temp)
        rows = cur.fetchall()

        conn.commit()
        conn.close()

        dataframe_cols=[tuple[0] for tuple in cur.description]#列名和数据库列一致
        df = pd.DataFrame(rows, columns=dataframe_cols)
    
        return df
        pass
        


    def delete_data_of_day_stock(self, record_date):
        conn = psycopg2.connect(database="usr", user=self.user, password=self.password, host="127.0.0.1",
                                port="5432")
        cur = conn.cursor()
        sql_temp="delete from hdata_holder where record_date = " + "\'" + record_date + "\' ;"
        cur.execute(sql_temp)

        conn.commit()
        conn.close()

        pass
 
    #fix bug: delete zero when the stock is closed
    def delete_amount_is_zero(self):
        conn = psycopg2.connect(database="usr", user=self.user, password=self.password, host="127.0.0.1",
                                port="5432")
        cur = conn.cursor()
        sql_temp="delete from hdata_holder where amount = 0;"
        cur.execute(sql_temp)

        conn.commit()
        conn.close()

        pass
 
