#!/usr/bin/env python  
# -*- coding: utf-8 -*-

import  psycopg2
import tushare as ts
import pandas as pd
from time import clock

class HData_hsgt(object):
    def __init__(self,user,password):
        # self.aaa = aaa
        self.hdata_hstg_table=[]
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
                drop table if exists hdata_hsgt_table;
                create table hdata_hsgt_table(
                    record_date date,
                    stock_code varchar,  
                    share_holding float,
                    percent float
                    );
                alter table hdata_hsgt_table add primary key(stock_code,record_date);
                ''')
        conn.commit()
        conn.close()
        print("db_hdata_hsgt_table_create finish")
        pass

    def db_get_maxdate_of_stock(self,stock_code):#获取某支股票的最晚日期
        self.cur.execute("select max(record_date) from hdata_hsgt_table where stock_code="+"\'"+stock_code+"\'"+";")
        ans=self.cur.fetchall()
        if(len(ans)==0):
            return None
        return ans[0][0]
        self.conn.commit()
        pass

    def insert_perstock_hdatadate(self,  data):#插入一支股票的所有历史数据到数据库#如果有code和index相同的不重复插入
        t1=clock()

        #print(" insert_perstock_hdatadate begin")
        if data is None:
            print("None")
        else:
            for i in range(0,len(data)):
                # print (i)
                str_temp=""

                str_temp+="\'"+data.index[i].strftime("%Y-%m-%d")+"\'"



                for j in range(0,data.shape[1]):
                    str_temp+=","+"\'"+str(data.iloc[i,j])+"\'"

                sql_temp="values"+"("+str_temp+")"
                #print(sql_temp)
                self.cur.execute("insert into hdata_hsgt_table "+sql_temp+";")
            self.conn.commit()

        #print(clock()-t1)

        #print(" insert_perstock_hdatadate finish")


    def insert_optimize_stock_hdatadate(self, data):#插入一支股票的所有历史数据到数据库#如果有code和index相同的不重复插入

        #data format: record_date , stock_code, share_holding, percent
        t1=clock()

        #print(" insert_perstock_hdatadate begin")
        if data is None:
            print("None")
        else:
            length = len(data)
            sql_cmd = ""
            each_num = 1000
            for i in range(0,length):
                # print (i)

                str_temp=""
                #str_temp+="\'"+data.index[i].strftime("%Y-%m-%d")+"\'"
                str_temp+="\'"+data.index[i]+"\'"

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
                        self.cur.execute("insert into hdata_hsgt_table (record_date , stock_code , share_holding, percent ) values "+sql_cmd+";")
                        self.conn.commit()
                        sql_cmd = ""

            #print(sql_cmd)
            if(sql_cmd != ""):
                self.cur.execute("insert into hdata_hsgt_table (record_date , stock_code , share_holding, percent ) values "+sql_cmd+";")
                self.conn.commit()

        #print(clock()-t1)

        #print(stock_code+" insert_perstock_hdatadate finish")


    def get_all_hdata_of_stock_code(self, stock_code):#将数据库中的数据读取并转为dataframe格式返回
        conn = psycopg2.connect(database="usr", user=self.user, password=self.password, host="127.0.0.1",
                                port="5432")
        cur = conn.cursor()

        #sql_temp="select * from (select * from hdata_hsgt_table where stock_code='000922' order by record_date desc LIMIT 5) as tbl order by record_date asc;"
        #sql_temp="select * from (select * from hdata_hsgt_table where stock_code="+"\'"+stock_code+"\' order by record_date desc LIMIT 100) as tbl order by record_date asc;"
        sql_temp="select * from hdata_hsgt_table where stock_code="+"\'"+stock_code+"\';"
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
        
    def get_limit_hdata_of_stock_code(self, stock_code, end_day, limit_number):#将数据库中的数据读取并转为dataframe格式返回
        conn = psycopg2.connect(database="usr", user=self.user, password=self.password, host="127.0.0.1",
                                port="5432")
        cur = conn.cursor()

        #sql_temp="select * from (select * from hdata_hsgt_table where stock_code='000922' order by record_date desc LIMIT 5) as tbl order by record_date asc;"
        sql_temp="select * from (select * from hdata_hsgt_table where stock_code="+"\'"+stock_code+"\'  and record_date <= "+"\'"+ end_day +"\'  order by record_date desc LIMIT "+"\'"+str(limit_number)+"\' ) as tbl order by record_date asc;"
        #sql_temp="select * from hdata_hsgt_table where stock_code="+"\'"+stock_code+"\';"
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
        #select * from (select * from hdata_hsgt_table where stock_code='000922' order by record_date desc LIMIT 5) as tbl order by record_date asc;
        sql_temp="select * from hdata_hsgt_table where record_date = "+"\'"+day+"\';"
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
 
       
    def get_all_hdata_of_stock(self):#将数据库中的数据读取并转为dataframe格式返回
        conn = psycopg2.connect(database="usr", user=self.user, password=self.password, host="127.0.0.1",
                                port="5432")
        cur = conn.cursor()
        #select * from (select * from hdata_hsgt_table where stock_code='000922' order by record_date desc LIMIT 5) as tbl order by record_date asc;
        sql_temp="select * from hdata_hsgt_table;"
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
        
    def get_startday_2_currday_hdata_of_stock(self, stop_day, curr_day, number):#将数据库中的数据读取并转为dataframe格式返回
        conn = psycopg2.connect(database="usr", user=self.user, password=self.password, host="127.0.0.1",
                                port="5432")
        cur = conn.cursor()
        #select * from (select * from hdata_hsgt_table where stock_code='000922' order by record_date desc LIMIT 5) as tbl order by record_date asc;
        sql_temp="select record_date from hdata_hsgt_table where record_date between " + "\'" + stop_day + "\'  and  " + "\'" + curr_day + "\'  group by record_date order by record_date desc limit " + "\'" + str(number) + "\' ;"
        cur.execute(sql_temp)
        rows = cur.fetchall()

        conn.commit()
        conn.close()

        dataframe_cols=[tuple[0] for tuple in cur.description]#列名和数据库列一致
        df = pd.DataFrame(rows, columns=dataframe_cols)
    
        return df
        pass
        


