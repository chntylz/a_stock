#!/usr/bin/env python  
# -*- coding: utf-8 -*-

import  psycopg2
import tushare as ts
import pandas as pd
from time import clock

debug = 0
#debug = 1

class HData_divided(object):
    def __init__(self,user,password):
        # self.aaa = aaa
        self.hdata_divided=[]
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
                drop table if exists hdata_divided;
                create table hdata_divided(
                    record_date     date,
                    stock_code      varchar,
                    is_divided      float,
                    div_value       float
                    );
                alter table hdata_divided add primary key(stock_code,record_date);
                ''')
        conn.commit()
        conn.close()
        print("db_hdata_divided_create finish")
        pass


    def insert_allstock_hdatadate(self, data):#插入一支股票的所有历史数据到数据库#如果有code和index相同的不重复插入
        
        conn = psycopg2.connect(database="usr", user=self.user, password=self.password, host="127.0.0.1",
                                port="5432")

        data_format = " record_date, stock_code, is_divided, div_value"

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
                        self.cur.execute("insert into hdata_divided (" + data_format + " ) values " + sql_cmd + ";")
                        self.conn.commit()
                        sql_cmd = ""

            #print(sql_cmd)
            if(sql_cmd != ""):
                self.cur.execute("insert into hdata_divided (" + data_format + " ) values " + sql_cmd + ";")
                self.conn.commit()

        #print(clock()-t1)
        #print(stock_code+" insert_perstock_hdatadate finish")

        conn.commit()
        conn.close()

    def get_data_from_hdata(self, stock_code=None,
                        start_date=None,
                        end_date=None,
                        limit=0):#将数据库中的数据读取并转为dataframe格式返回
        conn = psycopg2.connect(database="usr", user=self.user, password=self.password, host="127.0.0.1",
                                port="5432")
        cur = conn.cursor()
        and_flag = False

        sql_temp = "select * from hdata_divided"

        if stock_code is None and start_date is None and end_date is None:
            pass
        else:
            sql_temp += " where "

        if stock_code is None:
            pass
        else:
            sql_temp += " stock_code="+"\'"+stock_code+"\'"
            and_flag |= True

        if start_date is None:
            pass
        else:
            if and_flag:
                sql_temp += " and record_date >="+"\'"+start_date+"\'"
            else:
                sql_temp += " record_date >="+"\'"+start_date+"\'"

            and_flag |= True


        if end_date is None:
            pass
        else:
            if and_flag:
                sql_temp += " and record_date <="+"\'"+end_date+"\'"
            else:
                sql_temp += " record_date <="+"\'"+end_date+"\'"


        sql_temp += " order by record_date desc "

        if limit == 0:
            pass
        else:
            sql_temp += " LIMIT "+"\'"+str(limit)+"\'"


        sql_temp += ";"

        if debug:
            print("get_data_from_hdata, sql_temp:%s" % sql_temp)



        #select * from (select * from hdata_hsgt_table where stock_code='000922' order by record_date desc LIMIT 5) as tbl order by record_date asc;
        cur.execute(sql_temp)
        rows = cur.fetchall()

        conn.commit()
        conn.close()

        dataframe_cols=[tuple[0] for tuple in cur.description]#列名和数据库列一致
        df = pd.DataFrame(rows, columns=dataframe_cols)

        if debug:
            print(type(df))
            print(df.head(2))

        return df

        pass


