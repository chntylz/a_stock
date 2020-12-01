#!/usr/bin/env python  
# -*- coding: utf-8 -*-

import  psycopg2
import tushare as ts
import pandas as pd
from time import clock

debug = 0
#debug = 1
db_columns = "record_date , stock_code , open , close , high , low , volume ,  amount , p_change "

class HData_day(object):
    def __init__(self,user,password):
        # self.aaa = aaa
        self.hdata_d_table=[]
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
                drop table if exists hdata_d_table;
                create table hdata_d_table(
                    record_date date,
                    stock_code varchar,  
                    open float,close float,high float,low float,
                    volume float,
                    amount float,
                    p_change float
                    );
                alter table hdata_d_table add primary key(stock_code,record_date);
                ''')
        conn.commit()
        conn.close()
        print("db_hdata_d_table_create finish")
        pass

    def db_get_maxdate_of_stock(self,stock_code):#获取某支股票的最晚日期
        self.cur.execute("select max(record_date) from hdata_d_table where stock_code="+"\'"+stock_code+"\'"+";")
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
                self.cur.execute("insert into hdata_d_table "+sql_temp+";")
            self.conn.commit()

        #print(clock()-t1)

        #print(stock_code+" insert_perstock_hdatadate finish")

    def insert_allstock_hdatadate(self, data):#插入一支股票的所有历史数据到数据库#如果有code和index相同的不重复插入

        #data format: record_date , stock_code , open , close , high , low  , volume ,  amount  , p_change 
        t1=clock()

        #print(stock_code+" insert_perstock_hdatadate begin")
        if data is None:
            print("None")
        else:
            length = len(data)
            sql_cmd = ""
            each_num = 1000
            for i in range(0,length):
                if debug:
                    print (i)

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
                    if debug:
                        print(sql_cmd)
                    if(sql_cmd != ""):
                        self.cur.execute("insert into hdata_d_table (record_date , stock_code , open , close , high , low  , volume ,  amount  , p_change ) values "+sql_cmd+";")
                        self.conn.commit()
                        sql_cmd = ""

            if debug:
                print(sql_cmd)
            if(sql_cmd != ""):
                self.cur.execute("insert into hdata_d_table (record_date , stock_code , open , close , high , low  , volume ,  amount  , p_change ) values "+sql_cmd+";")
                self.conn.commit()

        #print(clock()-t1)

        #print(stock_code+" insert_perstock_hdatadate finish")




    def update_allstock_hdatadate(self, data):

        t1=clock()

        if debug:
            print(" update_perstock_hdatadate begin")

        if data is None:
            print("None")
        else:
            length = len(data)
            sql_cmd = ""
            sql_head="UPDATE hdata_d_table SET is_zig = tmp.is_zig, is_quad=tmp.is_quad, is_peach=tmp.is_peach FROM ( VALUES "
            sql_tail=" ) AS tmp (record_date, stock_code, is_peach, is_zig, is_quad ) WHERE hdata_d_table.record_date = tmp.record_date and hdata_d_table.stock_code = tmp.stock_code;"
            each_num = 1000
            for i in range(0,length):
                if debug:
                    print (i)

                str_temp = ""
                str_temp+="DATE "+"\'"+str(data.iloc[i,0])+"\'" + ","
                column_size = data.shape[1]

                if debug:
                    print('column_size=%d'% (column_size))
                '''
                for j in range(1, column_size - 1):
                    str_temp+="\'"+str(data.iloc[i,j])+"\'" + ","
                str_temp+="\'"+str(data.iloc[i,column_size-1])+"\'" 
                '''
                str_temp+="\'"+str(data.iloc[i,1])+"\'" + ","
                for j in range(2, column_size - 1):
                    str_temp+=str(data.iloc[i,j])+ ","
                str_temp+=str(data.iloc[i,column_size-1]) 

                sql_cmd= sql_cmd + "("+str_temp+")"

                if i % each_num == 0 or i == (length -1):
                    pass
                else:
                    sql_cmd = sql_cmd+ ","

                if i % each_num == 0:
                    if debug:
                        print(sql_cmd)
                    if(sql_cmd != ""):
                        final_sql=sql_head +sql_cmd+ sql_tail
                        if debug:
                            print('final_sql=%s'%(final_sql))
                            
                        self.cur.execute(final_sql)
                        self.conn.commit()
                        sql_cmd = ""

            if debug:
                print(sql_cmd)
            if(sql_cmd != ""):
                final_sql=sql_head +sql_cmd+ sql_tail
                if debug:
                    print('final_sql=%s'%(final_sql))
                    
                self.cur.execute(final_sql)
                self.conn.commit()
                pass

        if debug:
            print(clock()-t1)
            print(" insert_perstock_hdatadate finish")


    def get_all_hdata_of_stock(self,stock_code):#将数据库中的数据读取并转为dataframe格式返回
        conn = psycopg2.connect(database="usr", user=self.user, password=self.password, host="127.0.0.1",
                                port="5432")
        cur = conn.cursor()

        #sql_temp="select * from (select * from hdata_d_table where stock_code='000922' order by record_date desc LIMIT 5) as tbl order by record_date asc;"
        #sql_temp="select * from (select * from hdata_d_table where stock_code="+"\'"+stock_code+"\' order by record_date desc LIMIT 100) as tbl order by record_date asc;"
        sql_temp="select * from hdata_d_table where stock_code="+"\'"+stock_code+"\';"
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

        #sql_temp="select * from (select * from hdata_d_table where stock_code='000922' order by record_date desc LIMIT 5) as tbl order by record_date asc;"
        #sql_temp="select " +  db_columns +"  from (select " + db_columns + " from hdata_d_table where stock_code="+"\'"+stock_code+"\'  and record_date <= "+"\'"+ end_day +"\'  order by record_date desc LIMIT "+"\'"+str(limit_number)+"\' ) as tbl order by record_date asc;"
        sql_temp="select * from (select * from hdata_d_table where stock_code="+"\'"+stock_code+"\'  and record_date <= "+"\'"+ end_day +"\'  order by record_date desc LIMIT "+"\'"+str(limit_number)+"\' ) as tbl order by record_date asc;"
        #sql_temp="select * from hdata_d_table where stock_code="+"\'"+stock_code+"\';"
        if debug:
            print('sql_temp:%s'%sql_temp)
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
        #select * from (select * from hdata_d_table where stock_code='000922' order by record_date desc LIMIT 5) as tbl order by record_date asc;
        sql_temp="select * from hdata_d_table where record_date = "+"\'"+day+"\';"
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
        #select * from (select * from hdata_d_table where stock_code='000922' order by record_date desc LIMIT 5) as tbl order by record_date asc;
        sql_temp="select * from hdata_d_table;"
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
        #select * from (select * from hdata_d_table where stock_code='000922' order by record_date desc LIMIT 5) as tbl order by record_date asc;
        sql_temp="select record_date from hdata_d_table where record_date between " + "\'" + stop_day + "\'  and  " + "\'" + curr_day + "\'  group by record_date order by record_date desc limit " + "\'" + str(number) + "\' ;"
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
        sql_temp="select * from hdata_d_table where stock_code = " + "\'" + stock_code + "\'  and record_date = " + "\'" + record_date + "\' ;"
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
        sql_temp="delete from hdata_d_table where record_date = " + "\'" + record_date + "\' ;"
        cur.execute(sql_temp)

        conn.commit()
        conn.close()

        pass
 
    def delete_data_of_stock_code(self, stock_code):
        conn = psycopg2.connect(database="usr", user=self.user, password=self.password, host="127.0.0.1",
                                port="5432")
        cur = conn.cursor()
        sql_temp="delete from hdata_d_table where stock_code = " + "\'" + stock_code + "\' ;"
        cur.execute(sql_temp)

        conn.commit()
        conn.close()

        pass
 
    #fix bug: delete zero when the stock is closed
    def delete_amount_is_zero(self):
        conn = psycopg2.connect(database="usr", user=self.user, password=self.password, host="127.0.0.1",
                                port="5432")
        cur = conn.cursor()
        sql_temp="delete from hdata_d_table where amount = 0;"
        cur.execute(sql_temp)

        conn.commit()
        conn.close()

        pass
 


#alter table hdata_d_table add  "up_days" int not null default 0;


#update hdata_d_table set is_zig=0 where record_date = '2018-10-08' and stock_code = '002732';

#UPDATE hdata_d_table SET is_zig = tmp.is_zig, is_quad=tmp.is_quad FROM    (VALUES ( DATE  '2018-06-13', '600647', 11, 1), ( DATE  '2018-06-12', '600647', 12, 1) ) AS tmp (record_date, stock_code, is_zig, is_quad ) WHERE hdata_d_table.record_date = tmp.record_date and hdata_d_table.stock_code = tmp.stock_code;
#select * from hdata_d_table where stock_code ='600647' and (record_date='2018-06-13' or record_date='2018-06-12' ); 
        
