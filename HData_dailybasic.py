#!/usr/bin/env python  
# -*- coding: utf-8 -*-

'''
总市值
https://tushare.pro/document/2?doc_id=32

ts_code str TS股票代码
trade_date  str 交易日期
close   float   当日收盘价
turnover_rate   float   换手率（%）
turnover_rate_f float   换手率（自由流通股）
volume_ratio    float   量比
pe  float   市盈率（总市值/净利润）
pe_ttm  float   市盈率（TTM）
pb  float   市净率（总市值/净资产）
ps  float   市销率
ps_ttm  float   市销率（TTM）
dv_ratio    float   股息率 （%）
dv_ttm  float   股息率（TTM）（%）
total_share float   总股本 （万股）
float_share float   流通股本 （万股）
free_share  float   自由流通股本 （万）
total_mv    float   总市值 （万元）
circ_mv float   流通市值（万元）

'''

import  psycopg2
import tushare as ts
import pandas as pd

debug = 0

class HData_dailybasic(object):
    def __init__(self,user,password):
        # self.aaa = aaa
        self.hdata_dailybasic=[]
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
                drop table if exists hdata_dailybasic;
                create table hdata_dailybasic(
                    record_date     date,
                    stock_code      varchar,
                    close           float,
                    turnover_rate   float,
                    turnover_rate_f float,
                    volume_ratio    float,
                    pe              float,
                    pe_ttm          float,
                    pb              float,
                    ps              float,
                    ps_ttm          float,
                    dv_ratio        float,
                    dv_ttm          float,
                    total_share     float,
                    float_share     float,
                    free_share      float,
                    total_mv        float,
                    circ_mv         float

                    );
                alter table hdata_dailybasic add primary key(stock_code,record_date);
                ''')
        conn.commit()
        conn.close()
        print("db_hdata_dailybasic_create finish")
        pass




    def db_get_maxdate_of_stock(self,stock_code):#获取某支股票的最晚日期
        self.cur.execute("select max(record_date) from hdata_dailybasic where stock_code="+"\'"+stock_code+"\'"+";")
        ans=self.cur.fetchall()
        if(len(ans)==0):
            return None
        return ans[0][0]
        self.conn.commit()
        pass

    def insert_perstock_hdatadate(self,stock_code,data):#插入一支股票的所有历史数据到数据库#如果有code和index相同的不重复插入

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
                self.cur.execute("insert into hdata_dailybasic "+sql_temp+";")
            self.conn.commit()


        #print(stock_code+" insert_perstock_hdatadate finish")


    def insert_allstock_hdatadate(self, data):#插入一支股票的所有历史数据到数据库#如果有code和index相同的不重复插入

        data_format = " record_date, stock_code,  close, turnover_rate, turnover_rate_f, volume_ratio, pe, pe_ttm, pb,  ps, ps_ttm, dv_ratio, dv_ttm, total_share, float_share, free_share, total_mv, circ_mv"


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
                        self.cur.execute("insert into hdata_dailybasic (" + data_format + " ) values " + sql_cmd + ";")
                        self.conn.commit()
                        sql_cmd = ""

            #print(sql_cmd)
            if(sql_cmd != ""):
                self.cur.execute("insert into hdata_dailybasic (" + data_format + " ) values " + sql_cmd + ";")
                self.conn.commit()


        #print(stock_code+" insert_perstock_hdatadate finish")


    def get_all_hdata_of_stock(self,stock_code):#将数据库中的数据读取并转为dataframe格式返回
        conn = psycopg2.connect(database="usr", user=self.user, password=self.password, host="127.0.0.1",
                                port="5432")
        cur = conn.cursor()

        #sql_temp="select * from (select * from hdata_dailybasic where stock_code='000922' order by record_date desc LIMIT 5) as tbl order by record_date asc;"
        #sql_temp="select * from (select * from hdata_dailybasic where stock_code="+"\'"+stock_code+"\' order by record_date desc LIMIT 100) as tbl order by record_date asc;"
        sql_temp="select * from hdata_dailybasic where stock_code="+"\'"+stock_code+"\';"
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

        #sql_temp="select * from (select * from hdata_dailybasic where stock_code='000922' order by record_date desc LIMIT 5) as tbl order by record_date asc;"
        sql_temp="select * from (select * from hdata_dailybasic where stock_code="+"\'"+stock_code+"\'  and record_date <= "+"\'"+ end_day +"\'  order by record_date desc LIMIT "+"\'"+str(limit_number)+"\' ) as tbl order by record_date asc;"
        #sql_temp="select * from hdata_dailybasic where stock_code="+"\'"+stock_code+"\';"
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
        #select * from (select * from hdata_dailybasic where stock_code='000922' order by record_date desc LIMIT 5) as tbl order by record_date asc;
        sql_temp="select * from hdata_dailybasic where record_date = "+"\'"+day+"\';"
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
        #select * from (select * from hdata_dailybasic where stock_code='000922' order by record_date desc LIMIT 5) as tbl order by record_date asc;
        sql_temp="select * from hdata_dailybasic;"
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
        #select * from (select * from hdata_dailybasic where stock_code='000922' order by record_date desc LIMIT 5) as tbl order by record_date asc;
        sql_temp="select record_date from hdata_dailybasic where record_date between " + "\'" + stop_day + "\'  and  " + "\'" + curr_day + "\'  group by record_date order by record_date desc limit " + "\'" + str(number) + "\' ;"
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
        sql_temp="select * from hdata_dailybasic where stock_code = " + "\'" + stock_code + "\'  and record_date = " + "\'" + record_date + "\' ;"
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
        sql_temp="delete from hdata_dailybasic where record_date = " + "\'" + record_date + "\' ;"
        cur.execute(sql_temp)

        conn.commit()
        conn.close()

        pass
 
    #fix bug: delete zero when the stock is closed
    def delete_amount_is_zero(self):
        conn = psycopg2.connect(database="usr", user=self.user, password=self.password, host="127.0.0.1",
                                port="5432")
        cur = conn.cursor()
        sql_temp="delete from hdata_dailybasic where amount = 0;"
        cur.execute(sql_temp)

        conn.commit()
        conn.close()

        pass

    def get_data_from_hdata(self, stock_code=None,
                        start_date=None,
                        end_date=None,
                        limit=0):#将数据库中的数据读取并转为dataframe格式返回
        conn = psycopg2.connect(database="usr", user=self.user, password=self.password, host="127.0.0.1",
                                port="5432")
        cur = conn.cursor()
        and_flag = False

        sql_temp = "select * from hdata_dailybasic"

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



        #select * from (select * from hdata_dailybasic where stock_code='000922' order by record_date desc LIMIT 5) as tbl order by record_date asc;
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

 
