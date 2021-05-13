#!/usr/bin/env python  
# -*- coding: utf-8 -*-

import psycopg2
import pandas as pd
import time

import os
import numpy as np
from io import StringIO





debug = 0
#debug = 1



xq_cols = " stock_code, record_date, net_profit_cagr, ps, type, percent, has_follow, \
       tick_size, pb_ttm, float_shares, current, amplitude, pcf, \
       current_year_percent, float_market_capital, market_capital, \
       dividend_yield, lot_size, roe_ttm, total_percent, percent5m, \
       income_cagr, amount, chg, issue_date_ts, main_net_inflows, \
       volume, volume_ratio, pb, followers, turnover_rate, \
       first_percent, name, pe_ttm, total_shares "

class HData_xq_basicinfo(object):
    def __init__(self,user,password):
        # self.aaa = aaa
        self.xq_basicinfo_table=[]
        self.user=user
        self.password=password

        self.conn=None
        self.cur=None

    
    def db_connect(self):
        self.conn = psycopg2.connect(database="usr", \
                                user=self.user, password=self.password, host="127.0.0.1",\
                                port="5432")
        self.cur = self.conn.cursor()

    def db_disconnect(self):
        self.conn.close()

    def table_is_exist(self):
        self.db_connect()
        self.cur.execute("select count(*) from pg_class where relname = 'xq_basicinfo_table' ;")
        ans=self.cur.fetchall()
        #print(list(ans[0])[0])
        if list(ans[0])[0]:
            self.conn.commit()
            self.db_disconnect()
            return True
        else:
            self.conn.commit()
            self.db_disconnect()
            return False

        pass

    def db_hdata_xq_create(self):

        self.db_connect()

        # 创建stocks表
        self.cur.execute('''
            drop table if exists xq_basicinfo_table;
            create table xq_basicinfo_table(
		stock_code             varchar,
                record_date            date,
		net_profit_cagr        float,
		ps                     float,
		type                   float,
		percent                float,
		has_follow             boolean,
		tick_size              float,
		pb_ttm                 float,
		float_shares           float,
		current                float,
		amplitude              float,
		pcf                    float,
		current_year_percent   float,
		float_market_capital   float,
		market_capital         float,
		dividend_yield         float,
		lot_size               float,
		roe_ttm                float,
		total_percent          float,
		percent5m              float,
		income_cagr            float,
		amount                 float,
		chg                    float,
		issue_date_ts          date,
		main_net_inflows       float,
		volume                 float,
		volume_ratio           float,
		pb                     float,
		followers              float,
		turnover_rate          float,
		first_percent          float,
		name                   varchar,
		pe_ttm                 float,
		total_shares           float
               );
            alter table xq_basicinfo_table add primary key(stock_code, record_date);
            ''')
        self.conn.commit()
        self.db_disconnect()

        print("db_xq_basicinfo_table_create finish")
        pass

    def copy_from_stringio(self, df):
        """
        Here we are going save the dataframe in memory
        and use copy_from() to copy it to the table
        """
        # save dataframe to an in memory buffer
        buffer = StringIO()
        #df.to_csv(buffer, index_label='id', header=False)
        df.to_csv(buffer, index=0, header=False)
        buffer.seek(0)

        self.db_connect()
        try:
            self.cur.copy_from(buffer, table='xq_basicinfo_table', sep=",")
            self.conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            self.conn.rollback()
            self.db_disconnect()
            return 1
        
        if debug:
            print("copy_from_stringio() done")
        self.db_disconnect()

    def db_get_maxdate_of_stock(self,stock_code):#获取某支股票的最晚日期

        self.db_connect()
        self.cur.execute("select max(record_date) from xq_basicinfo_table \
                where stock_code=\'" + stock_code+ "\' ;")
        ans=self.cur.fetchall()
        if(len(ans)==0):
            self.conn.commit()
            self.db_disconnect()
            return None
        else:
            self.conn.commit()
            self.db_disconnect()
            return ans[0][0]

        pass

    def get_data_from_hdata(self, stock_code=None, 
                        start_date=None, 
                        end_date=None,
                        limit=0):#将数据库中的数据读取并转为dataframe格式返回
        self.db_connect()
        
        and_flag = False


        sql_temp = "select"  
        sql_temp += xq_cols 
        sql_temp += "from ( "

        sql_temp += "select"
        sql_temp += xq_cols
        sql_temp += "from xq_basicinfo_table"

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

        sql_temp +=" ) as tbl order by record_date asc"

        sql_temp += ";"

        if debug:
            print("get_data_from_hdata, sql_temp:%s" % sql_temp)



        #select * from (select * from hdata_hsgt_table where stock_code='000922' order by record_date desc LIMIT 5) as tbl order by record_date asc;
        self.cur.execute(sql_temp)
        rows = self.cur.fetchall()

        self.conn.commit()
        self.db_disconnect()

        dataframe_cols=[tuple[0] for tuple in self.cur.description]#列名和数据库列一致
        df = pd.DataFrame(rows, columns=dataframe_cols)
        df['record_date'] = df['record_date'].apply(lambda x: x.strftime('%Y-%m-%d'))        

        if debug:
            print(type(df))
            print(df.head(2))
    
        return df
        pass
 
    def delete_data_from_hdata(self, stock_code=None, 
                        start_date=None, 
                        end_date=None,
                        ):
        self.db_connect()
        
        and_flag = False

        sql_temp = "delete from xq_basicinfo_table"

        if stock_code is None and start_date is None and end_date is None:
            self.db_disconnect()
            pass
            return
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

        sql_temp += ";"

        if debug:
            print("delete_data_from_hdata, sql_temp:%s" % sql_temp)

        self.cur.execute(sql_temp)
        self.conn.commit()
        self.db_disconnect()
        pass
 

