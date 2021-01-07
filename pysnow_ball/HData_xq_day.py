#!/usr/bin/env python  
# -*- coding: utf-8 -*-

import  psycopg2
import tushare as ts
import pandas as pd
import time

debug = 0
#debug = 1

db_columns = " record_date , stock_code , open , close , high , low , volume ,  amount , p_change "

xq_cols = " record_date, stock_code, volume, open, high, low,\
        close, chg, percent, turnoverrate, amount, \
        pe, pb, ps,pcf, market_capital, \
        hk_volume, hk_pct, hk_net,\
        is_peach, is_zig, is_quad "




class HData_xq_day(object):
    def __init__(self,user,password):
        # self.aaa = aaa
        self.xq_d_table=[]
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

    def table_is_exist(self):
        self.db_connect()
        self.cur.execute("select count(*) from pg_class where relname = 'xq_d_table' ;")
        ans=self.cur.fetchall()
        print(list(ans[0])[0])
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
            drop table if exists xq_d_table;
            create table xq_d_table(
                record_date date, 
                stock_code varchar,
                volume  float, 
                open float, 
                high float, 
                low float,
                close float, 
                chg float, 
                percent float, 
                turnoverrate float,
                amount float, 
                pe float, 
                pb float, 
                ps float,
                pcf float, 
                market_capital float, 
                hk_volume float, 
                hk_pct float, 
                hk_net float,
                is_peach    float,
                is_zig      float,
                is_quad     float
                );
            alter table xq_d_table add primary key(stock_code,record_date);
            ''')
        self.conn.commit()
        self.db_disconnect()

        print("db_xq_d_table_create finish")
        pass

    def db_get_maxdate_of_stock(self,stock_code):#获取某支股票的最晚日期

        self.db_connect()
        self.cur.execute("select max(record_date) from xq_d_table \
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

    def insert_all_stock_data(self, data):
        #data format: record_date , stock_code , open , close , high , low  , volume ,  amount  , p_change 
        #data format: ['timestamp', 'symbol', 'open', 'close', 'high', 'low', 'volume', 'amount', 'percent', 'chg', 'turnoverrate', 'pe', 'pb', 'ps', 'pcf', 'market_capital', 'hk_volume', 'hk_pct', 'hk_net', 'is_quad', 'is_zig', 'is_quad']
        self.db_connect()
        t1=time.time()

        if debug:
            print('insert_all_stock_data()')
        if data is None:
            print("None")
        else:
            length = len(data)
            sql_cmd = ""
            each_num = 1000
            for i in range(0,length):
                if debug:
                    print (i)

                #str_temp+="\'"+stock_code+"\'"+","
                #str_temp+="\'"+data.index[i]+"\'"
                #str_temp+="\'"+data.index[i].strftime("%Y-%m-%d")+"\'"

                str_temp= "\'" + str(data.iloc[i,0]) +  "\'"    #timestamp must be string
                str_temp+=",\'"+str(data.iloc[i,1]) + "\'"      #stock_code must be string
                for j in range(2,data.shape[1]):
                    str_temp+=","+str(data.iloc[i,j])

                sql_cmd= sql_cmd + "("+str_temp+")"
                if i is 0:
                    sql_cmd = sql_cmd+","
                elif i % each_num == 0 or i == (length -1):
                    pass
                else:
                    sql_cmd = sql_cmd+","

                if i % each_num == 0 and i is not 0:
                    if debug:
                        print(sql_cmd)
                    if(sql_cmd != ""):
                        final_cmd = "insert into xq_d_table ("\
                                + xq_cols + \
                                " ) values "+sql_cmd+";"
                        if debug:
                            print(final_cmd)
                        self.cur.execute(final_cmd)
                        self.conn.commit()
                        sql_cmd = ""

            if debug:
                print(sql_cmd)
            if(sql_cmd != ""):
                final_cmd = "insert into xq_d_table ("\
                        + xq_cols + \
                        " ) values "+sql_cmd+";"
                if debug:
                    print(final_cmd)
                self.cur.execute(final_cmd)
                self.conn.commit()

        if debug:
            print(time.time()-t1)
            print('insert_all_stock_data(\\)')

        self.db_disconnect()



    def update_allstock_hdatadate(self, data):

        self.db_connect()

        t1=time.time()

        if debug:
            print(" update_perstock_hdatadate begin")

        if data is None:
            print("None")
        else:
            length = len(data)
            sql_cmd = ""
            sql_head="UPDATE xq_d_table SET is_zig = tmp.is_zig, \
                    is_quad=tmp.is_quad, is_peach=tmp.is_peach FROM ( VALUES "

            sql_tail=" ) AS tmp (record_date, stock_code, is_peach, is_zig, is_quad ) \
                    WHERE xq_d_table.record_date = tmp.record_date \
                    and xq_d_table.stock_code = tmp.stock_code;"

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
            print(time.time()-t1)
            print(" insert_perstock_hdatadate finish")

        self.db_disconnect()

    def delete_data_of_record_date(self, record_date):

        self.db_connect()

        sql_temp="delete from xq_d_table where record_date = " + "\'" + record_date + "\' ;"
        self.cur.execute(sql_temp)

        self.conn.commit()
        self.db_disconnect()

        pass
 
    def delete_data_of_stock_code(self, stock_code):
        self.db_connect()

        sql_temp="delete from xq_d_table where stock_code = " + "\'" + stock_code + "\' ;"
        self.cur.execute(sql_temp)

        self.conn.commit()
        self.db_disconnect()

        pass
 
    #fix bug: delete zero when the stock is closed
    def delete_amount_is_zero(self):
        self.db_connect()
        sql_temp="delete from xq_d_table where amount = 0;"
        self.cur.execute(sql_temp)

        self.conn.commit()
        self.db_disconnect()
        pass
 


#alter table xq_d_table add  "up_days" int not null default 0;


#update xq_d_table set is_zig=0 where record_date = '2018-10-08' and stock_code = '002732';

#UPDATE xq_d_table SET is_zig = tmp.is_zig, is_quad=tmp.is_quad FROM    (VALUES ( DATE  '2018-06-13', '600647', 11, 1), ( DATE  '2018-06-12', '600647', 12, 1) ) AS tmp (record_date, stock_code, is_zig, is_quad ) WHERE xq_d_table.record_date = tmp.record_date and xq_d_table.stock_code = tmp.stock_code;
#select * from xq_d_table where stock_code ='600647' and (record_date='2018-06-13' or record_date='2018-06-12' ); 
        
