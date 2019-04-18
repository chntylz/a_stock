#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created on Wed Mar 09 11:08:16 2016

@author: wangxin2
"""

import psycopg2
# 数据库连接参数
conn = psycopg2.connect(database="usr", user="usr", password="usr", host="127.0.0.1", port="5432")
cur = conn.cursor()
cur.execute("CREATE TABLE ztest(id serial PRIMARY KEY, num integer,data varchar);")
# insert one item
cur.execute("INSERT INTO ztest(num, data)VALUES(%s, %s)", (1, 'aaa'))
cur.execute("INSERT INTO ztest(num, data)VALUES(%s, %s)", (2, 'bbb'))
cur.execute("INSERT INTO ztest(num, data)VALUES(%s, %s)", (3, 'ccc'))

cur.execute("SELECT * FROM ztest;")
rows = cur.fetchall()        # all rows in table
print(rows)
for i in rows:
    print(i)
conn.commit()
cur.close()
conn.close()
