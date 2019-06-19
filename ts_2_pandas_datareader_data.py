
import psycopg2
# 数据库连接参数

conn = psycopg2.connect(database="usr", user="usr", password="usr", host="127.0.0.1", port="5432")
cur = conn.cursor()
cur.execute("SELECT * FROM hdata_date where stock_code='000922' order by record_date desc;")


rows = cur.fetchall()        # all rows in table
print(rows)

cur.close()
conn.close()



from pandas.core.frame import DataFrame
df_test = DataFrame(rows, columns=['stock_code', 'date', 'open', 'high', 'close', 'low', 'volume', 'price_change', 'p_change', 'ma5', 'ma10', 'ma20', 'v_ma5', 'v_ma10', 'v_ma20', 'turnover'])
index=df_test["date"]
df_test = DataFrame(rows, index=index, columns=['stock_code', 'date', 'open', 'high', 'close', 'low', 'volume', 'price_change', 'p_change', 'ma5', 'ma10', 'ma20', 'v_ma5', 'v_ma10', 'v_ma20', 'turnover'])
df_test.head(10)


