import pandas as pd
df= pd.read_csv(D:\Data Engineer Projects\advarisk\company-sales.csv)
print(df)

pip install pyspark
df = spark.read.format("csv").option("header", "true").load(D:\Data Engineer Projects\advarisk\company-sales.csv)
df.to_csv("D:\Data Engineer Projects\advarisk/company_sales_data.csv")

import psycopg2
conn = psycopg2.connect("host='localhost' port='5432' dbname='company_sales' user='postgres' password='123456789'")
cur = conn.cursor()
cur.execute("""truncate table "meta".temp_unicommerce_status;""")
cur.execute("""Copy temp_unicommerce_status from 'D:\Data Engineer Projects\advarisk/company_sales_data.csv';""")
conn.commit()
conn.close()