import pandas as pd
from sqlalchemy import create_engine
import pymysql

file_path = "Relational Data & SQL Optimization/chicago_crimes_100k.csv"
df = pd.read_csv(file_path)

df['date'] = pd.to_datetime(df['date'])

print(df['id'].duplicated().sum())

engine = create_engine('mysql+pymysql://root:root@localhost:3306/chicago_db')

connection = pymysql.connect(
    host='localhost',
    user='root',
    password='root'
)

try:
    with connection.cursor() as cursor:
        cursor.execute("CREATE DATABASE IF NOT EXISTS chicago_db")
finally:
    connection.close()
    
try:
    df.to_sql(
        name='crimes_data',
        con=engine,
        if_exists='replace',
        index=False,
        chunksize=5000
    )
except Exception as e:
    print(e)