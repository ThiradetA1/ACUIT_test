import pandas as pd
from sqlalchemy import create_engine, text
import pymysql

file_path = "Relational Data & SQL Optimization/chicago_crimes_100k.csv"
df = pd.read_csv(file_path)

df = df[['id','case_number','date','primary_type','district','latitude','longitude']]
df.columns = ['id','case_number','occurrence_date','primary_type','district','latitude','longitude']

df['occurrence_date'] = pd.to_datetime(df['occurrence_date'])

df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')

df = df[(df['latitude'] >= -90) & (df['latitude'] <= 90)]
df = df[(df['longitude'] >= -180) & (df['longitude'] <= 180)]

df['latitude'] = df['latitude'].round(6)
df['longitude'] = df['longitude'].round(6)

df['district'] = df['district'].astype(str).str.strip()
df['district'] = df['district'].replace('nan', None)

df = df.drop_duplicates(subset=['id'])

connection = pymysql.connect(host='localhost',user='root',password='root')

try:
    with connection.cursor() as cursor:
        cursor.execute("CREATE DATABASE IF NOT EXISTS chicago_db")
finally:
    connection.close()
    
engine = create_engine('mysql+pymysql://root:root@localhost:3306/chicago_db')

with engine.begin() as conn:
    conn.execute(text(
    """
    CREATE TABLE IF NOT EXISTS chicago_crimes (
            id INT PRIMARY KEY,
            case_number VARCHAR(20),
            occurrence_date DATETIME,
            primary_type VARCHAR(100),
            district VARCHAR(10),
            latitude DECIMAL(9, 6),
            longitude DECIMAL(9, 6),
            location POINT SRID 4326
        );
    """
    ))
    try:
        conn.execute(text("CREATE INDEX idx_crime_date ON chicago_crimes (occurrence_date);"))
        conn.execute(text("CREATE SPATIAL INDEX idx_crime_location ON chicago_crimes (location);"))
    except Exception:
        pass 

with engine.begin() as conn:
    conn.execute(text("TRUNCATE TABLE chicago_crimes;"))

try:
    df.to_sql(
        name='chicago_crimes',
        con=engine,
        if_exists='append', 
        index=False,
        chunksize=5000
    )

    with engine.begin() as conn:
        conn.execute(text("""
    UPDATE chicago_crimes 
    SET location = ST_GeomFromText(
        CONCAT('POINT(', latitude, ' ', longitude, ')'), 4326
    )
    WHERE location IS NULL 
    AND latitude BETWEEN -90 AND 90 
    AND longitude BETWEEN -180 AND 180;
    """))


except Exception as e:
    print(e)