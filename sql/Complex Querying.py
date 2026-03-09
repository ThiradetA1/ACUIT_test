import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine, text

engine = create_engine('mysql+pymysql://root:root@localhost:3306/chicago_db')

sql_query = """
WITH MaxTheftDate AS (
    SELECT MAX(DATE(occurrence_date)) AS max_dt
    FROM chicago_crimes    
    WHERE primary_type = 'THEFT'
),
DailyTheft AS (
    SELECT 
        DATE(c.occurrence_date) AS crime_date,
        c.district,
        COUNT(c.id) AS daily_count
    FROM chicago_crimes c
    CROSS JOIN MaxTheftDate m
    WHERE c.primary_type = 'THEFT'
      AND DATE(c.occurrence_date) >= m.max_dt - INTERVAL 36 DAY
    GROUP BY DATE(c.occurrence_date), c.district
),
DateSpine AS (
    SELECT DISTINCT crime_date FROM DailyTheft
),
DistrictList AS (
    SELECT DISTINCT district FROM DailyTheft
),
FullGrid AS (
    SELECT d.crime_date, dl.district
    FROM DateSpine d
    CROSS JOIN DistrictList dl
),
Filled AS (
    SELECT
        g.crime_date,
        g.district,
        COALESCE(dt.daily_count, 0) AS daily_count
    FROM FullGrid g
    LEFT JOIN DailyTheft dt
            ON g.crime_date = dt.crime_date
           AND g.district   = dt.district
),
RollingAverage AS (
    SELECT 
        crime_date,
        district,
        daily_count,
        ROUND(AVG(daily_count) OVER (
            PARTITION BY district
            ORDER BY crime_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ), 2) AS rolling_avg_7d
    FROM Filled
)
SELECT 
    r.crime_date,
    r.district,
    r.daily_count,
    r.rolling_avg_7d
FROM RollingAverage r
CROSS JOIN MaxTheftDate m
WHERE r.crime_date > m.max_dt - INTERVAL 30 DAY
ORDER BY r.district ASC, r.crime_date DESC;
"""

with engine.connect() as conn:
    df_result = pd.read_sql(text(sql_query), conn)
    
    
top_5_districts = df_result.groupby('district')['daily_count'].sum().nlargest(5).index
df_top_5 = df_result[df_result['district'].isin(top_5_districts)]

plt.figure(figsize=(14, 7))
sns.lineplot(
    data=df_top_5, 
    x='crime_date', 
    y='rolling_avg_7d', 
    hue='district',
    marker='o'
)

plt.title('7-Day Rolling Average of THEFT Crimes (Top 5 Districts) - Last 30 Days', fontsize=16)
plt.xlabel('Date', fontsize=12)
plt.ylabel('7-Day Rolling Average', fontsize=12)
plt.xticks(rotation=45)
plt.grid(True, linestyle='--', alpha=0.7)
plt.tight_layout()

plt.show()