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