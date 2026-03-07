import pandas as pd

file_path = "Relational Data & SQL Optimization/chicago_crimes_100k.csv"
df = pd.read_csv(file_path)

total_rows = len(df)

missing_lat = df['latitude'].isnull().sum()
missing_lon = df['longitude'].isnull().sum()

percent_missing_lat = (missing_lat/total_rows) *100
percent_missing_lon = (missing_lon/total_rows) *100

print(f"Missing latitude : ",percent_missing_lat,"%")
print(f"Missing longitude : ",percent_missing_lon,"%")

#Geocoding ใช้ Column block ส่งไปที่ API Google Map หรือ Nominatim เพื่อแปลงกลับมาเป็น Lat/lon
#