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

#ใช้ Column ['block'] ส่งไปที่ API Google Map หรือ Nominatim เพื่อแปลงกลับมาเป็น Lat/lon
#Check Bias ตรวจสอบก่อนว่าข้อมูล latitude กับ longitude ที่หายไปเป็นแบบสุ่มหรือแบบเฉพาะพท้นที่เพื่อไม่ให้ model เกิดการ biass
#ทำการตรวจสอบพิกัดที่ไม่ได้อยู่ในขอบเขตของ chicago
#ใช้ Uber H3 เพื่อแปลงพิกัดเป็น Discrete Area IDs เพื่อแก้ GPS Noise 
#ทำข้อมูล 2 ชุด ชุดแรกเป็น original data ชุดสองเป็น ข้อมูลที่เราจัดการมา เพื่อดูว่าข้อมูลที่เราทำมา สร้าง Noise หรือ ทำให้ Model เกิด Bias ไปจากความจริงไหม