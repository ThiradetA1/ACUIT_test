import requests
import time
import pandas as pd

url = "https://data.cityofchicago.org/resource/ijzp-q8t2.json"

all_data = []
target_rows = 100000
rows_per_batch = 5000
offset = 0


while len(all_data) < target_rows:
    params = {
        "$limit": rows_per_batch,
        "$offset": offset,
        "$order": "id ASC" 
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            
            if not data or len(data) == 0:
                break
        
            all_data.extend(data)
            offset += len(data)
            print(f"Progress: {len(all_data)} / {target_rows} rows", end="\r")
            
            if len(all_data) >= target_rows:
                break
        elif response.status_code == 429:
            time.sleep(30)
            continue
        else:
            print(f"\nError: {response.status_code}")
            break
            
    except Exception as e:
        print(f"\nError occurred: {e}")
        break
        
    time.sleep(0.1)

if all_data:
    df = pd.DataFrame(all_data)
    df = df.head(target_rows) 
    df.to_csv("chicago_crimes_100k.csv", index=False)
    print(len(df))
else:
    print("\nไม่มีข้อมูลถูกดึงมา")