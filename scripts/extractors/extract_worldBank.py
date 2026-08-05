import requests
import json
from datetime import datetime
import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
if project_root not in sys.path:
    sys.path.append(project_root)

from utils.aws_helper import upload_to_s3

# Konfigurasi S3
BUCKET_NAME = 'learn-aws-imam'
current_date = datetime.now().strftime('%Y-%m-%d')

# Dictionary endpoint API World Bank (PDB, Populasi, dan Master Metadata)
# Pake country/all biar narik seluruh negara, dan per_page=20000 biar ketarik semua dalam 1 request
endpoints = {
    "gdp": "https://api.worldbank.org/v2/country/all/indicator/NY.GDP.MKTP.CD?format=json&per_page=20000",
    "population": "https://api.worldbank.org/v2/country/all/indicator/SP.POP.TOTL?format=json&per_page=20000",
    "indicator_metadata": "https://api.worldbank.org/v2/indicator?format=json&per_page=20000"
}

# Looping untuk narik dan upload masing-masing dataset
for data_name, url in endpoints.items():
    print(f"Memulai ekstraksi data {data_name}...")
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()
        
        # Bikin path dinamis di S3 khusus di layer BRONZE/world_bank/
        path = f"BRONZE/world_bank/stg_wb_{data_name}/wb_{data_name}_data_{current_date}.json"
        
        # Convert ke string dan upload ke S3
        data_string = json.dumps(data)
        upload_to_s3(BUCKET_NAME, path, data_string)
        
        print(f"✅ Berhasil upload {data_name} ke s3://{BUCKET_NAME}/{path}")
    else:
        print(f"❌ Gagal mengambil data {data_name}, status code: {response.status_code}")

print("Ekstraksi World Bank selesai!")