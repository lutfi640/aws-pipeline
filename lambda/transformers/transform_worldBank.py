import boto3
import json
import pandas as pd
import io
from datetime import datetime

# ==========================================
# 1. FUNGSI UTILS (LANGSUNG DITEMPEL DI SINI)
# ==========================================
def read_from_s3(bucket, key):
    s3_client = boto3.client('s3')
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    return obj['Body'].read().decode('utf-8')

def read_parquet_from_s3(bucket, key):
    s3_client = boto3.client('s3')
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    return pd.read_parquet(io.BytesIO(obj['Body'].read()))

def upload_to_s3(bucket, key, data):
    s3_client = boto3.client('s3')
    s3_client.put_object(Bucket=bucket, Key=key, Body=data)
    print(f"Sukses upload ke s3://{bucket}/{key}")

# ==========================================
# 2. LOGIC TRANSFORMATION UTAMA (DINAMIS)
# ==========================================
print("Menerima event dinamis dari Airflow:", event)

# Ambil bucket dan key dari event (Default ke hari ini kalau gak ada)
bucket_name = event.get('bucket', 'learn-aws-imam')
date_str = event.get('date', datetime.now().strftime('%Y-%m-%d'))

print(f"Mulai memproses data World Bank tanggal: {date_str} dari bucket: {bucket_name}")

# Definisi Path Bronze
gdp_key = f"BRONZE/world_bank/stg_wb_gdp/wb_gdp_data_{date_str}.json"
pop_key = f"BRONZE/world_bank/stg_wb_population/wb_population_data_{date_str}.json"

try:
    # ---------------------------------------------------------
    # A. BACA & GABUNGKAN 2 SUMBER JSON
    # ---------------------------------------------------------
    print("Membaca JSON GDP...")
    content_gdp = read_from_s3(bucket_name, gdp_key)
    df_gdp = pd.json_normalize(json.loads(content_gdp)[1])
    
    print("Membaca JSON Populasi...")
    content_pop = read_from_s3(bucket_name, pop_key)
    df_pop = pd.json_normalize(json.loads(content_pop)[1])
    
    # Gabungkan (Union)
    df_raw = pd.concat([df_gdp, df_pop], ignore_index=True)
    
    # Cleansing Awal (Buang null dan betulin tipe data)
    df_fact = df_raw.dropna(subset=['value']).copy()
    df_fact['year'] = df_fact['date'].astype(int)
    df_fact['value'] = df_fact['value'].astype(float)
    
    # ==========================================
    # 3. LOOKUP KE TABEL DIMENSI (MERGE UNTUK AMBIL ID)
    # ==========================================
    print("Membaca data tabel dimensi dari layer Silver...")
    dim_country = read_parquet_from_s3(bucket_name, "SILVER/world_bank/dim_country/dim_country.parquet")
    dim_indicator = read_parquet_from_s3(bucket_name, "SILVER/world_bank/dim_indicator/dim_indicator.parquet")
    
    print("Melakukan Join (Mapping ID)...")
    # Join Country -> dapat country_id (Join key: "ID" = "ID")
    df_fact = df_fact.merge(
        dim_country[['country_id', 'country_code']], 
        how='inner', 
        left_on='country.id', 
        right_on='country_code'
    )
    
    # Join Indicator -> dapat indicator_id (Join key: "NY.GDP.MKTP.CD" = "NY.GDP.MKTP.CD")
    df_fact = df_fact.merge(
        dim_indicator[['indicator_id', 'indicator_code']], 
        how='inner', 
        left_on='indicator.id', 
        right_on='indicator_code'
    )
    
    # Filter dan susun kolom (hanya simpan yang penting, biarkan dimensi di-handle pas query Gold)
    final_columns = ['country_id', 'indicator_id', 'year', 'value']
    df_final = df_fact[final_columns].sort_values(by=['country_id', 'indicator_id', 'year'])

    # ==========================================
    # 4. IMPLEMENTASI DYNAMIC HIVE PARTITIONING 
    # ==========================================
    print("Mulai memecah partisi data berdasarkan year...")
    
    # Pecah DataFrame otomatis berdasarkan tahun pakai Pandas groupby
    for report_year, group_df in df_final.groupby('year'):
        
        # Buang kolom year biar nggak menuhin size file parquet (udah ada di nama folder)
        group_df = group_df.drop(columns=['year'])
        
        # Convert ke Parquet cuma untuk pecahan tahun ini aja
        parquet_buffer = io.BytesIO()
        group_df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
        
        # Format penamaan S3 menggunakan Hive Partitioning
        silver_path = f"SILVER/world_bank/fact_indicator/year={report_year}/data.parquet"
        
        # Upload ke S3
        upload_to_s3(bucket_name, silver_path, parquet_buffer.getvalue())
    
    print(f"✅ SUKSES! Semua data World Bank ter-mapping ke ID dimensi, dan dipartisi per tahun.")
    
except Exception as e:
    print(f"❌ Error terjadi di dalam Lambda Executor (Transform): {str(e)}")
    raise e