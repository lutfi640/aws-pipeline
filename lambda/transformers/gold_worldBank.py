import boto3
import json
import pandas as pd
import io

# ==========================================
# 1. FUNGSI UTILS
# ==========================================
def read_parquet_from_s3(bucket, key):
    s3_client = boto3.client('s3')
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    return pd.read_parquet(io.BytesIO(obj['Body'].read()))

def upload_to_s3(bucket, key, data):
    s3_client = boto3.client('s3')
    s3_client.put_object(Bucket=bucket, Key=key, Body=data)
    print(f"Sukses upload ke s3://{bucket}/{key}")

def list_s3_directories(bucket, prefix):
    """Fungsi pembantu buat nyari folder partisi (year=XXXX) di S3"""
    s3_client = boto3.client('s3')
    paginator = s3_client.get_paginator('list_objects_v2')
    directories = set()
    
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter='/'):
        if 'CommonPrefixes' in page:
            for item in page['CommonPrefixes']:
                directories.add(item['Prefix'])
    return list(directories)

# ==========================================
# 2. LOGIC TRANSFORMATION UTAMA (GOLD LAYER)
# ==========================================
print("Menerima event dinamis dari Airflow:", event)
bucket_name = event.get('bucket', 'learn-aws-imam')

try:
    print("Membaca data tabel dimensi dari layer Silver...")
    dim_country = read_parquet_from_s3(bucket_name, "SILVER/world_bank/dim_country/dim_country.parquet")
    dim_indicator = read_parquet_from_s3(bucket_name, "SILVER/world_bank/dim_indicator/dim_indicator.parquet")

    print("Mencari seluruh partisi tahunan di Fact Table...")
    fact_prefix = "SILVER/world_bank/fact_indicator/"
    partition_dirs = list_s3_directories(bucket_name, fact_prefix)
    
    fact_dfs = []
    
    # Looping semua folder partisi (year=2000, year=2001, dst)
    for folder_path in partition_dirs:
        # Ekstrak angka tahun dari nama folder (misal dari year=2020/ ambil 2020)
        year_str = folder_path.split('=')[-1].replace('/', '')
        silver_fact_path = f"{folder_path}data.parquet"
        
        try:
            df_year = read_parquet_from_s3(bucket_name, silver_fact_path)
            # Masukin kembali kolom year ke dalam dataframe untuk kebutuhan agregasi
            df_year['year'] = int(year_str) 
            fact_dfs.append(df_year)
        except Exception:
            pass

    if not fact_dfs:
        raise ValueError("Tidak ada data di Fact Table sama sekali!")

    df_fact = pd.concat(fact_dfs, ignore_index=True)

    print("Melakukan Join balik (Denormalisasi)...")
    # 1. Join ke tabel Country (kita ambil nama negara dan ISO codenya aja)
    df_merged = df_fact.merge(
        dim_country[['country_id', 'country_name', 'iso3_code']], 
        on='country_id', 
        how='left'
    )
    
    # 2. Join ke tabel Indicator (cuma buat mastiin namanya benar sebelum di-pivot)
    df_merged = df_merged.merge(
        dim_indicator[['indicator_id', 'indicator_code']], 
        on='indicator_id', 
        how='left'
    )
    
    print("Melakukan Pivoting agar metrik menjadi Kolom...")
    # Bikin nama kolomnya rapi: GDP atau Population
    df_merged['metric_name'] = df_merged['indicator_code'].map({
        'NY.GDP.MKTP.CD': 'gdp_usd',
        'SP.POP.TOTL': 'population'
    }).fillna('other_metric')
    
    # Buang kolom ID karena di Data Mart gak dipake lagi
    df_merged.drop(columns=['country_id', 'indicator_id', 'indicator_code'], inplace=True)
    
    # PIVOT MAGIC: Mengubah value metrik menjadi kolom baru
    df_gold = df_merged.pivot_table(
        index=['iso3_code', 'country_name', 'year'], 
        columns='metric_name', 
        values='value', 
        aggfunc='first'
    ).reset_index()

    # Nambahin 1 metrik bisnis baru hasil kalkulasi: GDP per Capita
    if 'gdp_usd' in df_gold.columns and 'population' in df_gold.columns:
        print("Menghitung GDP per Capita...")
        df_gold['gdp_per_capita'] = df_gold['gdp_usd'] / df_gold['population']
        
        # Bersihin nilai inf/nan akibat pembagian nol
        import numpy as np
        df_gold['gdp_per_capita'] = df_gold['gdp_per_capita'].replace([np.inf, -np.inf], np.nan)

    # Pastikan data diurutkan rapi
    df_gold = df_gold.sort_values(by=['country_name', 'year']).reset_index(drop=True)

    # ==========================================
    # UPLOAD KE GOLD LAYER
    # ==========================================
    print(f"Menyimpan data Gold (Total: {len(df_gold)} baris) ke S3...")
    gold_path = "GOLD/world_bank/dm_macroeconomics/data.parquet"
    
    parquet_buffer = io.BytesIO()
    df_gold.to_parquet(parquet_buffer, index=False, engine='pyarrow')
    
    upload_to_s3(bucket_name, gold_path, parquet_buffer.getvalue())
    
    print("🎉 SUKSES! Data Mart World Bank udah jadi dan siap dicolok ke Grafana!")
    
except Exception as e:
    print(f"❌ Error terjadi di task Gold: {str(e)}")
    raise e