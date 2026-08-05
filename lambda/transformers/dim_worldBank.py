import boto3
import json
import pandas as pd
import io
from datetime import datetime

# ==========================================
# 1. FUNGSI SAKTI UPSERT (FULL INLINE & NO BOTOCORE)
# ==========================================
def upsert_dimension(df_new, bucket, key, id_prefix, join_col, id_col, zfill_len):
    """Fungsi SAKTI untuk nge-merge dan auto-increment ID dimensi di S3, 
    semua logic S3 digabung (inline) dan tanpa import botocore"""
    s3_client = boto3.client('s3')
    
    # --- 1. CEK & BACA FILE LAMA DARI S3 (TANPA BOTOCORE EXCEPTION) ---
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=key)
    
    file_exists = False
    if 'Contents' in response:
        for item in response['Contents']:
            if item['Key'] == key:
                file_exists = True
                break
                
    if file_exists:
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        df_existing = pd.read_parquet(io.BytesIO(obj['Body'].read()))
    else:
        df_existing = pd.DataFrame()
    
    # --- 2. LOGIC FILTER DATA BARU & AUTO INCREMENT ---
    if df_existing.empty:
        df_new[id_col] = id_prefix + (df_new.index + 1).astype(str).str.zfill(zfill_len)
        df_final = df_new
        is_updated = True
    else:
        existing_keys = df_existing[join_col].tolist()
        df_delta = df_new[~df_new[join_col].isin(existing_keys)].reset_index(drop=True)
        
        if not df_delta.empty:
            max_id = int(df_existing[id_col].str.replace(id_prefix, '').astype(int).max())
            df_delta[id_col] = id_prefix + (df_delta.index + max_id + 1).astype(str).str.zfill(zfill_len)
            df_final = pd.concat([df_existing, df_delta], ignore_index=True)
            is_updated = True
        else:
            df_final = df_existing
            is_updated = False
            
    # --- 3. UPLOAD KE S3 KALAU ADA DATA BARU (INLINED) ---
    if is_updated:
        cols = [id_col] + [c for c in df_final.columns if c != id_col]
        df_to_upload = df_final[cols]
        
        parquet_buffer = io.BytesIO()
        df_to_upload.to_parquet(parquet_buffer, index=False, engine='pyarrow')
        s3_client.put_object(Bucket=bucket, Key=key, Body=parquet_buffer.getvalue())
        print(f"✅ Dimensi Ter-update: {key} (Total Row: {len(df_to_upload)})")
    else:
        print(f"⏭️ Tidak ada data baru untuk {key}. Skip upload.")

# ==========================================
# 2. LOGIC TASK: BUILD DIMENSIONS (LANGSUNG EKSEKUSI)
# ==========================================
print("Menerima event dinamis dari Airflow:", event)

bucket_name = event.get('bucket', 'learn-aws-imam')
# Fallback ke hari ini kalau parameter date gak dikirim
date_str = event.get('date', datetime.now().strftime('%Y-%m-%d'))

# Susun path 2 file JSON yang mau dibaca
key_gdp = f"BRONZE/world_bank/stg_wb_gdp/wb_gdp_data_{date_str}.json"
key_metadata = f"BRONZE/world_bank/stg_wb_indicator_metadata/wb_indicator_metadata_data_{date_str}.json"

try:
    print(f"[TASK: DIM WB] Mulai memproses data tanggal: {date_str} dari bucket: {bucket_name}")
    s3_client_main = boto3.client('s3')
    
    # ---------------------------------------------------------
    # A. BACA JSON BRONZE (GDP) buat ekstrak DIM_COUNTRY
    # ---------------------------------------------------------
    obj_gdp = s3_client_main.get_object(Bucket=bucket_name, Key=key_gdp)
    data_gdp = json.loads(obj_gdp['Body'].read().decode('utf-8'))
    
    # Info: World Bank API naruh datanya di index [1]
    df_gdp = pd.json_normalize(data_gdp[1]) 

    # 1. Siapin Data Unik: DIM_COUNTRY
    df_country = df_gdp[['country.id', 'country.value', 'countryiso3code']].drop_duplicates(subset=['country.id']).dropna(subset=['country.id']).reset_index(drop=True)
    df_country.rename(columns={
        'country.id': 'country_code',      # Bakal jadi join_col (misal: "ID")
        'country.value': 'country_name',   # Misal: "Indonesia"
        'countryiso3code': 'iso3_code'     # Misal: "IDN"
    }, inplace=True)


    # ---------------------------------------------------------
    # B. BACA JSON BRONZE (METADATA) buat ekstrak DIM_INDICATOR
    # ---------------------------------------------------------
    obj_meta = s3_client_main.get_object(Bucket=bucket_name, Key=key_metadata)
    data_meta = json.loads(obj_meta['Body'].read().decode('utf-8'))
    
    df_meta = pd.json_normalize(data_meta[1])

    # 2. Siapin Data Unik: DIM_INDICATOR
    df_indicator = df_meta[['id', 'name', 'sourceNote', 'sourceOrganization']].drop_duplicates(subset=['id']).dropna(subset=['id']).reset_index(drop=True)
    df_indicator.rename(columns={
        'id': 'indicator_code',            # Bakal jadi join_col (misal: "NY.GDP.MKTP.CD")
        'name': 'indicator_name',
        'sourceNote': 'description',
        'sourceOrganization': 'source_org'
    }, inplace=True)


    # ==========================================
    # 3. UPSERT KE S3 SILVER (MERGE INSERT)
    # ==========================================
    # Upsert Tabel Country (Contoh ID: CNT-001)
    upsert_dimension(
        df_new=df_country, 
        bucket=bucket_name, 
        key="SILVER/world_bank/dim_country/dim_country.parquet", 
        id_prefix='CNT-', join_col='country_code', id_col='country_id', zfill_len=3
    )

    # Upsert Tabel Indicator (Contoh ID: IND-001)
    upsert_dimension(
        df_new=df_indicator, 
        bucket=bucket_name, 
        key="SILVER/world_bank/dim_indicator/dim_indicator.parquet", 
        id_prefix='IND-', join_col='indicator_code', id_col='indicator_id', zfill_len=3
    )

    print(f"🎉 SUKSES! 2 Tabel Dimensi World Bank berhasil di-upsert ke Silver.")

except Exception as e:
    print(f"Error di Task DIM World Bank: {str(e)}")
    raise e