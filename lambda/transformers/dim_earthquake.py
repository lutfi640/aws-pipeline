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
    # Kita nge-list isi S3 di prefix tersebut buat ngecek filenya udah ada/belum
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=key)
    
    file_exists = False
    if 'Contents' in response:
        for item in response['Contents']:
            if item['Key'] == key:
                file_exists = True
                break
                
    if file_exists:
        # Skenario: File udah ada, baca parquetnya
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        df_existing = pd.read_parquet(io.BytesIO(obj['Body'].read()))
    else:
        # Skenario First Run: File belum ada, kasih dataframe kosong
        df_existing = pd.DataFrame()
    
    # --- 2. LOGIC FILTER DATA BARU & AUTO INCREMENT ---
    if df_existing.empty:
        # Skenario 1: File belum ada (First Run)
        df_new[id_col] = id_prefix + (df_new.index + 1).astype(str).str.zfill(zfill_len)
        df_final = df_new
        is_updated = True
    else:
        # Skenario 2: File udah ada, cari record yang benar-benar BARU
        existing_keys = df_existing[join_col].tolist()
        df_delta = df_new[~df_new[join_col].isin(existing_keys)].reset_index(drop=True)
        
        if not df_delta.empty:
            # Ambil angka ID terakhir buat dilanjutin (misal ambil 10 dari 'PLC-0010')
            max_id = int(df_existing[id_col].str.replace(id_prefix, '').astype(int).max())
            
            # Generate ID buat data baru
            df_delta[id_col] = id_prefix + (df_delta.index + max_id + 1).astype(str).str.zfill(zfill_len)
            
            # Gabungin data lama sama data baru
            df_final = pd.concat([df_existing, df_delta], ignore_index=True)
            is_updated = True
        else:
            df_final = df_existing
            is_updated = False
            
    # --- 3. UPLOAD KE S3 KALAU ADA DATA BARU (INLINED) ---
    if is_updated:
        # Reorder kolom biar ID selalu di depan
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
date_str = datetime.now().strftime('%Y-%m-%d')
default_key = f"BRONZE/earthquake_data_{date_str}.json"
file_key = event.get('key', default_key)

try:
    print(f"[TASK: DIM] Mulai memproses file: {file_key} dari bucket: {bucket_name}")
    
    # --- BACA JSON BRONZE (INLINED) ---
    s3_client_main = boto3.client('s3')
    obj_main = s3_client_main.get_object(Bucket=bucket_name, Key=file_key)
    content = obj_main['Body'].read().decode('utf-8')
    data = json.loads(content)
    
    df_raw = pd.json_normalize(data['features'])

    # 1. Siapin Data Unik Baru: DIM_PLACE
    df_place = df_raw[['properties.place']].drop_duplicates().dropna().reset_index(drop=True)
    df_place.rename(columns={'properties.place': 'place'}, inplace=True)
    df_place['country'] = df_place['place'].apply(lambda x: str(x).split(',')[-1].strip() if ',' in str(x) else str(x).strip())
    
    # 2. Siapin Data Unik Baru: DIM_ALERT
    df_raw['properties.alert'] = df_raw['properties.alert'].fillna('unknown')
    df_alert = pd.DataFrame({'alert': df_raw['properties.alert'].unique()})
    
    # 3. Siapin Data Unik Baru: DIM_TYPE
    df_type = df_raw[['properties.type']].drop_duplicates().dropna().reset_index(drop=True)
    df_type.rename(columns={'properties.type': 'event_type'}, inplace=True)

    # ==========================================
    # 3. UPSERT KE S3 SILVER (MERGE INSERT)
    # ==========================================
    upsert_dimension(
        df_new=df_place, 
        bucket=bucket_name, 
        key="SILVER/earthquake/dim_place/dim_place.parquet", 
        id_prefix='PLC-', join_col='place', id_col='place_id', zfill_len=4
    )

    upsert_dimension(
        df_new=df_alert, 
        bucket=bucket_name, 
        key="SILVER/earthquake/dim_alert/dim_alert.parquet", 
        id_prefix='ALT-', join_col='alert', id_col='alert_id', zfill_len=2
    )

    upsert_dimension(
        df_new=df_type, 
        bucket=bucket_name, 
        key="SILVER/earthquake/dim_type/dim_type.parquet", 
        id_prefix='TYP-', join_col='event_type', id_col='type_id', zfill_len=2
    )

    print(f"🎉 SUKSES! 3 Tabel Dimensi berhasil di-upsert ke Silver.")

except Exception as e:
    print(f"Error di Task DIM: {str(e)}")
    raise e