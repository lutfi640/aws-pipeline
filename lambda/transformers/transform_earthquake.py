import boto3
import json
import pandas as pd
import io
import uuid
from datetime import datetime
import great_expectations as gx

# ==========================================
# 1. FUNGSI UTILS (OPERASI S3)
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

def save_log_to_s3(bucket, prefix, df):
    """Fungsi khusus untuk nge-save DataFrame Log ke S3 (Format Parquet)"""
    if df.empty:
        return
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine='pyarrow')
    
    # Pakai format Hive partition dan unique ID biar run parallel gak saling timpa
    date_str = datetime.now().strftime('%Y-%m-%d')
    time_str = datetime.now().strftime('%H%M%S')
    file_name = f"{prefix}/dt={date_str}/log_{time_str}_{uuid.uuid4().hex[:6]}.parquet"
    
    upload_to_s3(bucket, file_name, buffer.getvalue())


# ==========================================
# 2. FUNGSI GREAT EXPECTATIONS (DQ & DG)
# ==========================================
def validate_bronze_ingestion(df, dq_logs_list):
    print("Mulai validasi Bronze Data Quality...")
    ge_df = gx.from_pandas(df)

    # Definisikan rules
    checks = [
        ("Column properties.mag exists", ge_df.expect_column_to_exist("properties.mag")),
        ("Column geometry.coordinates exists", ge_df.expect_column_to_exist("geometry.coordinates")),
        ("Column properties.time exists", ge_df.expect_column_to_exist("properties.time"))
    ]
    
    # Evaluasi & catat ke log (Biar kecatat di Parquet sebelum di-Fail)
    for rule_name, res in checks:
        status = 'SUCCESS' if res.success else 'FAILED'
        dq_logs_list.append({
            'layer': 'BRONZE',
            'rule_name': rule_name,
            'status': status,
            'log_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })
        if not res.success:
            raise Exception(f"DQ Error: {rule_name} gagal!") # Circuit breaker aktif!

    print("✅ Bronze Data Quality Validation: LULUS")
    return True

def validate_silver_governance(df, dq_logs_list):
    print("Mulai validasi Silver Data Governance...")
    ge_df = gx.from_pandas(df)

    checks = [
        ("No Nulls in place_id", ge_df.expect_column_values_to_not_be_null("place_id")),
        ("No Nulls in alert_id", ge_df.expect_column_values_to_not_be_null("alert_id")),
        ("No Nulls in type_id", ge_df.expect_column_values_to_not_be_null("type_id")),
        ("Magnitude logic range (-2 to 10)", ge_df.expect_column_values_to_be_between("magnitude", min_value=-2.0, max_value=10.0)),
        ("Longitude valid range", ge_df.expect_column_values_to_be_between("longitude", min_value=-180.0, max_value=180.0)),
        ("Latitude valid range", ge_df.expect_column_values_to_be_between("latitude", min_value=-90.0, max_value=90.0))
    ]
    
    for rule_name, res in checks:
        status = 'SUCCESS' if res.success else 'FAILED'
        dq_logs_list.append({
            'layer': 'SILVER',
            'rule_name': rule_name,
            'status': status,
            'log_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })
        if not res.success:
            raise Exception(f"DG Error: {rule_name} gagal!")
            
    print("✅ Silver Data Governance Validation: LULUS")
    return True


# ==========================================
# 3. LOGIC TRANSFORMATION UTAMA (LAMBDA HANDLER)
# ==========================================
# (Dalam eksekusinya asumsikan event sudah di-pass dari Lambda Handler)
print("Menerima event dinamis dari Airflow:", event)

bucket_name = event.get('bucket', 'learn-aws-imam')
default_key = f"BRONZE/earthquake/stg_earthquake/earthquake_data_{datetime.now().strftime('%Y-%m-%d')}.json"
file_key = event.get('key', default_key)

# 📝 SETUP VARIABEL LOGGING
run_id = event.get('run_id', uuid.uuid4().hex) # Ambil run_id dari airflow atau bikin baru
process_log = {
    'run_id': run_id,
    'project_name': 'Earthquake-Analytics',
    'schema_name': 'earthquake',
    'source_file': file_key,
    'start_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
    'end_time': None,
    'status': 'RUNNING',
    'error_message': 'None'
}
dq_dg_logs = [] # List untuk nampung semua rekaman DQ & DG

try:
    print(f"Mulai memproses file: {file_key} dari bucket: {bucket_name}")

    # --- BACA DATA RAW JSON ---
    content = read_from_s3(bucket_name, file_key)
    data = json.loads(content)
    df = pd.json_normalize(data['features'])

    # 🛑 INJECT: BRONZE DATA QUALITY CHECK (Bawa dq_dg_logs buat dicatat)
    validate_bronze_ingestion(df, dq_dg_logs)

    # --- FLATTEN & CLEANSING ---
    df['properties.time'] = pd.to_datetime(df['properties.time'], unit='ms')
    df['properties.updated'] = pd.to_datetime(df['properties.updated'], unit='ms')
    df = df.loc[:, ['properties.mag', 'properties.place', 'properties.time',
                    'properties.updated', 'properties.alert', 'properties.tsunami', 'properties.sig', 
                    'properties.type', 'geometry.coordinates']]
    
    df['longitude'] = df['geometry.coordinates'].apply(lambda x: x[0] if isinstance(x, list) and len(x) > 0 else None)
    df['latitude'] = df['geometry.coordinates'].apply(lambda x: x[1] if isinstance(x, list) and len(x) > 1 else None)
    df['depth'] = df['geometry.coordinates'].apply(lambda x: x[2] if isinstance(x, list) and len(x) > 2 else None)

    df.drop(columns=['geometry.coordinates'], inplace=True)
    df.rename(columns={'properties.mag': 'magnitude', 'properties.place': 'place', 
                       'properties.time': 'time', 'properties.updated': 'updated',
                       'properties.alert': 'alert', 'properties.tsunami': 'tsunami', 
                       'properties.sig': 'sig', 'properties.type': 'type'}, inplace=True)

    df['alert'] = df['alert'].fillna('unknown')

    # --- LOOKUP KE TABEL DIMENSI (MERGE ID) ---
    print("Mapping ID Dimensi Silver...")
    df_dim_place = read_parquet_from_s3(bucket_name, "SILVER/earthquake/dim_place/dim_place.parquet")
    df_dim_alert = read_parquet_from_s3(bucket_name, "SILVER/earthquake/dim_alert/dim_alert.parquet")
    df_dim_type = read_parquet_from_s3(bucket_name, "SILVER/earthquake/dim_type/dim_type.parquet")
    
    df = df.merge(df_dim_place[['place', 'place_id']], on='place', how='left')
    df = df.merge(df_dim_alert[['alert', 'alert_id']], on='alert', how='left')
    df = df.merge(df_dim_type[['event_type', 'type_id']], left_on='type', right_on='event_type', how='left')
    
    df.drop(columns=['place', 'alert', 'type', 'event_type'], inplace=True)

    # 🛑 INJECT: SILVER DATA GOVERNANCE CHECK
    validate_silver_governance(df, dq_dg_logs)

    # --- DYNAMIC HIVE PARTITIONING ---
    print("Mulai memecah partisi data event_date...")
    df['event_date'] = df['time'].dt.strftime('%Y-%m-%d')

    for event_date, group_df in df.groupby('event_date'):
        group_df = group_df.drop(columns=['event_date'])
        parquet_buffer = io.BytesIO()
        group_df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
        
        silver_path = f"SILVER/earthquake/fact_earthquake/dt={event_date}/data.parquet"
        upload_to_s3(bucket_name, silver_path, parquet_buffer.getvalue())
    
    # Jika sampai sini, proses berarti mulus tanpa crash
    process_log['status'] = 'SUCCESS'
    print("Sukses! Semua data diproses ke Data Warehouse.")
    
except Exception as e:
    # Tangkap pesan error kalau ada yang crash (entah karena code atau karena Validasi GE)
    print(f"Error dalam Lambda: {str(e)}")
    process_log['status'] = 'FAILED'
    process_log['error_message'] = str(e)
    raise e # Lempar balik errornya biar Airflow berubah warna jadi merah

finally:
    # --- BLOCK INI AKAN SELALU DIEKSEKUSI (Bahkan Jika Ada Error) ---
    print("Menulis Data Ops Metadata Logs ke Layer GOLD...")
    process_log['end_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # 1. Simpan Process Log (sys_sync_dw_process)
    df_process = pd.DataFrame([process_log])
    save_log_to_s3(bucket_name, "GOLD/sys/sys_sync_dw_process", df_process)
    
    # 2. Simpan Data Quality & Governance Log (sys_dw_dqdg)
    if dq_dg_logs:
        df_dqdg = pd.DataFrame(dq_dg_logs)
        df_dqdg['run_id'] = run_id # Inject run_id agar bisa di-join dgn process log
        save_log_to_s3(bucket_name, "GOLD/sys/sys_dw_dqdg", df_dqdg)
        
    print("Data Ops Logging Selesai Ditulis ke Parquet!")