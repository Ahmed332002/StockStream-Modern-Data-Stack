import os
import boto3
import snowflake.connector
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from botocore.client import Config
from cosmos import ProjectConfig, ProfileConfig, ExecutionConfig, DbtTaskGroup
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

# setup environment variables for MinIO credentials
MINIO_ENDPOINT = 'http://host.docker.internal:9002' 
BUCKET_NAME = 'bronze-stocks'

def ingest_all_minio_to_snowflake():
    # connect to MinIO
    s3_client = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=os.getenv('MINIO_ROOT_USER', 'minioadmin'),
        aws_secret_access_key=os.getenv('MINIO_ROOT_PASSWORD', 'minioadmin'),
        config=Config(signature_version='s3v4', s3={'addressing_style': 'path'})
    )

    # connect to Snowflake
    conn = snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA')
    )
    cur = conn.cursor()

    try:
        prefix = 'inbox/'
        print(f"🚀 Starting Ingestion Pipeline for Bucket: {BUCKET_NAME}")
        print(f"🔍 Searching for new files in: {prefix}...")
        
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=BUCKET_NAME, Prefix=prefix)

        batch_size = 5000
        current_batch_local_paths = []
        processed_keys = []
        total_found = 0

        for page in pages:
            if 'Contents' not in page:
                continue

            for obj in page['Contents']:
                file_key = obj['Key']
                
                # just a safety check to ensure we only process JSON files in the inbox folder
                if file_key == prefix or not file_key.lower().endswith('.json'):
                    continue

                total_found += 1
                
                # temporarily download the file locally for staging
                local_path = f"/tmp/{os.path.basename(file_key)}"
                s3_client.download_file(BUCKET_NAME, file_key, local_path)
                
                current_batch_local_paths.append(local_path)
                processed_keys.append(file_key)

                # batch upload to Snowflake Stage when batch size is reached
                if len(current_batch_local_paths) >= batch_size:
                    print(f"📤 Batch Ready: Uploading {len(current_batch_local_paths)} files to Snowflake Stage...")
                    cur.execute("PUT 'file:///tmp/*.json' @%STOCKS_RAW_DATA OVERWRITE=TRUE PARALLEL=16")
                    
                    # cleanup local files after upload
                    for f in current_batch_local_paths:
                        if os.path.exists(f): os.remove(f)
                    
                    print(f"✅ Batch Uploaded. Total progress: {total_found} files tracked.")
                    current_batch_local_paths = []

        # upload any remaining files in the last batch
        if current_batch_local_paths:
            print(f"📤 Uploading final batch of {len(current_batch_local_paths)} files...")
            cur.execute("PUT 'file:///tmp/*.json' @%STOCKS_RAW_DATA OVERWRITE=TRUE PARALLEL=16")
            for f in current_batch_local_paths:
                if os.path.exists(f): os.remove(f)
        
        if total_found > 0:
            # copy data from stage to Snowflake table
            print(f"📥 Snowflake Stage is full. Executing COPY INTO for {total_found} files...")
            copy_sql = """
            COPY INTO STOCKS_DB.RAW.STOCKS_RAW_DATA (
                json_data, file_name, file_row_number, ingested_at, batch_id
            )
            FROM (
              SELECT $1, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, CURRENT_TIMESTAMP(), UUID_STRING()
              FROM @%STOCKS_RAW_DATA
            )
            FILE_FORMAT = (TYPE = JSON)
            ON_ERROR = 'CONTINUE';
            """
            cur.execute(copy_sql)
            cur.execute("REMOVE @%STOCKS_RAW_DATA")
            print("✨ Data successfully loaded into STOCKS_RAW_DATA and stage cleared.")

            # track processed files by moving them to an archive folder in MinIO
            print(f"📦 Starting Archiving process for {len(processed_keys)} files...")
            for key in processed_keys:
                archive_key = key.replace('inbox/', 'archive/')
                s3_client.copy_object(
                    Bucket=BUCKET_NAME,
                    CopySource={'Bucket': BUCKET_NAME, 'Key': key},
                    Key=archive_key
                )
                s3_client.delete_object(Bucket=BUCKET_NAME, Key=key)
            
            print(f"⭐ MISSION ACCOMPLISHED: {total_found} files processed and moved to archive.")
        else:
            print("ℹ️ No new files found in 'inbox/'. Nothing to process.")

    except Exception as e:
        print(f"❌ CRITICAL ERROR during ingestion: {str(e)}")
        raise e
    finally:
        cur.close()
        conn.close()
        print("🔌 Database connections closed.")





DBT_PROJECT_PATH = "/usr/local/airflow/dbt_stocks"

DBT_EXECUTABLE_PATH = "/usr/local/bin/dbt"

project_config = ProjectConfig(
    dbt_project_path=DBT_PROJECT_PATH,
)


profile_config = ProfileConfig(
    profile_name="dbt_stocks", 
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_conn", 
        profile_args={
            "schema": "SILVER", 
        },
    ),
)

execution_config = ExecutionConfig(
    dbt_executable_path=DBT_EXECUTABLE_PATH,
)

with DAG(
    "final_optimized_ingestion_v6",
    start_date=datetime(2026, 3, 1),
    ## "*/5 * * * *"
    schedule=None,
    catchup=False,
    tags=['production', 'stocks', 'monitored']
) as dag:

    ingest_task = PythonOperator(
        task_id="ingest_from_minio_to_snowflake",
        python_callable=ingest_all_minio_to_snowflake,
        execution_timeout=None 
    )

    dbt_transform = DbtTaskGroup(
    group_id="dbt_transformation",
    project_config=project_config,
    profile_config=profile_config,
    execution_config=execution_config,
    operator_args={
        "install_deps": True, 
        "select": ["path:models/staging", "path:models/marts"],
        "full_refresh": False,
        },
    )

    ingest_task >> dbt_transform