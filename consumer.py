import json
import os
import boto3
from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from dotenv import load_dotenv

load_dotenv(dotenv_path=".env")
# 2. قراءة القيم من الملف
MINIO_USER = os.getenv("MINIO_ROOT_USER")
MINIO_PASS = os.getenv("MINIO_ROOT_PASSWORD")

# 1. إعدادات MinIO
s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9002", # بورت الـ API بتاع MinIO
    aws_access_key_id=MINIO_USER,
    aws_secret_access_key=MINIO_PASS
)
bucket_name = "bronze-stocks"

# تأكد إن الباكت موجود
try:
    s3.head_bucket(Bucket=bucket_name)
except:
    s3.create_bucket(Bucket=bucket_name)

# 2. إعدادات كافكا و Schema Registry
conf = {
    'bootstrap.servers': 'localhost:29092',
    'group.id': 'on-prem-consumer-group',
    'auto.offset.reset': 'earliest',
    'partition.assignment.strategy': 'roundrobin'
}

schema_client = SchemaRegistryClient({'url': 'http://localhost:8081'})
avro_deserializer = AvroDeserializer(schema_client)

consumer = Consumer(conf)
consumer.subscribe(['finnhub_stocks'])

print("🚀 Consumer is running... Waiting for messages...")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None: continue
        if msg.error():
            print(f"❌ Consumer error: {msg.error()}")
            continue

        # فك تشفير البيانات (Value)
        record = avro_deserializer(msg.value(), None)
        
        # --- الـ Pro Move: إضافة الـ Metadata ---
        record["kafka_partition"] = msg.partition()
        record["kafka_offset"] = msg.offset()
        record["ingested_at"] = msg.timestamp()[1] # وقت وصولها لكافكا
        
        # تحويل لـ JSON وحفظه في MinIO
        symbol = record['symbol']
        offset = record['kafka_offset']
        file_path = f"inbox/{symbol}_{offset}.json"
       

        s3.put_object(
            Bucket=bucket_name,
            Key=file_path,
            Body=json.dumps(record)
        )
        print(f"✅ Saved to MinIO: {file_path}")

except KeyboardInterrupt:
    pass
finally:
    consumer.close()