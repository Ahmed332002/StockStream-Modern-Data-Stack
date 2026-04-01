import finnhub
import json
import boto3
import os
from dotenv import load_dotenv

load_dotenv(dotenv_path=".env")

# إعدادات MinIO
s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9002",
    aws_access_key_id=os.getenv("MINIO_ROOT_USER"),
    aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD")
)
bucket_name = "bronze-stocks"

# setup Finnhub API client
API_KEY = 'd7279r1r01qjeeeg6g2gd7279r1r01qjeeeg6g30'
finnhub_client = finnhub.Client(api_key=API_KEY)

SYMBOLS = ['BINANCE:BTCUSDT', 'BINANCE:ETHUSDT', 'AAPL', 'TSLA', 'GOOGL', 'MSFT', 'AMZN']

def get_companies_metadata():
    companies_data = []
    for symbol in SYMBOLS:
        if ":" in symbol: continue # currently skipping crypto since their metadata structure is different and we want to focus on stocks for this part
            
        try:
            profile = finnhub_client.company_profile2(symbol=symbol)
            if profile and 'name' in profile:
                companies_data.append(profile)
                print(f"✅ Fetched profile for: {symbol}")
        except Exception as e:
            print(f"❌ Error fetching {symbol}: {e}")
            
    # temporary save to local file before uploading to MinIO
    with open('companies_metadata.json', 'w') as f:
        json.dump(companies_data, f, indent=4)
    
    # upload the metadata file to MinIO
    try:
        s3.upload_file('companies_metadata.json', bucket_name, 'metadata/companies_metadata.json')
        print(f"🚀 Metadata uploaded to MinIO: {bucket_name}/metadata/companies_metadata.json")
    except Exception as e:
        print(f"⚠️ Failed to upload to MinIO: {e}")

if __name__ == "__main__":
    get_companies_metadata()