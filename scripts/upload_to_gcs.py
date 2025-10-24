from google.cloud import storage
import os

# IMPORTANT: Set your service account key locally before running this script
# Windows: $env:GOOGLE_APPLICATION_CREDENTIALS="path\to\your\key.json"
# Linux/Mac: export GOOGLE_APPLICATION_CREDENTIALS="path/to/your/key.json"

# Check if credentials are set
if not os.environ.get('GOOGLE_APPLICATION_CREDENTIALS'):
    print("Error: GOOGLE_APPLICATION_CREDENTIALS not set")
    print("Set it with: $env:GOOGLE_APPLICATION_CREDENTIALS='path\\to\\key.json'")
    exit(1)

# GCS client initialization
client = storage.Client(project='taxi-streaming-project')
bucket = client.bucket('taxi-streaming-data-bucket')

# File upload
blob = bucket.blob('2024-10-24T10-00-00Z.parquet')
blob.upload_from_filename('2024-10-24T10-00-00Z.parquet')

print(f'✓ File uploaded to gs://taxi-streaming-data-bucket/2024-10-24T10-00-00Z.parquet')
