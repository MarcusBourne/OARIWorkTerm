import os
import re
import boto3
from pathlib import Path
from botocore.exceptions import ClientError

LOCAL_FOLDER = Path("F:\DATA.html files")
CREDENTIALS_FILE = "creds.txt"
BUCKET_NAME = "cna-webfiles"
S3_PREFIX = "webdata/drillcore"

print(f"Starting")
print(f"Local folder to scan: {LOCAL_FOLDER}")
print(f"S3 Bucket: {BUCKET_NAME}\n")

if not os.path.exists(CREDENTIALS_FILE):
    raise FileNotFoundError(f"Credentials file not found: {CREDENTIALS_FILE}")

with open(CREDENTIALS_FILE) as f:
    access_key, secret_key = [x.strip() for x in f.read().split(",")]

s3 = boto3.client("s3", aws_access_key_id=access_key, aws_secret_access_key=secret_key)

file_pattern = re.compile(r'^([\w\d\-]+)_DATA\.html$', re.IGNORECASE)



html_files = list(LOCAL_FOLDER.glob("*.html"))
print(f"Found {len(html_files)} matching files in local folder.")

for file in html_files:
    print(f"Processing file: {file.name}")
    match = file_pattern.match(file.name)
    if not match:
        print(f"Filename does not match pattern: {file.name}")
        continue

    folder_name = match.group(1)
    s3_key = f"{S3_PREFIX}/{folder_name}/{file.name}"

    print(f"Target S3 path: {s3_key}")

    try:
        s3.head_object(Bucket=BUCKET_NAME, Key=s3_key)
        print(f"File already exists in S3: {s3_key}\n")
        continue
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            print(f"Uploading to {s3_key}.")
        else:
            print(f"Failed checking S3 for {s3_key}: {e}")
            continue


    try:
        s3.upload_file(str(file), BUCKET_NAME, s3_key)
        print(f"Successfully uploaded: {s3_key}\n")
    except Exception as e:
        print(f"Failed to upload {file.name}: {e}")

print(f"\nFinished.")