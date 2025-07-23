import os
import re
import shutil
from pathlib import Path
import boto3
from botocore.exceptions import ClientError


folder_suffix = "_Mineralogy"
file_pattern = re.compile(r'^[\w\d]+-[\w\d]+-[\w\d]+_DATA\.html$', re.IGNORECASE)
desktop_base = Path.home() / "Desktop" / "Mineralogy_HTMLs" #"where to save / folder name"
creds_file = desktop_base / "creds.txt" #aws credentials file in format "accesskey, secretkey"

if not creds_file.exists():
    raise FileNotFoundError(f"Credentials file not found: {creds_file}")

with open(creds_file) as f:
    access_key, secret_key = [x.strip() for x in f.read().split(",")]

s3 = boto3.client("s3", aws_access_key_id=access_key, aws_secret_access_key=secret_key)
BUCKET_NAME = "cna-webfiles"
S3_BASE_FOLDER = "webdata/drillcore"

def s3_file_exists(bucket, key):
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == "404":
            return False
        raise

def upload_to_s3_if_missing(local_path, filename):
    folder_name = filename.split("_")[0]
    s3_key = f"{S3_BASE_FOLDER}/{folder_name}/{filename}"

    if s3_file_exists(BUCKET_NAME, s3_key):
        print(f"File already exists in s3://{BUCKET_NAME}/{s3_key}\n")
    else:
        try:
            s3.upload_file(str(local_path), BUCKET_NAME, s3_key)
            print(f"Uploaded: {local_path} -> s3://{BUCKET_NAME}/{s3_key}\n")
        except ClientError as e:
            print(f"Upload failed for {local_path}: {e}")

def find_and_upload_only(base_path):
    for root, dirs, files in os.walk(base_path):
        if root.endswith(folder_suffix):
            mineralogy_root = root
            for dirpath, _, filenames in os.walk(mineralogy_root):
                for filename in filenames:
                    if file_pattern.match(filename):
                        src_path = os.path.join(dirpath, filename)

                        # uncomment to save files locally
                        
                        # relative_path = os.path.relpath(src_path, mineralogy_root)
                        # dest_path = desktop_base / Path(mineralogy_root).name / relative_path
                        # dest_path.parent.mkdir(parents=True, exist_ok=True)
                        # try:
                        #     shutil.copy2(src_path, dest_path)
                        #     print(f"\nCopied: {src_path} -> {dest_path}")
                        # except Exception as e:
                        #     print(f"Failed to copy {src_path}: {e}")
                        #     continue

                        upload_to_s3_if_missing(src_path, filename)


drive_letter = input("Enter a drive letter to scan (E): ").strip().upper()
if not drive_letter or len(drive_letter) != 1 or not drive_letter.isalpha():
    print("Invalid input. Please enter a single drive letter (like 'E').")
else:
    external_drive_path = f"{drive_letter}:\\" 
    if os.path.exists(external_drive_path):
        print(f"Scanning {external_drive_path}...\n")
        find_and_upload_only(external_drive_path)
    else:
        print(f"Drive {external_drive_path} does not exist or is not accessible.")
