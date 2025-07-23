import boto3
from botocore.exceptions import ClientError

BUCKET_NAME        = "cna-webfiles"
BASE_PREFIX        = "webdata/drillcore/"
LOCAL_FILE_PATH    = "C:/Users/marcus.bourne/Desktop/txttos3/README.txt"
DEST_FILENAME      = "README.txt"
CREDS_FILE         = "creds.txt"


def load_aws_creds(creds_file: str):
    with open(creds_file, "r") as f:
        access_key, secret_key = [part.strip() for part in f.read().strip().split(",", 1)]
    return access_key, secret_key


def get_all_keys(s3_client, bucket: str, prefix: str):
    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
    keys = []
    for page in pages:
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    print(f"DEBUG: Found {len(keys)} objects under prefix '{prefix}'")
    if keys:
        print("DEBUG: Sample keys:")
        for k in keys[:10]:
            print(f"  {k}")
    return keys


def get_subfolders_from_keys(keys: list, prefix: str):
    folders = set()
    for key in keys:
        if key.endswith('/') or not key.startswith(prefix):
            continue
        remainder = key[len(prefix):]
        parts = remainder.split('/', 1)
        if len(parts) > 1 and parts[0]:
            folders.add(prefix + parts[0] + '/')
    return sorted(folders)


def upload_to_each_folder(s3_client, bucket: str, folders: list, local_path: str, dest_name: str):
    for folder in folders:
        key = folder + dest_name
        try:
            s3_client.upload_file(local_path, bucket, key)
            print(f"Uploaded to s3://{bucket}/{key}")
        except ClientError as e:
            print(f"Failed to upload to {bucket}/{key}: {e}")


if __name__ == "__main__":

    access_key, secret_key = load_aws_creds(CREDS_FILE)

    s3 = boto3.client("s3", aws_access_key_id=access_key, aws_secret_access_key=secret_key)

    keys = get_all_keys(s3, BUCKET_NAME, BASE_PREFIX)

    subfolders = get_subfolders_from_keys(keys, BASE_PREFIX)
    print(f"Found {len(subfolders)} folders under '{BASE_PREFIX}'")

    upload_to_each_folder(s3, BUCKET_NAME, subfolders, LOCAL_FILE_PATH, DEST_FILENAME)