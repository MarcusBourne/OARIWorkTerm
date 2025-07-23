import os
import boto3
import logging
from botocore.exceptions import ClientError

# Configure logging
def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )


def read_credentials(cred_file_path: str):
    """
    Reads AWS credentials from a file with format:
    access_key, secret_key
    """
    try:
        with open(cred_file_path, 'r') as f:
            line = f.read().strip()
    except FileNotFoundError:
        logging.error(f"Credentials file not found: {cred_file_path}")
        raise

    parts = [p.strip() for p in line.split(',')]
    if len(parts) != 2:
        raise ValueError("Credentials file must contain exactly two values separated by a comma.")
    return parts[0], parts[1]


def list_s3_folders(s3_client, bucket: str, prefix: str):
    """
    Returns all folder names directly under the given prefix in the S3 bucket,
    regardless of how many hyphens or segments they contain.
    """
    paginator = s3_client.get_paginator('list_objects_v2')
    folders = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix + '/', Delimiter='/'):
        for cp in page.get('CommonPrefixes', []):
            folder = cp['Prefix'].rstrip('/').split('/')[-1]
            folders.append(folder)
    return folders


def delete_composites_objects(s3_client, bucket: str, prefix: str):
    """
    Deletes all objects under the given prefix in the specified S3 bucket.
    """
    paginator = s3_client.get_paginator('list_objects_v2')
    total_deleted = 0
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        if 'Contents' not in page:
            continue
        delete_keys = [{'Key': obj['Key']} for obj in page['Contents']]
        try:
            response = s3_client.delete_objects(Bucket=bucket, Delete={'Objects': delete_keys})
            deleted = len(response.get('Deleted', []))
            total_deleted += deleted
            logging.info(f"Deleted {deleted} objects under prefix {prefix}")
        except ClientError as e:
            logging.error(f"Error deleting objects under {prefix}: {e}")
    if total_deleted == 0:
        logging.info(f"No objects found under prefix {prefix} to delete.")


def upload_jpgs_to_composites(s3_client, bucket: str, prefix: str, local_folder: str):
    """
    Uploads all .jpg files in the local folder to the S3 composites prefix.
    """
    count = 0
    for fname in os.listdir(local_folder):
        if not fname.lower().endswith('.jpg'):
            continue
        local_path = os.path.join(local_folder, fname)
        s3_key = f"{prefix}{fname}"
        try:
            s3_client.upload_file(local_path, bucket, s3_key)
            logging.info(f"Uploaded {fname} to s3://{bucket}/{s3_key}")
            count += 1
        except ClientError as e:
            logging.error(f"Failed uploading {fname}: {e}")
    if count == 0:
        logging.warning(f"No .jpg files found in local folder {local_folder}.")


def main():
    setup_logging()

    # === User Configuration ===
    CREDS_FILE = 'creds.txt'  # Path to your credentials file
    LOCAL_BASE_DIR = r'D:\JPGS'  # Local directory containing subfolders
    BUCKET_NAME = 'cna-webfiles'  # S3 bucket name
    S3_BASE_PREFIX = 'webdata/drillcore'  # Base prefix in your bucket
    # ===========================

    # Read AWS credentials
    access_key, secret_key = read_credentials(CREDS_FILE)

    # Initialize S3 client
    s3 = boto3.client(
        's3',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )

    # Fetch all existing folder names under drillcore
    try:
        s3_folders = list_s3_folders(s3, BUCKET_NAME, S3_BASE_PREFIX)
        logging.info(f"Found {len(s3_folders)} folders in S3 under {S3_BASE_PREFIX}")
    except ClientError as e:
        logging.error(f"Unable to list S3 folders: {e}")
        return

    # Process each local subfolder
    for folder_name in os.listdir(LOCAL_BASE_DIR):
        local_folder = os.path.join(LOCAL_BASE_DIR, folder_name)
        if not os.path.isdir(local_folder):
            continue

        # Require an exact match of the full folder name
        if folder_name not in s3_folders:
            logging.warning(f"Skipping '{folder_name}': no matching S3 folder found.")
            continue

        composites_prefix = f"{S3_BASE_PREFIX}/{folder_name}/composites/"
        logging.info(f"Processing local '{folder_name}' -> S3 prefix '{composites_prefix}'")

        # Delete existing composites objects
        delete_composites_objects(s3, BUCKET_NAME, composites_prefix)
        # Upload new .jpg files
        upload_jpgs_to_composites(s3, BUCKET_NAME, composites_prefix, local_folder)


if __name__ == '__main__':
    main()