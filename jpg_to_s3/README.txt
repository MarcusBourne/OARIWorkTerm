this script will automatically upload .jpg documents into every drillcore folder in the s3.

LOCAL_BASE_DIR is where the jpgs to be uploaded are located on your drive
creds.txt is you AWS info in the format "accesskey, secret key"
to change file destination update handlers "BUCKET_NAME" and "S3_BASE_PREFIX"
