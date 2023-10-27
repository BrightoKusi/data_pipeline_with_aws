import boto3
import redshift_connector 

def create_bucket(access_key, secret_key, bucket_name, region):
    client = boto3.client(
        's3',
        aws_access_key_id= access_key,
        aws_secret_access_key= secret_key
    )

    response = client.create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration={
            'LocationConstraint': region,
        },
    )


