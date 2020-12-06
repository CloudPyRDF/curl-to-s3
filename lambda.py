import json

import boto3
import urllib3


def lambda_handler(event, context):
    print("Started")

    eos_url = 'https://cernbox.cern.ch/cernbox/webdav/eos/'
    if event.get("url"):
        eos_url = event['url']
    http = urllib3.PoolManager()
    url = eos_url + event['eos_path'] + event['eos_filename']
    header = urllib3.make_headers(basic_auth=event['eos_login'] + ':' + event['eos_password'])

    print("Starting request")

    # this should stream the response straight to s3 bucket
    s3 = boto3.client('s3')
    result = s3.upload_fileobj(
        Fileobj=http.request('GET', url, headers=header, preload_content=False),
        Bucket=event['s3_bucket_name'],
        Key=event['s3_object_key']
    )
    print(result)
    print("finished")

    return {
        'statusCode': 200,
        'body': json.dumps('Uploaded!')
    }
