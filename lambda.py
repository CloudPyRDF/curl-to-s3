import json
import os
import ssl

import boto3
import urllib3

bucket = os.environ.get('bucket')
eos_login = os.environ.get('eos_login')
eos_password = os.environ.get('eos_password')


def lambda_handler(event, context):
    print("starting with:")
    print(event)
    eos_url = 'https://cernbox.cern.ch/cernbox/webdav/eos/'
    if event.get("url"):
        eos_url = event['url']

    cert_reqs = ssl.CERT_NONE
    urllib3.disable_warnings()

    http = urllib3.PoolManager(cert_reqs=cert_reqs)
    url = f'{eos_url}{event["eos_path"]}{event["eos_filename"]}'
    print(url)
    header = urllib3.make_headers(basic_auth=f'{eos_login}:{eos_password}')
    print(header)

    print("Starting request")

    # this should stream the response straight to s3 bucket
    s3 = boto3.client('s3')
    result = s3.upload_fileobj(
        Fileobj=http.request('GET', url, headers=header, preload_content=False),
        Bucket=bucket, Key=event['s3_object_key']
    )
    print(result)
    print("finished")

    return {
        'statusCode': 200,
        'body': json.dumps('Uploaded!')
    }
