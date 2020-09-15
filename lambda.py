import json
import boto3
import urllib3


def lambda_handler(event, context):

    http = urllib3.PoolManager()
    r = http.request('GET', event['url'])
    s3 = boto3.resource('s3')
    obj = s3.Object(event['bucket_name'], event['object_key'])
    obj.put(Body=r.data)
    return {
        'statusCode': 200,
        'body': json.dumps('Uploaded!')
    }
