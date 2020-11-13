import json
import boto3
import urllib3


def lambda_handler(event, context):
    print("Started")

    eos_url = 'https://cernbox.cern.ch/cernbox/webdav/eos/'
    http = urllib3.PoolManager()
    url = eos_url + event['path'] + event['resource']
    header = urllib3.make_headers(basic_auth=event['login']+':'+event['password'])
    print("Starting request")
    r = http.request('GET', url, headers=header)
    print("Finished request, putting into s3")

    s3 = boto3.resource('s3')
    obj = s3.Object(event['bucket_name'], event['object_key'])
    
    obj.put(Body=r.data)
    return {
        'statusCode': 200,
        'body': json.dumps('Uploaded!')
    }
