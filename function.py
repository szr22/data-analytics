import boto3
import json

# S3 bucket we'll be interacting with
s3_bucket = 'random-users-data-214467825313'

# Initialize an boto3 S3 client, and list the objects in our bucket. The data about the contents of our bucket will be stored in a list called s3_keys.
s3 = boto3.client('s3')

def _list_object_keys(s3_bucket):
    objects = s3.list_objects_v2(
            Bucket = s3_bucket
        )['Contents']

    s3_keys = []
    for object in objects:
        if object['Key'].startswith('users_'):
            s3_keys.append(object['Key'])
    return s3_keys

def filter_data(filters):
    empty_filter = []
    for key, value in filters.items():
        if len(value) <= 0:
            empty_filter.append(key)

    for key in empty_filter:
        del filters[key]

    s3_keys = _list_object_keys(s3_bucket)

    filter_string = ''
    for key, value in filters.items():
        key = key.replace('"', '')
        if key == 'dob.age':
            if filter_string:
                filter_string += f' AND s3o.{key}={value}'
            else:
                filter_string = f's3o.{key}={value}'
        else:
            if filter_string:
                filter_string += f' AND s3o.{key}=\'{value}\''
            else:
                filter_string = f's3o.{key}=\'{value}\''
    data = []

    for key in s3_keys:
        response = s3.select_object_content(
                Bucket = s3_bucket,
                Key = key,
                Expression = f'SELECT * FROM S3Object[*][*] as s3o WHERE {filter_string}',
                ExpressionType = 'SQL',
                InputSerialization = {'JSON': {'Type': 'Document'}},
                OutputSerialization = {'JSON': {}}
            )

        for event in response['Payload']:
            if 'Record' in event:
                records = event['Records']['Payload'].decode('utf-8').split('\n')
                for record in records:
                    if record:
                        data.append(json.loads(record))
    return data

def get_data():
    # Because we need to combine data from multiple S3 objects, initialize a list to hold this data before returning it.
    data = []
    s3_keys = _list_object_keys(s3_bucket)

    # After collecting the appropriate keys that begin with "users_" gather each object, and combine the returned data with the existing "data" list.
    for key in s3_keys:
        object = s3.get_object(
                Bucket = s3_bucket,
                Key = key
            )

        object_data = json.loads(object['Body'].read())
        data += object_data

    # Return our combined data from all "users_" objects.
    return data

def handler(event, context):
    # If the events object contains query string parameters call the "filter_data" function and return the results in the appropriate format.
    if 'queryStringParameters' in event.keys():
        return {
            'isBase64Encoded': False,
            'statusCode': 200,
            'body': json.dumps(filter_data(json.loads(event['queryStringParameters']['filters']))),
            'headers': {"Access-Control-Allow-Origin": "*"}
        }
    # Otherwise call the "get_data" function and return appropriately formatted results.
    return {'isBase64Encoded': False,'statusCode': 200,'body': json.dumps(get_data()), 'headers': {"Access-Control-Allow-Origin": "*"}}