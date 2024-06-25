import os
import json
# import boto3
from line_utils_commons import logging
from botocore.exceptions import ClientError
from line_utils_commons.clients.boto_factory import BotoFactory

boto_factory = BotoFactory()
logger = logging.getLogger(__name__)

# AWS_ACCESS_KEY_ID = os.environ['SERVICE_EXECUTION_USER_ACCESSKEY']
# AWS_SECRET_ACCESS_KEY = os.environ['SERVICE_EXECUTION_USER_SECRET']

# BUCKET_NAME = os.path.expandvars('$AWS_ACCOUNT_ID-$PARENT_STACKNAME-$ENVIRONMENT-datastore-bucket')


def _get_resource(aws_secrets_config):
    return boto_factory.get_resource('s3', aws_secrets_config=aws_secrets_config)


def _get_client(aws_secrets_config):
    return boto_factory.get_client('s3', aws_secrets_config=aws_secrets_config)

    # if aws_secrets_config:
    #     return boto_factory.get_client('s3', {
    #         'aws_region': aws_secrets_config['aws_region'],
    #         'aws_access_key_id': aws_secrets_config['aws_access_key_id'],
    #         'aws_secret_access_key': aws_secrets_config['aws_secret_access_key']
    #     })
    #     # return boto3.client(
    #     #     's3',
    #     #     region_name=aws_secrets_config['aws_region'],
    #     #     aws_access_key_id=aws_secrets_config['aws_access_key_id'],
    #     #     aws_secret_access_key=aws_secrets_config['aws_secret_access_key']
    #     # )
    # else:
    #     # return boto3.client(
    #     #     "s3",
    #     #     region_name=os.environ['AWS_REGION_ID'],
    #     #     aws_access_key_id=AWS_ACCESS_KEY_ID,
    #     #     aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    #     # )
    #     return boto_factory.get_client('s3')


def get_object(
    bucket_name,
    key,
    aws_secrets_config
):
    # s3_resource = boto3.resource(
    #     's3',
    #     region_name=aws_secrets_config['aws_region'],
    #     aws_access_key_id=aws_secrets_config['aws_access_key_id'],
    #     aws_secret_access_key=aws_secrets_config['aws_secret_access_key']
    # )
    s3_object = _get_resource(aws_secrets_config).Bucket(bucket_name).Object(key=key)
    return s3_object


def get_objects(
    bucket_name,
    prefix,
    aws_secrets_config=None
):
    # s3_resource = boto3.resource(
    #     's3',
    #     region_name=aws_secrets_config['aws_region'],
    #     aws_access_key_id=aws_secrets_config['aws_access_key_id'],
    #     aws_secret_access_key=aws_secrets_config['aws_secret_access_key']
    # )
    bucket = _get_resource(aws_secrets_config).Bucket(bucket_name)
    return bucket.objects.filter(Prefix=prefix)


def delete_all(bucket_names, aws_secrets_config=None):
    # s3_resource = boto3.resource(
    #     's3',
    #     region_name=aws_secrets_config['aws_region'],
    #     aws_access_key_id=aws_secrets_config['aws_access_key_id'],
    #     aws_secret_access_key=aws_secrets_config['aws_secret_access_key']
    # )
    s3_client = _get_client(aws_secrets_config)

    for bucket_name in bucket_names:
        bucket = _get_resource(aws_secrets_config).Bucket(bucket_name)
        bucket.objects.all().delete()
        bucket.object_versions.delete()
        s3_client.delete_bucket(Bucket=bucket_name)


def delete_folder_objects(
    bucket_name,
    prefix,
    aws_secrets_config
):
    # s3_resource = boto3.resource(
    #     's3',
    #     region_name=aws_region,
    #     aws_access_key_id=aws_access_key_id,
    #     aws_secret_access_key=aws_secret_access_key
    # )
    # bucket = s3_resource.Bucket(bucket_name)
    object_list = get_objects(
        bucket_name,
        prefix,
        aws_secrets_config
    )
    for key in object_list:
        key.delete()
        logger.debug(f'Deleted ..{key}')


def delete_folder(
    bucket_name,
    prefix,
    aws_secrets_config
):
    # s3 = boto3.resource('s3')
    # s3_resource = boto3.resource(
    #     's3',
    #     region_name=aws_secrets_config['aws_region'],
    #     aws_access_key_id=aws_secrets_config['aws_access_key_id'],
    #     aws_secret_access_key=aws_secrets_config['aws_secret_access_key']
    # )
    bucket = _get_resource(aws_secrets_config).Bucket(bucket_name)
    for obj in bucket.objects.filter(Prefix=prefix):
        logger.debug(f'Deleted {obj.key}')
        _get_resource(aws_secrets_config).Object(bucket.name, obj.key).delete()


# def get_default_client():
#
#     return s3_client


def list_bucket(aws_secrets_config=None):

    s3_client = _get_client(aws_secrets_config)

    response = s3_client.list_buckets()
    bucket_list = response['Buckets']
    # Output the bucket names
    # logger.debug('Existing buckets:')
    # for bucket in bucket_list:
    #     logger.debug(f'  {bucket["Name"]}')

    return bucket_list


def list_objects(prefix_path, aws_secrets_config=None):

    s3_client = _get_client(aws_secrets_config)

    # my_bucket = s3_client.Bucket(BUCKET_NAME)
    logger.debug(f'{BUCKET_NAME}/{prefix_path} ')
    # response = s3_client.list_objects(Bucket=BUCKET_NAME, Prefix=prefix_path, Delimiter='/')
    keys = []
    response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix_path)
    if 'Contents' in response.keys():
        for obj in response['Contents']:
            keys.append(obj['Key'])
    # for o in result.get('CommonPrefixes'):
    #     logger.debug('sub folder : ', o.get('Prefix'))
    return keys


# def list_dir(prefix='', max_keys=10):
#     """Get all items in a directory.
#     Returns a dictionary with keys: 'files' and 'directories'
#     """
#     # strip leading '/' from the prefix
#     prefix = prefix.lstrip('/')
#     if prefix != '' and prefix[-1] != '/':
#         prefix = prefix + '/'
#
#     # a list of keys/prefixes
#     all_keys = bucket.get_all_keys(
#         max_keys=10, delimiter=delimiter, prefix=prefix
#     )
#
#     out = {
#         'prefix': prefix,
#         'files': [],
#         'directories': []
#     }
#
#     for key in all_keys:
#         if isinstance(key, Prefix):
#             out['directories'].append(key.name)
#         elif isinstance(key, Key) and key.name != prefix:
#             out['files'].append(key.name)
#
#     return out


def create_presigned_url(bucket_name, object_name, expiration=3600):
    """Generate a presigned URL to share an S3 object

    :param bucket_name: string
    :param object_name: string
    :param expiration: Time in seconds for the presigned URL to remain valid
    :return: Presigned URL as string. If error, returns None.
    """

    # Generate a presigned URL for the S3 object
    # s3_client = boto3.client('s3')
    try:
        response = _get_client().generate_presigned_url(
            'get_object',
            Params={'Bucket': bucket_name, 'Key': object_name},
            ExpiresIn=expiration
        )
    except ClientError as e:
        logging.error(e)
        return None

    # The response contains the presigned URL
    return response


def read_json_object(bucket_name, bucket_key):
    body_content = read_object(bucket_name, bucket_key)
    json_data = json.loads(body_content)
    return json_data


def read_object(bucket_name, bucket_key, aws_secrets_config=None):

    s3_client = _get_client(aws_secrets_config)
    s3_object = s3_client.get_object(Bucket=bucket_name, Key=bucket_key)
    body_content = s3_object['Body'].read()
    return body_content


def put_json_object(bucket_name, bucket_key, json_data):
    body_content = json.dumps(json_data)
    put_object(bucket_name, bucket_key, body_content)


def put_object(bucket_name, bucket_key, body_content, aws_secrets_config=None):
    s3_client = _get_client(aws_secrets_config)

    s3_client.put_object(
        Body=body_content,
        Bucket=bucket_name,
        Key=bucket_key
    )


if __name__ == "__main__":

    AWS_ACCESS_KEY_ID = os.environ['SERVICE_EXECUTION_USER_ACCESSKEY']
    AWS_SECRET_ACCESS_KEY = os.environ['SERVICE_EXECUTION_USER_SECRET']
    BUCKET_NAME = os.path.expandvars('$AWS_ACCOUNT_ID-$PARENT_STACKNAME-$ENVIRONMENT-datastore-bucket')
    S3_BUCKET = f's3://{BUCKET_NAME}'

    AWS_SECRETS_CONFIG = {
        'aws_region': os.environ['AWS_REGION_ID'],
        'aws_access_key_id': AWS_ACCESS_KEY_ID,
        'aws_secret_access_key': AWS_SECRET_ACCESS_KEY
    }
    bucket_key = 'files/ohlc_feed/test'
    json_object = {
        'data':[1, 2, 3],
        'status': "Working"
    }

    put_object(BUCKET_NAME, bucket_key, str(json.dumps(json_object)))
    return_data = read_json_object(BUCKET_NAME, bucket_key)
    logger.debug(return_data)
    # objects = get_objects(
    #     BUCKET_NAME,
    #     'results/',
    #     # 'results/tables/',
    #     # 'results/b80ee8a54486e94dcc15d327978b4650/',
    #     AWS_SECRETS_CONFIG
    # )
    # for object in objects:
    #     logger.debug(object)



    # delete_folder_objects(
    #     BUCKET_NAME,
    #     prefix,
    #     # 'results/tables/',
    #     # 'results/b80ee8a54486e94dcc15d327978b4650/',
    #     AWS_SECRETS_CONFIG
    # )
    # prefix = 'files/'
    # delete_folder(
    #     BUCKET_NAME,
    #     prefix,
    #     AWS_SECRETS_CONFIG
    # )
    # prefix = 'tables/'
    # delete_folder(
    #     BUCKET_NAME,
    #     prefix,
    #     AWS_SECRETS_CONFIG
    # )