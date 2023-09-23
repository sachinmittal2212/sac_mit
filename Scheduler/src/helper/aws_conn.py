import boto3
from shutil import copy
import os
from helper.logging import logger 

class S3Conn():
  def __init__(self, bucket):
    logger.info('Initialing S3 conncetion. Bucket Name: {}'.format(bucket))
    self.bucket=bucket
    self.s3_client=boto3.client('s3')

  def get_raw_data_from_S3(self, key):
    try:
      data=self.s3_client.get_object(Bucket=self.bucket, Key=key)
      data=data['Body']
    except Exception as e:
      error_code = e.response['Error']['Code']
    return data.read()

  def get_file_from_keyword(self, prefix, keyword):
    logger.info('Searching for {} in {}'.format(keyword, prefix))
    objs=self.s3_client.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
    return [obj['Key'] for obj in objs['Contents'] if keyword in obj['Key']]

  def put_file_to_s3(self, S3_key=None, local_key=None):
    print('Transfering the files from {} to {}'.format(S3_key, local_key))
    if os.environ.get("AWS_EXECUTION_ENV") is not None:
      self.s3_client.upload_file(str(S3_key), self.bucket, str(local_key))
    else:
      copy(local_key, S3_key)

  def archive_file(self, pickup_key, drop_key):
    print('Starting archiving from', pickup_key, 'to', drop_key)
    if os.environ.get("AWS_EXECUTION_ENV") is not None:
      copy_src={'Bucket': self.bucket, 'Key':str(pickup_key)}
      self.s3_client.copy(copy_src, self.bucket, str(drop_key))
      print('Deleting file from ', pickup_key)
      self.s3_client.delete_object(Bucket=self.bucket, Key=pickup_key)
    else:
      copy(pickup_key, drop_key)
      os.remove(pickup_key)


class SecretManagerConn():
  def __init__(self, *args):
    if(len(args) > 0):
      self.sm=args[0]
      self.rgn=args[1]
    else:
      self.sm=os.environ['sn_secret_name']
      self.rgn=os.environ['region']
    self.acct=os.environ['acct']
    self.arn='arn:aws:secretsmanager:'+self.rgn+':'+self.acct+':secret:'+self.sm

  def get_sm_dict(self):
    import json
    import base64
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client( service_name='secretsmanager',
        region_name=self.rgn
    )
    print("Using ", self.arn)
    get_secret_value_response = client.get_secret_value(SecretId=self.arn)
    if 'SecretString' in get_secret_value_response:
        secret = get_secret_value_response['SecretString']
    else:
        secret = base64.b64decode(get_secret_value_response['SecretBinary'])
    return json.loads(secret)


class SQSConn():
  def __init__(self, queue_name):
    logger.info('Initializing SQS connection. SQS: {}'.format(queue_name))
    sqs = boto3.resource('sqs')
    self.queue = sqs.get_queue_by_name(QueueName=queue_name)

  def push_msg_to_queue(self, MsgBody):
    try:
      response = self.queue.send_message( MessageBody=MsgBody, MessageGroupId='CRD')
    except:
      print('Unable to send the data to SQS Queue')
    return response