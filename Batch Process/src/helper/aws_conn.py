import boto3
from shutil import copy
import os
from helper.log.logging import logger
from pathlib import Path
from helper.utils import get_regex_fname, path_env, NVL

class S3Conn():
	def __init__(self, Bucket=None):
		"""
		Establishes connection with S3 Bucket  
		"""
		try:
			self.bucket=Bucket
			self.s3_client=boto3.client('s3')
			logger.info('Initiated S3 conn with bucket: {}'.format(self.bucket))
		except:
			logger.error('Issue will initiating S3 bucket. Please check if {} exist')
			raise

	def get_data(self, path=None):
		if path:
			if os.environ.get("AWS_EXECUTION_ENV") is not None:
				return self.get_raw_data_from_S3(path)
			else:
				raw_data = open(path_env(path), "rb").read()
				return raw_data
		else:
			return None

	def get_dict_from_xml(self, prc_info=None):
		from helper.config.xml import XMLConfig
		(config, validate)=(prc_info['CFG_KEY'], prc_info['VALID_KEY'])
		xml_data=self.get_data(path=config)
		xsd_data=self.get_data(path=validate) if validate else None
		cfg = XMLConfig(xml_data=xml_data, xsd_data=xsd_data)
		cfg_dict = cfg.xml_to_dict()
		return cfg_dict

	def get_raw_data_from_S3(self, key):
		"""
		Get raw data from S3 key
		Parameter:
			- key (String): S3 bucket keep
		"""
		try:
			data=self.s3_client.get_object(Bucket=self.bucket, Key=key)
			data=data['Body']
			logger.info('Extracted raw from {} key: {}'.format(self.bucket, key))
		except Exception as e:
			logger.info('Unable to extract raw data from S3 {} & key {}.\
				Please validate it key and bucket exist.'.format(self.bucket, key))
			raise
		return data.read()

	def get_file_from_keyword(self, prefix, keyword):
		"""
		Search for files with certain keyword in particuler directory.
		Parameter:
			- prefix (String): in which directory you looking 
			- keyword (String): text you searching for.
		"""
		objs=self.s3_client.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
		return [obj['Key'] for obj in objs['Contents'] if keyword in obj['Key']]

	def put_file_to_s3(self, S3_key=None, local_key=None):
		"""
		Transfer the file from local directory to S3 location.
		Parameter:
			- S3_key (String): S3 drop location
			- local_key (String): local pick up location
		"""
		logger.info('Transfering the files from local: {} to S3:{} '\
								.format(local_key, S3_key))
		try:
			if os.environ.get("AWS_EXECUTION_ENV") is not None:
				self.s3_client.upload_file(str(local_key), self.bucket, str(S3_key))
			else:
				copy(local_key, S3_key)
			logger.info('Transfered from {} to {}'.format(local_key, S3_key))
		except Exception as e:
			logger.info('Issue in S3Conn.put_file_to_s3. Error {}'.format(e))
			raise

	def get_s3_path(self, key):
		if self.is_exist(key):
			return key
		else:
			self.s3_client.put_object(Bucket=self.bucket, Key=key)
			return key

	def get_file_ordered_list(self, key):
		"""
		Validates s3 and get the latest file in the location
		"""
		import re
		path = Path(get_regex_fname(key))
		prefix = path_env(path.parent)
		paginator = self.s3_client.get_paginator('list_objects_v2')
		# print(self.bucket)
		page_iterator = paginator.paginate(Bucket=self.bucket, Prefix=prefix)
		# for page in page_iterator:
		# 	all_key = [doc.get('Key') for doc in page.get('Contents')]
		# if not all_key:
		# 	return False
		all_key=[]
		for page in page_iterator:
			for doc in page.get('Contents'):
				all_key.append((doc.get('Key'), doc.get('LastModified')))
		file_keys = []
		# print(all_key)
		for (s3_key, datetime) in all_key:
			if re.match(path.as_posix(), s3_key):
				file_keys.append((s3_key,datetime))
		if file_keys:
			sorted_file_keys= sorted(file_keys, key=lambda x: x[1], reverse=True)
			return sorted_file_keys 
		return False

	def delete_file(self, path):
		logger.info('Deleting {}'.format(path))
		try:
			self.s3_client.delete_object(Bucket=self.bucket, Key=path)
		except:
			logger.warning('Unable to delete file {}'.format(path))


	def get_latest_s3_fpath(self, key):
		"""
		Validates s3 and get the latest file in the location
		"""
		from pathlib import Path
		import re
		path = Path(get_regex_fname(key))
		prefix = path_env(path.parent)
		if os.environ.get("AWS_EXECUTION_ENV") is None:
			all_files = os.listdir(prefix)
			file_list=[ path for files in all_files if re.match(path.name, files)]
			latest_file=max(file_list, key=os.path.getctime) if file_list else False
			return latest_file
		else:
			paginator = self.s3_client.get_paginator('list_objects_v2')
			# print(self.bucket)
			page_iterator = paginator.paginate(Bucket=self.bucket, Prefix=prefix)
			all_key=[]
			for page in page_iterator:
				if page.get('Contents'):
					for doc in page.get('Contents'):
						all_key.append((doc.get('Key'), doc.get('LastModified')))
				else:
					return False
			if not all_key:
				return False
			file_keys = []
			for (key, datetime) in all_key:
				if re.match(path.as_posix(), key):
					file_keys.append((key,datetime))
			if not file_keys:
				logger.info('No files found with key', path)
				return False
			sorted_file_keys= sorted(file_keys, key=lambda x: x[1], reverse=True)
			return sorted_file_keys[0][0]

	def is_exist(self, key: str) -> bool:
		result = self.s3_client.list_objects_v2(Bucket=self.bucket, Prefix=key)
		if 'Content' in result:
			return True
		else:
			return False 

	def last_modified_of_file(self, key: str) -> str:
		if os.environ.get("AWS_EXECUTION_ENV") is not None: 
			obj = self.s3_client.head_object(Bucket=self.bucket, Key=key)
			datetime_value = obj["LastModified"]
			return datetime_value
		else:
			import datetime
			return datetime.datetime.fromtimestamp(os.path.getmtime(key))

	def move_file(self, src_key, dest_key):
		"""
		Moving files in S3 location
		Parameter: 
			- pickup_key (String): Key of the file we need to archive
			- drop_key (String): Key of the file where we have to drop the file
		"""
		logger.info('Moving archiving from {} to {}'.format(src_key, dest_key))
		if os.environ.get("AWS_EXECUTION_ENV") is not None:
			copy_src={'Bucket': self.bucket, 'Key':str(src_key)}
			self.s3_client.copy(copy_src, self.bucket, str(dest_key))
			logger.info('Deleting file from {}'.format(src_key))
			self.s3_client.delete_object(Bucket=self.bucket, Key=src_key)
		else:
			copy(src_key, dest_key)
			os.remove(src_key)

class SecretManagerConn():
	def __init__(self, *args):
		"""
		Establishes secret manager connection.
		Default secret manager: /0065/[env]/crdbatch
		Parameter:
			Region (String): Region in which secret manager is deployed 
			Sm (String): Secret Manager name like '/0065/uat/crdbatch_secret-8WP3Bj'
		"""
		if(len(args) > 0):
			logger.debug('Found AWS secret manager in config.')
			self.sm=args[0]
			self.rgn=args[1]
		else:
			self.sm=os.environ['AWS_SECRET_MANAGER']
			self.rgn=os.environ['AWS_REGION'] or 'us-east-1'
		logger.info('Secret Manager: {}'.format(self.sm))
		self.acct=os.environ['AWS_ACCOUNT_CODE']
		self.arn = 'arn:aws:secretsmanager:'
		self.arn=str(self.arn)+str(self.rgn)+':'+str(self.acct)+':secret:' \
			+str(self.sm)
		try:
			self.sm_client = boto3.client('secretsmanager',
																		region_name=os.environ['AWS_REGION'])
		except:
			logger.exception('Unable to initiated secret manager.')
			raise
		

	def get_sm_dict(self):
		"""
		Get secret value in form of dict
		"""
		import json
		import base64
		# Create a Secrets Manager client
		try:
			get_secret_value_response = self.sm_client.get_secret_value(SecretId=self.arn)
			if 'SecretString' in get_secret_value_response:
					secret = get_secret_value_response['SecretString']
			else:
					secret = base64.b64decode(get_secret_value_response['SecretBinary'])
		except Exception as e:
			logger.error('Unable to extract secret. Check Secret Manager ARN.')
			raise e
		return json.loads(secret)


class SQSConn():
	def __init__(self, queue_name):
		logger.info('Initializing SQS connection. SQS: {}'.format(queue_name))
		sqs = boto3.resource('sqs')
		sqs_client = boto3.client('sqs')
		self.queue = sqs.get_queue_by_name(QueueName=queue_name)

	def push_msg_to_queue(self, MsgBody):
		try:
			response = self.queue.send_message( MessageBody=MsgBody, 
																					MessageGroupId='CRD')
		except:
			logger.exception('Unable to send the data to SQS Queue')
			return False
		return response