import boto3
from shutil import copy
import os
from pathlib import Path
from helper.log.logging import logger
from helper.utils import update_env

class S3Conn():
	def __init__(self, Bucket:str=None):
		"""
		Establishes connection with S3 Bucket  
		Parameters:
			- bucket:S3 bucket name like 'ivz-[env]-0065-crd-batch-ue1'
		"""
		try:
			#TODO: We can do regex bucket name for env specific code 
			self.bucket=Bucket 
			self.s3_client=boto3.client('s3')
			logger.info('Initiated S3 conn with bucket: {}'.format(self.bucket))
		except:
			logger.error('Issue will initiating S3 bucket. Please check if exist')
			raise

	def get_raw_data_from_S3(self, key:str):
		"""
		Get raw data from S3 key
		Parameter:
			- key: S3 bucket keep
		"""
		try:
			logger.debug('Extracting raw from {} key: {}'.format(self.bucket, key))
			data=self.s3_client.get_object(Bucket=self.bucket, Key=key)
			data=data['Body']
			logger.info('Extracted raw from {} key: {}'.format(self.bucket, key))
		except Exception as e:
			# error_code = e.response['Error']['Code']
			logger.exception('Unable to extract raw data from S3 {} & key {}.\
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
		except Exception as e:
			logger.info('Issue in S3Conn.put_file_to_s3. Error {}'.format(e))
			raise

	def archive_file(self, pickup_key, drop_key):
		"""
		Archiving files in S3 location
		Parameter: 
			- pickup_key (String): Key of the file we need to archive
			- drop_key (String): Key of the file where we have to drop the file
		"""
		logger.info('Starting archiving from {} to {}'.format(pickup_key, drop_key))
		if os.environ.get("AWS_EXECUTION_ENV") is not None:
			copy_src={'Bucket': self.bucket, 'Key':str(pickup_key)}
			self.s3_client.copy(copy_src, self.bucket, str(drop_key))
			logger.info('Deleting file from {}'.format(pickup_key))
			self.s3_client.delete_object(Bucket=self.bucket, Key=pickup_key)
		else:
			copy(pickup_key, drop_key)
			os.remove(pickup_key)

	def get_data(self, path=None):
		if os.environ.get("AWS_EXECUTION_ENV") is not None:
			return self.get_raw_data_from_S3(path)
		else:
			local_path = Path('../test/')/path
			# local_path = Path(path)
			raw_data = open(local_path, "rb").read()
			return raw_data

	def get_dict_from_xml(self, prc_info=None):
		from helper.config.xml_reader import XMLConfig
		(config, validate)=(prc_info['CFG_KEY'], prc_info['VALID_KEY'])
		xml_data=self.get_data(path=config)
		xsd_data=self.get_data(path=validate) if validate else None
		cfg = XMLConfig(xml_data=xml_data, xsd_data=xsd_data)
		cfg_dict = cfg.xml_to_dict()
		return cfg_dict