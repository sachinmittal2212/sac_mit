import boto3
import os
from helper.log.logging import logger

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
