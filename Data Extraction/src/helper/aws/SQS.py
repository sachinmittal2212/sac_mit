class SQSConn():

	def __init__(self, queue_name):
		logger.debug('Initializing SQS connection. SQS: {}'.format(queue_name))
		self.sqs = boto3.client('sqs')
		acct_code = os.environ['AWS_ACCOUNT_CODE']
		sqs_url = 'https://sqs.us-east-1.amazonaws.com/'
		self.sqs_queue_url = sqs_url+acct_code+'/'+queue_name
		logger.info('SQS queue url: {}'.format(self.sqs_queue_url))

	def push_msg_to_queue(self, MsgBody):
		try:
			response = self.sqs.send_message( QueueUrl=self.sqs_queue_url, 
																				MessageBody=MsgBody, 
																				MessageGroupId='CRD', 
																				MessageDeduplicationId='enable')
			logger.info('Response Metadata: {}'.format(response['ResponseMetadata']))
		except:
			logger.exception('Unable to send the data to SQS Queue')
			raise
		return response