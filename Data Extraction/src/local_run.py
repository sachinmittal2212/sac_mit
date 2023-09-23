
def set_env_var(env=None):
  import os
  # os.environ["ENV"]="uat"
  # os.environ["sn_account"]="ivz_dev"
  # os.environ["AWS_SECRET_MANAGER"]="/0065/uat/crdbatch_secret-8WP3Bj"
  # os.environ["AWS_ACCOUNT_CODE"]="166562811506"

  os.environ["AWS_REGION"]="us-east-1"

  os.environ["ENV"]="dev"
  os.environ["sn_account"]="ivz_dev"
  os.environ["AWS_SECRET_MANAGER"]="/0065/dev/crdbatch_secret-rto1Jh"
  os.environ["AWS_ACCOUNT_CODE"]="782258485841"
  os.environ['LOG_LEVEL'] = "DEBUG"
  os.environ['CRD_SAAS_EQALTS_CAA']="oms-crdapi-adapter-eqalt-services"
  os.environ['IMFT_URL']="securedata-uat.ops.invesco.net"

  # os.environ["ENV"]="prd"
  # os.environ["sn_account"]="ivz_prod"
  # os.environ["AWS_SECRET_MANAGER"]="/0065/prd/crdbatch_secret-EvCEZa"
  # os.environ["AWS_ACCOUNT_CODE"]="342122720323"
  if env is not None:
    os.environ["AWS_EXECUTION_ENV"] = 'lambda'


event={
  "Records": [
    {
      "messageId": "19dd0b57-b21e-4ac1-bd88-01bbb068cb78",
      "receiptHandle": "MessageReceiptHandle",
      # "body": "{'crd_batch_interface/ic/config/ic_esgc_crd_eu-taxonomy': [' other: temp| other0: temp1', '2022-06-03 11:30:00']}",
      # "body": "{'crd_batch_interface/ic/config/ic_crds_devcpl_s3_audit-data-extract-test-group-privilege': [' other: temp| other0: temp1', '2022-06-03 11:30:00']}",
      # "body": "{'crd_batch_interface/jaws/config/jaws_crds_jaws_LoadCSMREGIONLISTMEMBERSAAS': [' other: temp| other0: temp1', '2022-06-03 11:30:00']}",
      "body": "{'crd_batch_interface/jaws/config/jaws_crds_jaws_LoadCsBrokerSAAS': [' other: temp| other0: temp1', '2022-06-03 11:30:00']}",
      # "body": "{'CRD_TO_JAWS': [' other: temp| other1: temp1', '2022-06-03 11:30:00']}",
      "attributes": {
        "ApproximateReceiveCount": "1",
        "SentTimestamp": "1523232000000",
        "SenderId": "123456789012",
        "ApproximateFirstReceiveTimestamp": "1523232000001"
      },
      "messageAttributes": {},
      "md5OfBody": "{{{md5_of_body}}}",
      "eventSource": "aws:sqs",
      "eventSourceARN": "arn:aws:sqs:us-east-1:123456789012:MyQueue",
      "awsRegion": "us-east-1"
    }
  ]
}



set_env_var()
import os
# print(os.environ)
from lambda_function import lambda_handler
lambda_handler(event, None)