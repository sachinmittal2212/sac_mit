from lambda_function import lambda_handler
from helper.custom_logging import LambdaContext

def set_env_var(env=None):
  import os
  print('Updating env variable..')
  os.environ["ENV"]="uat"
  os.environ["sn_account"]="ivz_dev"
  os.environ["AWS_SECRET_MANAGER"]="/0065/uat/crdbatch_secret-8WP3Bj"
  os.environ["AWS_ACCOUNT_CODE"]="166562811506"
  os.environ["region"]="us-east-1"

  # os.environ["ENV"]="prd"
  # os.environ["sn_account"]="ivz_dev"
  # os.environ["AWS_SECRET_MANAGER"]="/0065/prd/crdbatch_secret-EvCEZa"
  # os.environ["acct"]="342122720323"
  # os.environ["region"]="us-east-1"

  # os.environ["ENV"]="dev"
  # os.environ["sn_account"]="ivz_dev"
  # os.environ["AWS_SECRET_MANAGER"]="/0065/dev/crdbatch_secret-rto1Jh"
  # os.environ["AWS_ACCOUNT_CODE"]="782258485841"
  # os.environ['LOG_LEVEL'] = "DEBUG"
  # os.environ['CRD_ASOF_CAA']="oms-crdapi-adapter-craobtst-services"
  # os.environ['IMFT_URL']="securedata-uat.ops.invesco.net"

  os.environ["AWS_REGION"]="us-east-1"

  if env is not None:
    os.environ["AWS_EXECUTION_ENV"] = 'lambda'

event ={
  "Records": [
    {
      "eventVersion": "2.1",
      "eventSource": "testing:s3",
      "awsRegion": "us-east-1",
      "eventTime": "2022-07-18T17:38:05.876Z",
      "eventName": "ObjectCreated:Put",
      "userIdentity": {
        "principalId": "AWS:AROA3MIR5HJITSN7TSJVV:ivz-dev-0065-crd-dataextract-ue1"
      },
      "requestParameters": { "sourceIPAddress": "10.224.2.49" },
      "responseElements": {
        "x-amz-request-id": "676N6D72ADYPVP3S",
        "x-amz-id-2": "alL3tEMBlgHXgkd5ofBNo49NuhBbjHQ+NR38+G/e8BNbo9DZmtAaNopUx7boKvwuJFrfyOEIYnewbgsNwaGgwHsn+NuHCW4s"
      },
      "s3": {
        "s3SchemaVersion": "1.0",
        "configurationId": "1f3c7351-7327-461b-b8e8-2174dc2a98d6",
        "bucket": {
          "name": "ivz-dev-0065-crd-batch-ue1",
          "ownerIdentity": { "principalId": "A2R3W4C0KR77HY" },
          "arn": "arn:aws:s3:::ivz-dev-0065-crd-batch-ue1"
        },
        "object": {
          # "key": "crd_batch_interface/ic/input/ic_crds_deloitte_test_unify_ic.csv",
          # "key": "crd_batch_interface/ic/input/ic_crds_devcpl_s3_INTERNAL_AUDIT_CRD_ASOF_CS_VIOLATION_V2.csv",
          # "key": "crd_batch_interface/ic/input/ic_crds_devcpl_s3_audit-data-extract-pdf-authorization.csv",
          # "key": "crd_batch_interface/ic/input/ic_crds_devcpl_s3_internal_audit_CRD_BROKERS.csv",
          # "key": "crd_batch_interface/ic/input/ic_csv_crds_demo.csv",
          # "key": "crd_batch_interface/ic/input/ic_csv_crds_execute_demo.csv",
          "key": "crd_batch_interface/ic/input/ic_esgc_crd_iss_veiris-rating.csv",
          # "key": "crd_batch_interface/jaws/input/jaws_crds_jaws_csm-code.csv",
          # "key": "crd_batch_interface/ic/input/eq_crds_s3_ts-order-top100.csv",
          # "key": "crd_batch_interface/jaws/input/jaws_crds_jaws_LoadIvzFxHistoricalRateSAAS.csv",
          # "key": "crd_batch_interface/jaws/input/jaws_crds_jaws_LoadCsBrokerSAAS.csv",
          "size": 207,
          "eTag": "c8e36974dbfa3810586cd4f7542597f5",
          "versionId": "tMyblLZ.bwWD5NpJoDamYSlSeVD0J8TY",
          "sequencer": "0062D59A7DC5A00F45"
        }
      }
    }
  ]
}
 
# import os
# from pathlib import Path
# host_file_path = os.path.join(Path().absolute(), 'helper', 'known_hosts')
# print(str(Path(host_file_path).as_posix()))
set_env_var()
lambda_handler(event, context=LambdaContext)