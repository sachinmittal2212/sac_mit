"""
Script to add additional setting to lambda function
"""
import json 
import os
import boto3

# For local run 
# dev
# os.environ['AWS_REGION']='us-east-1'
# os.environ['AWS_ACCOUNT_CODE']='782258485841'
# os.environ['AppEnv']='dev'
# os.environ['AWS_ACCESS_KEY_ID']='AKIA3MIR5HJITAWTVX7F'
# os.environ['AWS_SECRET_ACCESS_KEY']=''
function_name = "crd-dataextract"

# uat
# os.environ['AWS_REGION']='us-east-1'
# os.environ['AWS_ACCOUNT_CODE']='342122720323'
# os.environ['AppEnv']='prd'
# os.environ['AWS_ACCESS_KEY_ID']='AKIAU7KA2NRBTNLB4QYC'
# os.environ['AWS_SECRET_ACCESS_KEY']=''

def get_source_arn(policy):
  if policy['Principal'] == "s3.amazonaws.com":
    return "arn:aws:s3:::ivz-"+os.environ['AppEnv']+"-0065-"+policy['SourceArnName']

# Extracting config 
def get_config():
  dir_path = os.path.dirname(os.path.realpath(__file__))
  cfg_path = os.path.join(dir_path, "custom_config.json")
  config = json.load(open(cfg_path))
  return config

def get_lambda_func_arn(name=None):
  func_name = "ivz-"+os.environ['AppEnv']+"-0065-"+name+"-ue1"
  return "arn:aws:lambda:"+os.environ['AWS_REGION']+":"\
  +os.environ['AWS_ACCOUNT_CODE']+":function:"+func_name

def get_lambda_func_name(name=None):
  return "ivz-"+os.environ['AppEnv']+"-0065-"+name+"-ue1"

def get_s3_bucket():
  return 'ivz-'+os.environ['AppEnv']+'-0065-crd-batch-ue1'

def formatted_cfg(config):
  triggers = config.get('S3Trigger')
  policy_list = config.get('Policy')
  env_var=config.get('Environment-Variable').get(os.environ['AppEnv'])
  mem_size=config.get('Memory_Size')
  if not mem_size: 
    mem_size=10240
  return (triggers, policy_list, env_var, mem_size)

def waiting_to_start_updates(lambda_func):
  in_progress=True
  waittime=0
  import time
  while(in_progress and waittime < 30):
    response=lambda_func.get_function(FunctionName=get_lambda_func_name(name=function_name))
    if response['Configuration']['LastUpdateStatus'] != 'Successful':
      time.sleep(5)
      print('.')
      waittime=waittime+1
    else:
      in_progress=False

def main():
  config = get_config()

  (triggers, policy_list, env_var, mem_size)=formatted_cfg(config)

  print(triggers, policy_list, env_var, mem_size)

  cust_cfg = list(config.keys())

  print('Detected {} custom configurations'.format(cust_cfg))

  # Connecting to S3 bucket and lambda function 
  if 'S3Trigger' in cust_cfg:
    s3=boto3.client('s3',
                    aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
                    aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'])
    BucketName = get_s3_bucket()
  lambda_func=boto3.client('lambda')
  # Resourse Names
  LambdaFunctionName = get_lambda_func_arn(name=function_name)

  waiting_to_start_updates(lambda_func)

  if env_var:
    response=lambda_func.update_function_configuration(
      FunctionName=get_lambda_func_name(name=function_name),
      Environment={'Variables':env_var}, 
      MemorySize=mem_size
    )
  # Configuring Lambda's Resource based policy
  print('Getting existing resource based policy of {}..'.format(LambdaFunctionName))
  # response=None

  # Extracting existing resource level policy 
  try:
    response = lambda_func.get_policy(FunctionName=LambdaFunctionName)
  except:
    print('no policy found')
  existing_policy=[]
  if response is not None:
    try:
      for policy in json.loads(response['Policy'])['Statement']:
        existing_policy.append(policy['Sid'])
      print('Found existing policy: {}'.format(existing_policy))
    except:
      print('No policy found')

  # Update the resource level policy
  if policy_list:
    for policy in policy_list:
      # If policy doesnt exist add else ignore it.
      if(policy['StatementID'] not in existing_policy):
        print('Adding new policy {}'.format(policy['StatementID']))
        response = lambda_func.add_permission(
          FunctionName=LambdaFunctionName,
          StatementId=policy['StatementID'],
          Action=policy['Action'],
          Principal=policy['Principal'],
          SourceArn=get_source_arn(policy),
          SourceAccount=os.getenv('AWS_ACCOUNT_CODE')
        )
      else:
        print('{} policy already exist.'.format(policy['StatementID']))


  # Configuring S3 Triggers
  if triggers:
    if triggers.get('LambdaFunctionConfigurations') is not None:
      for data in triggers['LambdaFunctionConfigurations']:
        data['LambdaFunctionArn']=LambdaFunctionName
        location=data['Filter']['Key']['FilterRules'][0]['Value']
        print('Adding trigger to Lambda function Name: "{}" Location: "{}"'\
          .format(data['Id'], location))
    # print('Adding S3 triggers to Lambda functions {} locations.'.format())
    s3.put_bucket_notification_configuration(
        Bucket = BucketName, 
        NotificationConfiguration=triggers
      )

if __name__=="__main__":
  main()