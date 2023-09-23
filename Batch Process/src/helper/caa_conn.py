import json
import requests
from collections import OrderedDict
from helper.log.logging import logger
from helper.utils import get_data, update_env

class CRDApiAdaptor():
  def __init__(self, env=None, config="", df=None, S3=None):
    self.df=df
    self.config=config
    self.S3=S3
    self.ApiUrl=self.get_api_url(config['Environment'])
    self.tkn_str=self.get_auth_token()

  def get_api_url(self, env: str =None) -> str:
    url_domain=update_env(".oms.[ENV].invesco.aws/")
    if env in ['CRGBLUAT', 'CHARLTST', 'CRGBLPRD'] or env == None:
      url_app="https://oms-crdapi-adapter-services"
    else:
      import os 
      env=env+'_CAA'
      url_app='https://'+os.environ[env]
    url=url_app+url_domain
    logger.info('CRD Api Adaptor URL: {}'.format(url))
    return url

  def get_mt_conn(self, env: str = None) -> str:
    from helper.aws_conn import SecretManagerConn
    SM=SecretManagerConn()
    if env:
      return SM.get_sm_dict()[env+'_CAA']
    else:
      return SM.get_sm_dict()['default_CAA']

  def get_auth_token(self) -> str:
    headers = {"Content-Type": "application/json"}
    response = json.loads(requests.post(
        url=self.ApiUrl+"crdapi/adapter/authenticate",
        data=self.get_mt_conn(self.config['Environment']),
        verify=False,
        headers=headers).text)
    if response.get('jwtToken'):
      token = "Bearer "+response.get('jwtToken')
      return token
    else:
      logger.error('Unable to extract jwtToken from CRD API Adaptor.')
      raise(str(response))
  
  def get_api_xml_from_s3(self) -> str:
    path=self.config['XMLLocation']
    caa_xml=get_data(path=path, S3=self.S3)
    return caa_xml
  
  def format_crd_api_xml(self, xml_str):
    col_pos = []
    final_str = ""
    for line in xml_str.splitlines():
      import re
      x = re.findall("\${([^}]+)}", line)
      final_str = final_str+re.sub("\${([^}]+)}", "%s", line)
      col_pos.extend(x)
    return (col_pos, final_str)  

  def get_envolope_tags(self):
    env_attr = []
    if self.config.get('ExecutionType') != None:
      env_attr.append('transaction="'+self.config['ExecutionType']+'"')
    header='<envelope ack="info" '+' '.join(env_attr)+' >'
    footer='</envelope>'
    return (header, footer)

  def get_message_api_xml(self, df=None):
    df = self.df
    caa_xml_str=self.get_api_xml_from_s3().decode('utf-8')
    col_order, final_str = self.format_crd_api_xml(xml_str=caa_xml_str)
    final_xml = ""
    for row in df.itertuples():
      temp=[]
      for rowpos in col_order:
        temp.append(row[int(rowpos)+1])
      temp_tuple = tuple(temp)
      final_xml = final_xml +final_str%temp_tuple
    xml_header, xml_footer=self.get_envolope_tags()
    final_xml = xml_header+final_xml+xml_footer
    return final_xml
  
  def put_data_in_crd(self):
    api_data=self.get_message_api_xml()
    logger.info("API XML: {}".format(str(api_data)))
    headers = {"Content-Type": "application/xml", \
                "Authorization": self.tkn_str, \
                "accept": "application/json"}
    sec_resp = requests.post(
        url=self.ApiUrl+"/crdapi/adapter/xml/message",
        data=api_data,
        verify=False,
        headers=headers)
    resp_json = json.loads(sec_resp.text)
    logger.info('CAA xml/message response: {}'.format(str(resp_json)))
    if resp_json.get('status') != 'SUCCESS' or resp_json.get('status')==None:
      logger.error('CRD API Adaptor Failed: {}'.format(resp_json))
      raise('CRD API Adaptor Failed: {}'.format(resp_json))
  
  def get_resultset_api_json_payload(self):
    return self.config['ResultsetJson']

  def get_resultset_df_from_crd(self):
    api_data=self.get_resultset_api_json_payload()
    logger.info("API XML: {}".format(str(api_data)))
    headers = {"Content-Type": "application/json", \
                "Authorization": self.tkn_str, \
                "accept": "application/json"}
    sec_resp = requests.post(
        url=self.ApiUrl+"/crdapi/adapter/resultset",
        data=api_data,
        verify=False,
        headers=headers)
    # import json
    import pandas as pd
    print(sec_resp.json())
    df = pd.json_normalize(sec_resp.json(), record_path=['data'])
    return df


