from helper.handler import FileManipulation
from helper.config.prc_config import Process_Config
from helper.aws.S3 import S3Conn
from helper.transform import Transformation
from helper.log.db_logger import DB_Logging
from helper.log.logging import logger
from helper.utils import get_save_df_cfg
from helper.utils import get_path_from_cfg, get_local_path_from_cfg
from pathlib import Path
import os
# from memory_profiler import profile
import ast

def get_cfg_name_from_s3_key(cfg_key):
	return str(cfg_key.split('/')[-1]).split('.')[0]

def get_info_from_event(event:dict):
	prc_info={}
	event_info = event['Records'][0]
	cfg_key = list(ast.literal_eval(event_info['body']).keys())[0]
	cfg_key = cfg_key+'.xml'
	cfg_name = get_cfg_name_from_s3_key(cfg_key)
	prc_info['EVENT_SRC']=event_info['eventSource']
	prc_info['PRC_NAME']=cfg_name
	prc_info['APP_NAME']=cfg_key.split('/')[1]
	prc_info['VALID_KEY'] = 'crd_batch_interface/validator/validate.xsd'
	prc_info['CFG_KEY'] = cfg_key
	logger.append_keys(Process=prc_info['PRC_NAME'])
	logger.info('Extract Lambda with {} at {}.'.format( cfg_name, cfg_key))
	return prc_info

def get_src_type(cfg):
	src_keys=cfg.keys()
	return [src for src in list(src_keys) if src not in ['Alias', 'Name']]

def get_extract_from_src(cfg, S3):
	FM = FileManipulation( df=None, S3=S3)
	src_type = get_src_type(cfg=cfg)
	logger.info('Source Detected is: {}'.format(src_type))
	if 'CRDAPIAdaptor' in src_type:
		FM.get_df_from_resultset_CAA(cfg=cfg)
	if 'Database' in src_type:
		FM.get_src_df_from_db(db_cfg=cfg['Database'])
	if 'S3' in src_type:
		FM.get_src_df_from_s3_key(cfg=cfg, S3=S3)
	# TODO: Add alternative Chunking
	# TODO: IMFT as Source
	return FM

def put_extract_to_tmp_folder(FM, pc):
	df_cfg=get_save_df_cfg(input_cfg=pc.input_cfg)
	key=get_local_path_from_cfg(cfg=pc.input_cfg['S3'])
	FM.put_df_to_local( path=key, cfg=df_cfg)
	return key

def get_s3_bucket(prc_info):
	from helper.utils import NVL
	bucket = 'ivz-'+os.environ['ENV']+'-0065-crd-batch-ue1'
	return NVL(os.environ.get(prc_info['APP_NAME'].upper()+'_BUCKET'), bucket)

# @profile
def lambda_handler(event, context):
	prc_info=get_info_from_event(event=event)
	
	with DB_Logging( prc_info=prc_info) as db_logger:
		S3 = S3Conn(Bucket=get_s3_bucket(prc_info))
		pc = Process_Config(prc_info=prc_info, S3=S3)
		FM=get_extract_from_src(cfg=pc.src_cfg, S3=S3)
	
		if pc.trans_cfg:
			tfm = Transformation(FM=FM, pc=pc, S3=S3)
			FM.df=tfm.transform()
	
		if str(pc.input_cfg['ModifyExtract']).lower() == 'true':
			FM.get_formatted_df(col_cfg=pc.input_cfg['ColumnDetails'])
	
		tmp_key=put_extract_to_tmp_folder(FM=FM, pc=pc)
		s3_key = get_path_from_cfg(cfg=pc.input_cfg['S3'], dateTime=True)
		S3.put_file_to_s3(s3_key, tmp_key)