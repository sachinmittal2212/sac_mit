from pathlib import Path
import os

from helper.config.prc import Process_Config
from helper.aws_conn import S3Conn
from helper.handler import FileManipulation
from helper.validation import Validation
from helper.validation import Action_on_Violation
from helper.log.db_logger import DB_Logging
from helper.log.logging import logger

from helper.utils import get_local_path_from_cfg
from helper.utils import get_input_file_cfg
from helper.utils import get_list
from helper.utils import path_env
from helper.deliver import Deliver_Extract
from helper.archive import Archive

def get_cfg_name(file_key):
	temp_file_name=str(file_key.stem).split('_')
	return "_".join(temp_file_name[:-1])

def get_cfg_key(file_key):
	cfg_file_name = get_cfg_name(file_key)+'.xml'
	cfg_root_loc = file_key.parent.parent
	return Path(cfg_root_loc, 'config', cfg_file_name)

def get_info_from_event(event):
	prc_info={}
	event_info = event['Records'][0]
	try:
		s3_trig_key = event_info['s3']['object']['key']
		file_key = Path(s3_trig_key)
		prc_info['EVENT_SRC']=event['Records'][0]['eventSource']
		prc_info['PRC_NAME']=get_cfg_name(file_key)
		prc_info['APP_NAME']=s3_trig_key.split('/')[1]
		prc_info['VALID_KEY'] = path_env(Path('crd_batch_interface/validator/validate.xsd'))
		prc_info['CFG_KEY'] = path_env(get_cfg_key(file_key))
		prc_info['INPUT_FILE'] = s3_trig_key
		logger.append_keys(Process=prc_info['PRC_NAME'])
		return prc_info
	except Exception as e:
		logger.exception('Failed to extarct config info from event json.')
		logger.debug('Event JSON : {}'.format(event))
		raise

def get_input_df(pc, S3):
	# input_key=get_path_from_cfg(cfg=pc.src_cfg['S3'])
	file_cfg, col_cfg= get_input_file_cfg(prc_cfg=pc)
	FM=FileManipulation()
	FM.get_df_from_S3(S3=S3, file_key=pc.prc_info['INPUT_FILE'],
										file_cfg=file_cfg, col_cfg=col_cfg)
	return FM

def check_for_extract_in_tmp(pc):
	for s3_target in get_list(pc.dest_cfg['S3']):
		local_key = get_local_path_from_cfg(cfg=s3_target)
		return os.path.exists(local_key)

def get_s3_bucket(prc_info):
	from helper.utils import NVL
	bucket = 'ivz-'+os.environ['ENV']+'-0065-crd-batch-ue1'
	return NVL(os.environ.get(prc_info['APP_NAME'].upper()+'_BUCKET'), bucket)

# @logger.inject_lambda_context()
def lambda_handler(event, context):
	prc_info=get_info_from_event(event=event)
	with DB_Logging( prc_info=prc_info ) as db_logger:
		S3 = S3Conn(get_s3_bucket(prc_info))
		pc = Process_Config(prc_info=prc_info, S3=S3)
		FM=get_input_df(pc, S3)
		FM.df_transformation(pc=pc)
		if pc.valid_cfg:
			validate = Validation(df=FM.df, pc=pc)
			Action_on_Violation(validation=validate, S3=S3)
		Deliver_Extract(pc=pc, FM=FM, S3=S3)
		# input_key=get_path_from_cfg(cfg=pc.src_cfg['S3'])
		arc=Archive(file_key=path_env(prc_info['INPUT_FILE']), S3=S3, cfg=pc.arc_cfg['Input'], dt=False)
		arc.archive()