import pandas as pd
import os
import gc
from datetime import datetime
from pathlib import Path
from helper.utils import get_input_config, get_archive_config
from helper.log.logging import logger

class FileManipulation():
	"""
	The FileManipualtion deals with file and dataframe handling.
	  > df: Pandas DataFrame: It's the extracted dataframe from the datasource
	"""
	def __init__(self, df=pd.DataFrame()):
		self.df = df

	def add_column_to_df(self, col_dict):
		if 'RuntimeCreation' in col_dict.keys():
			if 'Constant' in col_dict['RuntimeCreation'].keys():
				const_val=col_dict['RuntimeCreation']['Constant']
				new_column_pos = int(col_dict['Index'])
				col_data = [const_val for x in range(len(self.df))]
				from helper.config.df import get_py_dtype
				dtype=get_py_dtype(col_dict['Datatype'])
			new_col_data= pd.Series(col_data, dtype=dtype)
			self.df.insert(new_column_pos, col_dict['Name'], new_col_data)
			logger.info('New column created - {}'.format(col_dict))

	def df_transformation(self, pc):
		"""
		This will do runtime formatting of the data set like adding new column.
		"""
		if str(pc.output_cfg['ModifyExtract']).lower() == 'true':	
			if pc.output_cfg.get('ColumnDetails'):
				col_cfg = pc.output_cfg.get('ColumnDetails')
				for col_dict in list(col_cfg['Column']):
					self.add_column_to_df(col_dict=col_dict)
				self.add_footer_to_df(pc=pc)

	def is_footer_exist(self, pc):
		col_cfg = pc.output_cfg['ColumnDetails']['Column']
		if [col.get('Footer') for col in col_cfg]:
			return True
		return False
	
	def add_footer_to_df(self,pc):
		from helper.utils import get_output_file_cfg
		from helper.config.df import Read_Dataframe_Config as RDC
		if self.is_footer_exist(pc):
			file_cfg, col_cfg= get_output_file_cfg(prc_cfg=pc)
			rdc = RDC(file_cfg=file_cfg, col_cfg=col_cfg)
			df_cfg = rdc.cfg
			if df_cfg.get('footer'):
				self.df.loc[len(self.df)] = df_cfg.get('footer')

	def get_df_from_raw_data(self, data, cfg={}):
		import io
		if cfg.get('type').lower().strip()=='csv':
			try:
				logger.info('Reading csv file from raw_data.')
				self.df = pd.read_csv( io.BytesIO(data), 
												sep=cfg.get('sep'),
												header=cfg.get('header'),
												# names=cfg.get('names'),
												# usecols=cfg.get('use_cols'), 
												dtype=cfg.get('dtypes'),
												# dtype=str,
												parse_dates=cfg.get('parse_dates'),
												engine='c',
												keep_default_na=False,
												na_values=cfg.get('na_values')
												)
				self.df = self.df.where((pd.notnull(self.df)), None)
				gc.collect()
			except Exception as e:
				if type(e).__name__ == 'OverflowError':
					del self.df
					gc.collect()
					log='Error: OverflowError. Trying to convert variables to string.'
					logger.info(log)
					self.df = pd.read_csv( io.BytesIO(data), 
													sep=cfg.get('sep'),
													header=cfg.get('header'),
													# names=cfg.get('use_cols'),
													# usecols=cfg.get('names'), 
													# dtype=cfg.get('dtypes'),
													dtype=str,
													parse_dates=cfg.get('parse_dates'),
													engine='c',
													keep_default_na=False,
													na_values=cfg.get('na_values')
													)
				else:
					raise
				self.df = self.df.where((pd.notnull(self.df)), None)
				gc.collect()

	def get_df_from_S3(self, S3, file_key, file_cfg=None, col_cfg=None):
		"""
		Get the raw data and then convert that raw data into df based on config
		Parameter:
			- S3 (Class Obj): S3 conn to extract raw data from S3
			- file_key (String): S3 file key of data
			- file_cfg (String): ['S3']['FileConfig']
			- col_cfg (String): ['ColumnDetails']['Column']
		"""
		raw_data = S3.get_data(file_key)
		from helper.config.df import Read_Dataframe_Config as RDC
		rdc=RDC(file_cfg=file_cfg, col_cfg=col_cfg)
		df_cfg = rdc.cfg
		self.get_df_from_raw_data(data=raw_data, cfg=df_cfg)
		del raw_data
		gc.collect()

	def put_str_to_local(self, str, local_key):
		"""
		Used to write string to dataframe.
		Parameter:
			- str (String):
			- local_key (String): Drop location 
		"""
		# Loading file from str
		if os.environ.get("AWS_EXECUTION_ENV") is not None:
				# to do 
				with open(local_key.as_posix(), 'w') as sch_xml:
						sch_xml.write(str)
		else:
				with open(local_key, 'w') as sch_xml:
						sch_xml.write(str)
	
	# TODO: Work on using Parquet flag
	def put_df_to_local(self, path=None, cfg=None):
		"""
		Uses the class dataframe and create file to local directory.
		"""
		logger.debug('Transfering df to local')
		if self.df.empty:
			logger.warning('df is None. This will generate empty file')
		if cfg['type'] == 'xls':
			self.put_df_to_xls(path, cfg)
		if cfg['type'] == 'xlsx':
			self.put_df_to_xlsx(path, cfg)
		else:
			self.put_df_to_csv(path, cfg)

	def put_df_to_xls(self, path, cfg):
		self.df.to_excel(path, 
										sheet_name='Sheet1', 
										header=cfg.get('header'),
										index=False)

	def put_df_to_xlsx(self, path, cfg):
		self.df.to_excel(path, 
										sheet_name='Sheet1', 
										header=cfg.get('header'),
										index=False)

	def put_df_to_csv(self, path, cfg):
		# TODO: Multi character Separator
		# TODO: Give adding quotes to column values
		# Why else ? To create delimiter file with non-csv extension  
		try:
			# import csv
			# , quoting=csv.QUOTE_ALL
			self.df.to_csv(path, 
										sep=cfg.get('sep'), 
										header=cfg.get('header'),
										index=False)
			logger.info('Extracted df to local: {}.'.format(path))
		except:
			logger.exception('put_df_to_local: Unable to create file to local.')
			raise 

	def put_file_to_imft(self, imft_cfg=None, src=None, dest=None, \
		username=None, password=None):
		"""
		Used to drop the file to IMFT filcation from local location 
		Parameter:
			- imft_cfg: IMFT details from the configuration file 
			- src: Source location of the file. Usually local 
			- dest: IMFT location from the IMFT root location 
			- username: Username of the IMFT location.
			- password: Password of the IMFT location.
		"""
		# TODO: Make imft_cfg only way to get the configuration.
		logger.info('Starting the IMFT location.')
		if dest == None:
			from helper.utils import get_path_from_cfg
			dest = get_path_from_cfg(cfg=imft_cfg, unix = True)
		from paramiko.client import SSHClient
		from paramiko import AutoAddPolicy
		client = SSHClient()
		# TODO: Get the hostname and port to env variables and function var
		hostname = 'securedata-uat.ops.invesco.net'
		# hostname = 'imft-uat.invesco.com'
		port = 1022
		client.set_missing_host_key_policy(AutoAddPolicy())
		username = imft_cfg['UserName'] if not username else username
		if password is None:
			from helper.aws_conn import SecretManagerConn
			SM = SecretManagerConn()
			secret = SM.get_sm_dict()
			password = secret[username]
		try:
			logger.info('Testing connection with keys')
			import pysftp
			cnopts = pysftp.CnOpts()
			host_file_path = os.path.join(str(Path(__file__).parent),'known_hosts')
			logger.info(host_file_path)
			cnopts.hostkeys.load(host_file_path)
			with pysftp.Connection(host=hostname, username=username, password=password, port=1022, cnopts=cnopts) as sftp:
				logger.info("Connection succesfully established ... ")
		except Exception as e:
			logger.info('PYSFTP is also not working. {}'.format(e))
		client.connect( hostname=hostname, username=username, password=password, port=port, timeout=660, banner_timeout=600, auth_timeout=660)
		with client.open_sftp() as sftp:
			logger.info('Connection with IMFT: {} established!!'.format(username))
			try:
				logger.debug(sftp.listdir())
				sftp.put(src, dest)
			except:
				logger.warning('Some issue with IMFT configuration')
				logger.exception('Unable to deliver file to IMFT')
				raise