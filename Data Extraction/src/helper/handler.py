# import pandas as pd
from pandas import Series, read_csv, DataFrame, notnull
from gc import collect
from helper.log.logging import logger
from helper.utils import get_data, get_df_dtype
from helper.utils import get_path_from_cfg
from helper.utils import get_read_df_cfg

class FileManipulation():
	"""
	The FileManipualtion deals with file and dataframe handling.
		> df: Pandas DataFrame: It's the extracted dataframe from the datasource
	"""
	def __init__(self, cfg_dict=None, df=None, S3=None):
		self.df = DataFrame() if df == None else df
		self.S3=S3

	def get_formatted_df(self, col_cfg):
		"""
		This will do runtime formatting of the data set like adding new column.
		"""
		for col_dict in list(col_cfg['Column']):
			if 'RuntimeCreation' in col_dict.keys():
				logger.debug('Creating new column in position {} at runtime.'\
					.format(col_dict))
				if 'Constant' in col_dict['RuntimeCreation'].keys():
					const=col_dict['RuntimeCreation']['Constant']
					col_pos = int(col_dict['Index'])
					# getting datatype from get_dtype for one column using col_cfg
					dtype=get_df_dtype([col_dict])[1][0]
					new_col_data= Series([const for x in range(len(self.df))],\
						dtype=dtype)
					self.df.insert(col_pos, col_dict['Name'], new_col_data)
					logger.info('New column created - {}'.format(col_dict))
		return self.df

	def get_df_from_raw_data(self, data, cfg={}):
		"""
		Used to generate raw data from data-frame.
		"""
		import io
		logger.info('Config when extracting raw data. {}'.format(str(cfg)))
		if cfg.get('type').lower().strip()=='xlsx':
			logger.info('Functionality not implemented.')
		else:
			df = read_csv( io.BytesIO(data), 
							sep=cfg.get('sep'),
							header=cfg.get('header'),
							names=cfg.get('names'),
							usecols=cfg.get('use_cols'), 
							dtype=cfg.get('dtypes'),
							parse_dates=cfg.get('parse_dates'),
							keep_default_na=True,
							na_values=cfg.get('na_values')
							)
			if cfg.get('header') == None:
				df.columns=[str(i) for i in range(len(df.columns))]
			df = df.where((notnull(df)), None)
			# df.info(memory_usage='deep')
			collect()
		return df

	def get_df_from_S3(self, s3_key, S3, df_cfg=None):
		try:
			raw_data = get_data(path=s3_key, S3=S3)
		except Exception as e:
				logger.warning("Unable to get raw data from IMFT", format(s3_key))
				raise
		self.df = self.get_df_from_raw_data(data=raw_data, cfg=df_cfg)
		del raw_data
		collect()
		# return df

	def get_xml_from_dict(self, dict, indent):
		from dict2xml import dict2xml
		return dict2xml(dict, indent = indent)

	def put_df_to_local(self, path=None, cfg=None):
		"""
		Uses the class dataframe and create file to local directory.
		"""
		if self.df.empty and list(self.df.columns) == []:
			logger.warning('df is None. This will generate empty file')
			self.df = DataFrame()
		if cfg['type'] == 'parquet':
			# TODO: Work on using Paraquet flag
			pass
		else:
			# TODO: Multi character Separator
			# TODO: Give adding quotes to column values
			try:
				self.df.to_csv( path, \
												sep=cfg.get('sep'), \
												header=cfg.get('header'), \
												index=cfg.get('index')) 
				logger.info('Extract df to local.')
			except:
				logger.exception('put_df_to_local: Unable to create file to local.')
				raise 


	def put_footer_to_df(self, cfg):
		if cfg.get('footer') != None:
			self.df.loc[len(self.df)] = cfg['footer']
		return cfg

	def get_src_df_from_db(self, db_cfg):
		from helper.db_conn import Db_Conn
		DB_Conn = Db_Conn(cfg = db_cfg)
		self.df = DB_Conn.get_df_from_db()

	def get_df_from_resultset_CAA(self, cfg):
		from helper.caa_conn import CRDApiAdaptor
		caa = CRDApiAdaptor(config=cfg['CRDAPIAdaptor'])
		self.df = caa.get_resultset_df_from_crd()

	def get_src_df_from_s3_key(self, cfg, S3):
		s3_key = get_path_from_cfg(cfg=cfg['S3'])
		s3_cfg = cfg['S3']
		file_cfg = s3_cfg['FileConfig']
		df_cfg=get_read_df_cfg(input_cfg=cfg)
		self.get_df_from_S3(df_cfg=df_cfg, S3=S3, s3_key=s3_key)