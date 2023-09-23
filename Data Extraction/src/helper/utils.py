import os
import re
from pathlib import Path
from helper.config.xml_reader import XMLConfig
from helper.log.logging import logger

DF_CFG = { 'sep': ',',
				'type': 'csv',
				'index': False,
				'header': None,
				'names': None,
				'use_cols': None,
				'dtypes': {},
				'footer': None,
				'parse_dates': False,
				'na_values': [''] }

def get_df_dtype(col_cfg):
	"""
	Return datatype of column based on the values passed in config file.
	Parameter:
		col_cfg (Dictionary): ColumnDetails (checkout ColumnDetails tag in XML)
	"""
	logger.debug('Retrieve datatype of column.')
	# if col_cfg:
	# 	logger.debug('No column details found. Using input_cfg to get the values')
	# 	if self.input_cfg.get('ColumnDetails'):
	# 		col_cfg = self.input_cfg['ColumnDetails']['Column'] 
	col_pos = 0
	datetime_col = []
	dtype_dict = {}
	if col_cfg is not None:
		for col in col_cfg:
				dtype = col['Datatype'].lower()
				if col['Datatype'] == 'String':
						dtype_dict[col_pos]='object'
				if dtype== 'integer':
						dtype_dict[col_pos] = 'int32'
				if dtype== 'largeinteger':
						dtype_dict[col_pos] = 'int64'
				if dtype == 'float':
						dtype_dict[col_pos] = 'float32'
				if dtype == 'largefloat':
						dtype_dict[col_pos] = 'float64'
				if dtype == 'category':
						dtype_dict[col_pos] = 'category'
				if dtype == 'datetime':
						datetime_col.append(col_pos)
				# if col['Datatype'] == 'sparsestring':
				#     dtype_dict[col_pos]='Sparse[string]'
				col_pos = col_pos+1
	logger.debug('Datetime Columns: {}'.format(datetime_col))
	logger.debug('Datatype of column: {}'.format(dtype_dict))
	return (datetime_col, dtype_dict)

def update_env(text):
	text=text.strip()
	return text.replace('[ENV]', os.environ['ENV'])

def get_cfg_dict_from_xml(config=None, validate=None,  S3=None):
	from os import environ
	xml_data=get_data(path=config, S3=S3)
	if validate != None:
		xsd_data=get_data(path=validate, S3=S3)
	else:
		xsd_data=None
	cfg = XMLConfig(xml_data=xml_data, xsd_data=xsd_data)
	cfg_dict = cfg.xml_to_dict()
	return cfg_dict

def get_data(bucket=None, path=None, S3=None):
	from os import environ
	if environ.get("AWS_EXECUTION_ENV") is not None:
		raw_data = S3.get_raw_data_from_S3(path)
		return raw_data
	else:
		# local_path = Path('../test/')/path
		raw_data = open(path_env(path), "rb").read()
		return raw_data

def get_file_cfg(input_cfg: dict):
	col_info = input_cfg.get('ColumnDetails')
	if col_info == None:
		col_cfg=None
	else:
		col_cfg= col_info.get('Column')
	file_cfg = input_cfg['S3']['FileConfig'] 
	return (col_cfg, file_cfg)

def get_df_na_value(file_cfg):
	if file_cfg.get('NAValues'):
		na_val=file_cfg['NAValue']['Value']
		val=get_list(na_val)
		val.append('')
		return val 
	return ['']

def get_df_header(file_cfg):
	if file_cfg.get('Header'):
		return int(file_cfg['Header'])
	return None

def get_df_footer(col_cfg):
	col_attr = col_cfg[0].keys()
	if 'Footer' in col_attr:
		return [col['Footer'] for col in col_cfg]
	return None

def get_read_df_cfg(input_cfg):
	(col_cfg, file_cfg )=get_file_cfg(input_cfg)
	df_cfg = DF_CFG
	if file_cfg:
		df_cfg['sep'] = file_cfg['Delimiter']
		df_cfg['type'] = file_cfg['FileType']
		df_cfg['index'] = get_bool(file_cfg.get('Index'))
		df_cfg['na_values'] = get_df_na_value(file_cfg=file_cfg)
		df_cfg['header']= get_df_header(file_cfg=file_cfg)
	if col_cfg :
		(df_cfg['parse_dates'], df_cfg['dtypes']) = get_df_dtype(col_cfg=col_cfg)
		df_cfg['names'] = [col['Name'] for col in col_cfg]
		df_cfg['use_cols'] = df_cfg['names']
		df_cfg['footer'] = get_df_footer(col_cfg=col_cfg)
	return df_cfg

def get_save_df_cfg(input_cfg):
	df_cfg=get_read_df_cfg(input_cfg)
	if df_cfg['header']==None:
		df_cfg['header']=False
	elif df_cfg['names'] != None:
		df_cfg['header']=df_cfg['names']
	elif df_cfg['header'] != None:
		df_cfg['header']=True
	return df_cfg

def get_write_df_header(file_cfg):
	if file_cfg.get('Header'):
		if file_cfg['Header'] == '0':
			return True
	return False


def get_write_df_cfg(input_cfg):
	(col_cfg, file_cfg )=get_file_cfg(input_cfg)
	df_cfg = DF_CFG
	if file_cfg:
		df_cfg['sep'] = file_cfg['Delimiter']
		df_cfg['type'] = file_cfg['FileType']
		df_cfg['index'] = get_bool(file_cfg.get('Index'))
		df_cfg['na_values'] = get_df_na_value(file_cfg=file_cfg)
		df_cfg['header']= get_write_df_header(file_cfg=file_cfg)
	if col_cfg :
		(df_cfg['parse_dates'], df_cfg['dtypes']) = get_df_dtype(col_cfg=col_cfg)
		df_cfg['names'] = [col['Name'] for col in col_cfg]
		df_cfg['header']=df_cfg['names']
		df_cfg['use_cols'] = df_cfg['names']
		df_cfg['footer'] = get_df_footer(col_cfg=col_cfg)
	return df_cfg

def get_str_from_byte(byte_str):
	for encode_type in ['ascii', 'utf-8', 'utf-16']:
		try:
			decode_str=byte_str.decode(encode_type)
			return decode_str
		except:
			print('Unable to extract from "'+encode_type+'" encoding.')

def get_bool(value):
	if value !=None and value.lower()== 'true':
		return True
	else:
		return False

def get_list(var):
	if isinstance(var, list):
		return var
	if var == None:
		return []
	else:
		return [var]

def filename_regex(self, f_name, cfg):
	"""
	Function to update the filename based on the expression.
	Suported Exp:
		- [datetime]
	Parameter:
		- f_name: filename
		- cfg: file configuration. Checkout FileConfig in XSD File.
	"""
	DT = '[datetime]'
	from datetime import datetime
	date_str = datetime.today().strftime("_%Y%m%d_%H%M%S_%f")
	if DT in f_name and 'DateFormat' in list(cfg.keys()):
		if cfg.get('DateFormat') is None:
			f_name = f_name.replace(DT, date_str)
		else:
			date_str = datetime.today().strftime(cfg['DateFormat'])
			f_name = f_name.replace(DT, date_str)
	return f_name

def path_env(path, unix=False):
	if unix:
		return str(Path(path).as_posix())
	if os.environ.get("AWS_EXECUTION_ENV") is None:
		if not re.search('..[/\\\\]test',str(path)):
			path = str(Path('..', 'test', path))
		else:
			path = str(Path(path))
		return path
	else:
		return str(Path(path).as_posix())

def get_path_from_cfg(cfg, dateTime=False):
	path = cfg['Location']
	fname = cfg['FileConfig']['FileName']
	fext = cfg['FileConfig']['FileType']
	if dateTime:
		from datetime import datetime
		dt = datetime.utcnow().strftime('_%Y-%m-%d-%H-%M-%S-%f')
		filename = fname+dt+'.'+fext
	else:
		filename=fname+'.'+fext
	file = Path(path, filename)
	return path_env(file)

def get_local_path_from_cfg(cfg):
	fname = cfg['FileConfig']['FileName']
	fext = cfg['FileConfig']['FileType']
	filename = fname+'.'+fext	
	if os.environ.get("AWS_EXECUTION_ENV") is None:
		path = str(Path('..', 'test', 'tmp', filename))
		return path
	else:
		path=Path('/tmp/', filename)
		return path.as_posix()

def get_raw_data_from_local(self, key):
	"""
	USED FOR LOCAL RUN.
	"""
	df_binary = open(key, "rb")
	return df_binary.read()

def NVL(variable, null_variable):
	if variable is None:
		return null_variable
	return variable
