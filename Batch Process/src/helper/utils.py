import os
import re

from pathlib import Path
from helper.config.xml import XMLConfig
from helper.log.logging import logger

# VARIABLES
GBL_REGX = ['[datetime]', '[env]']

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

def NVL(variable, null_variable):
	if variable is None:
		return null_variable
	return variable

def mssql_connection_string(server, database, uname, passwd, others):
	conn_str = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + \
		str(server)+';DATABASE='+str(database)+';UID='+str(uname) + \
		';PWD={'+str(passwd)+"}"+str(others)
	return conn_str


def update_env(text):
	text=text.strip()
	return text.replace('[ENV]', os.environ['ENV'])

def filename_regex(path, cfg={}):
	from datetime import datetime, timedelta
	dateTime = datetime.today()
	if cfg.get('DateOffset'):
		dateTime = dateTime+timedelta(int(cfg.get('DateOffset')))
	date_format = NVL(cfg.get('DateFormat'), "%Y%m%d_%H%M%S_%f")
	date_str = dateTime.strftime(date_format)
	if any(exp in path for exp in GBL_REGX):
		path = path.replace(GBL_REGX[0], date_str)
		path = path.replace(GBL_REGX[1], os.environ['ENV'])
	return path

def get_input_config(cfg_dict=None):
	"""
	Used to set the input congig in case when no input configuration is passed.
	Parameter:
		- cfg_dict: Configuration in the dictionary format.
	"""
	path_list=['crd_batch_interface', os.getenv('APP_NAME'), 'input']
	cfg=cfg_dict.get('InputConfig')
	default_s3_cfg={
			'Location': '/'.join(path_list),
			'FileConfig': { 'FileType': 'csv',
				'FileName': os.getenv('PRC_NAME'),
				'Delimiter': ',',
				'Header': '0',
				'Index': None } }
	if cfg == None:
		cfg={}
		logger.warning('Input config missing from config file.'+
			'Using generic config file.')
		cfg['ModifyExtract'] = 'false'
		cfg['S3'] = default_s3_cfg
	elif cfg.get('S3') == None:
		cfg['S3'] = default_s3_cfg
	logger.debug('Input config found in the config file.')
	input_cfg=cfg
	return input_cfg

def get_xml_from_dict(dict, indent):
	from dict2xml import dict2xml
	return dict2xml(dict, indent=indent)


def get_raw_data_from_local(key):
	df_binary = open(key, "rb")
	return df_binary.read()


def get_cfg_dict_from_xml(config, validate=None, S3=None):
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
	path = path_env(path)
	if environ.get("AWS_EXECUTION_ENV") is not None:
		raw_data = S3.get_raw_data_from_S3(key=path)
		return raw_data
	else:
		if path != None:
			raw_data = open(path, "rb").read()
			return raw_data
		else:
			logger.warning('Path value is none.')
		return None

def get_str_from_byte(byte_str):
	for encode_type in ['ascii', 'utf-8', 'utf-16']:
		try:
			decode_str=byte_str.decode(encode_type)
			return decode_str
		except:
			logger.info('Unable to extract from "'+encode_type+'" encoding.')

def get_regex_fname(filename):
	import re
	if any([exp in filename for exp in GBL_REGX]):
		for expression in GBL_REGX:
			filename = filename.replace(expression, '(.*)')
	return filename


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


def get_path_from_cfg(cfg, unix=False):
	path = cfg['Location']
	fname = cfg['FileConfig']['FileName']
	fext = cfg['FileConfig']['FileType']
	filename = fname+'.'+fext
	file = Path(path, filename)
	return path_env(file, unix)


def get_archive_config(cfg):
	logger.info('Using default archive config')
	if not cfg:
		cfg = {
			"Input": {
			"Offset": 180
			},
			"Output": {
				"Offset": 180
			}
		}
	return cfg
	
def get_list(var):
	if isinstance(var, list):
		return var
	elif var == None:
		return []
	else:
		return [var]

def read_jinja_template(str=None, df=None):
	import jinja2
	template=jinja2.Template(str)
	value = template.render(data=df)
	return value

def get_jinja_temp(S3, path, df=None):
	import jinja2
	temp_byte = S3.get_data(path)
	temp_str = get_str_from_byte(temp_byte)
	template=jinja2.Template(temp_str)
	if df is not None:
		value = template.render(data=df)
	else:
		value	 = template.render()
	return value

def get_input_file_cfg(prc_cfg):
	input_col_cfg, input_file_cfg = None, None
	if prc_cfg.input_cfg.get('ColumnDetails'):
		input_col_cfg = prc_cfg.input_cfg['ColumnDetails']['Column']
	input_file_cfg = prc_cfg.src_cfg['S3']['FileConfig']
	return input_file_cfg, input_col_cfg

def get_output_file_cfg(prc_cfg):
	col_cfg, file_cfg = None, None
	if prc_cfg.output_cfg.get('ColumnDetails'):
		col_cfg = prc_cfg.output_cfg['ColumnDetails']['Column']
	file_cfg = prc_cfg.dest_cfg['S3']['FileConfig']
	return file_cfg, col_cfg

def get_file_cfg(input_cfg: dict):
	if input_cfg:
		col_info = input_cfg.get('ColumnDetails')
		if col_info == None:
			col_cfg=None
		else:
			col_cfg= col_info.get('Column')
		s3_cfg = input_cfg.get('S3')
		if s3_cfg:
			file_cfg = s3_cfg.get('FileConfig') 
		else:
			file_cfg = None
	else:
		return (None, None)
	return (col_cfg, file_cfg)

def get_bool(value):
	if value !=None and str(value).lower()== 'true':
		return True
	else:
		return False

def get_df_na_value(file_cfg):
	if file_cfg.get('NAValues'):
		na_val=file_cfg['NAValue']['Value']
		val=get_list(na_val)
		val.append('')
		return val 
	return ['']

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

def get_write_df_header(file_cfg):
	if file_cfg.get('Header'):
		if file_cfg['Header'] == '0':
			return True
	return False

def get_df_footer(col_cfg):
	col_attr = col_cfg[0].keys()
	if 'Footer' in col_attr:
		return [col['Footer'] for col in col_cfg]
	return None

def get_write_df_cfg(cfg):
	(col_cfg, file_cfg )=get_file_cfg(cfg)
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

def put_extract_to_tmp_folder(FM, cfg):
	df_cfg = get_write_df_cfg(cfg=cfg)
	local_key = get_local_path_from_cfg(cfg=cfg['S3'])
	FM.put_df_to_local( path = local_key, cfg = df_cfg)
	return local_key

def get_insert_stmt(table_name:str, values:dict):
    return f"""INSERT INTO {table_name} \
    ({', '.join(list(values.keys()))}) \
    VALUES \
    ({', '.join(['%s'] * len(values))}) ;"""