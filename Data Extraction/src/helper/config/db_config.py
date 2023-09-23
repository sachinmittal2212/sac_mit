from helper.log.logging import logger
from helper.aws.Secret_Manager import SecretManagerConn as SM_Conn
from helper.utils import get_data
from helper.utils import update_env
from helper.utils import get_list

class Database_Config():
	def __init__(self, db_cfg, S3=None) -> None:
		self.cfg_dict=db_cfg
		self.db_type:str=None
		self.S3:object=S3

	def get_cfg(self):
		self.get_db_details()
		self.get_password(self.cfg_dict[self.db_type])
		self.get_sql_query()

	def get_db_details(self):
		if 'Alias' in self.cfg_dict.keys():
			logger.debug('Getting Database Alias from prc config file.')
			alias = self.cfg_dict['Alias']
			db_config=self.get_db_cred_from_generic_xml(alias)
			self.db_type = list(db_config[0].keys())[0]
			self.cfg_dict[self.db_type]=db_config[0][self.db_type]
			logger.info('Alias Extraction completed.')
		else:
			self.db_type=list(self.cfg_dict.keys())[0]

	def get_db_cred_from_generic_xml(self, alias):
		try:
			db_alias_cfg = 'crd_batch_interface/database.xml'
			db_xml_dict={'CFG_KEY':db_alias_cfg, 'VALID_KEY':None}
			cfg_db_dict = self.S3.get_dict_from_xml(prc_info=db_xml_dict)
		except:
			logger.error('Unable to extract db credentials from the database.xml')
			raise
		db_extract = get_list(cfg_db_dict['DBConfig']['Database'])
		db_config = [db for db in db_extract if db['@name']==alias]
		del db_config[0]['@name']
		return db_config

	def get_password(self, config):
		logger.info("Starting Password Retreival")
		if 'Password' in config:
			logger.info("Password stored in file. retriving Password from file")
		else:
			if config['SecretManager'] == None:
				logger.info('No AWS SM mentioned in the cfg. Using default SM.')
				SM = SM_Conn()
			else:
				sm_cfg = config['SecretManager']
				logger.debug(f'Retreiving password from AWS Secret Manager {sm_cfg}')
				SM = SM_Conn(sm_cfg['Name'], sm_cfg['Region'])
			SM_dict = SM.get_sm_dict()
			self.cfg_dict[self.db_type]['Password']=SM_dict[config.get('UserName')]

	def get_sql_query(self):
		if self.cfg_dict.get('Query') is not None:
			logger.info('Database query in config file.')
			self.cfg_dict['Query']=update_env(self.cfg_dict['Query'])
		elif self.cfg_dict.get('QueryS3Location') is not None:
			query_loc=self.cfg_dict['QueryS3Location']
			logger.info('Getting query from S3 {}'.format(query_loc))
			query_byte=get_data(path=query_loc, S3=self.S3)
			from helper.utils import get_str_from_byte
			self.cfg_dict['Query']=get_str_from_byte(query_byte)