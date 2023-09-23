from helper.log.logging import logger
from helper.utils import get_list
from helper.utils import update_env
from helper.db.snowflake import Snowflake_Database as SF
from helper.aws_conn import SecretManagerConn as SM_Conn

class Database_Connection():

  def __init__(self, db_config, S3) -> None:
    self._config=db_config
    self._db_type=None
    self.S3=S3
    self.get_db_details(self._config)
    self.get_password(self._config[self._db_type])
    self.get_sql_query(self._config)

  def get_db_details(self, cfg):
    if 'Alias' in cfg.keys():
      logger.debug('Getting Database Alias from prc config file.')
      alias = cfg['Alias']
      db_config=self.get_db_cred_from_generic_xml(alias)
      db_type = list(db_config[0].keys())[0]
      cfg[db_type]=db_config[0][db_type]
      logger.info('Alias Extraction completed.')
      self._db_type = db_type
      self._config = cfg
    else:
      self._config=cfg
      self._db_type=list(cfg.keys())[0]

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
      self._config[self._db_type]['Password']=SM_dict[config.get('UserName')]

  def get_sql_query(self, cfg):
    if cfg.get('Query') is not None:
      logger.info('Database query in config file.')
      self._config['Query']=update_env(self.cfg['Query'])
    elif cfg.get('QueryS3Location') is not None:
      query_loc=cfg['QueryS3Location']
      logger.info('Getting query from S3 {}'.format(query_loc))
      self._config['Query']=self.S3.get_data(path=query_loc).decode('ascii')

  def get_conn(self):
    if self._db_type == 'Snowflake':
      return SF(db_info=self._config)
    if self._db_type == 'MSSQLServer':
      logger.info('MS Sql Server Connection Detected.')
    if self._db_type == 'Oracle':
      logger.info('Oracle Connection Detected.')


  def get_db_cred_from_generic_xml(self, alias):
    try:
      from helper.utils import get_cfg_dict_from_xml
      db_alias_cfg = 'crd_batch_interface/database.xml'
      cfg_db_dict = get_cfg_dict_from_xml(config=db_alias_cfg)
    except:
      logger.error('Unable to extract db credentials from the database.xml')
      raise
    db_extract = get_list(cfg_db_dict['DBConfig']['Database'])
    db_config = [db for db in db_extract if db['@name']==alias]
    del db_config[0]['@name']
    return db_config



from abc import ABC, abstractmethod
class Database(ABC):

  @abstractmethod
  def connect(self):
    pass

  @abstractmethod
  def execute_stmt(self, query):
    pass

  @abstractmethod
  def get_df_from_db():
    pass

  @abstractmethod
  def insert_df_to_db():
    pass

  @abstractmethod
  def truncate_table():
    pass

