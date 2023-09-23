from helper.db_conn import Db_Conn
from collections import OrderedDict
from helper.utils import update_env
from helper.log.logging import logger

class DB_Logger():
  def __init__(self):
    self.sf_cfg = self.init_config()

  def init_config(self):
    sf_cfg = OrderedDict()
    sf_cfg=self.set_db_config()
    self.run_id = None
    self.TRIGGER = 'Unknown'
    self.PROCESS_ID = 'Unable to extract process id'
    self.LOGGER = 'BATCH LAMBDA'
    self.table_name = 'CBF_MESSAGE_LOGS'
    logger.debug(sf_cfg)
    return sf_cfg

  def set_db_config(self):
    db_cfg, cfg = OrderedDict(), OrderedDict()
    db_cfg['UserName']=update_env('[ENV]ESGIC_SERVICE_USER').upper()
    db_cfg['SecretManager']=None
    db_cfg['Account']=update_env('ivz_[ENV].us-east-1.privatelink')
    db_cfg['Role']=update_env('CORP-G-APP-SF-[ENV]ESGIC-DEVOPS').upper()
    db_cfg['Schema']='CRDBATCH'
    db_cfg['Database']=update_env('[ENV]ESGIC').upper()
    db_cfg['Warehouse']=update_env('[ENV]_ESG_WH_SR').upper()
    db_cfg['Query']=None
    # special case of prod in snowflake
    db_cfg['Account']=db_cfg['Account'].replace('prd', 'prod')
    cfg['Snowflake']=db_cfg
    from helper.config.db import Database_Config
    db=Database_Config(db_cfg=cfg)
    db.get_cfg()
    return db.cfg_dict

  def get_run_id_sql(self):
    return "SELECT TOP 1 RUN_ID FROM CBF_MESSAGE_LOGS WHERE PROCESS_ID= '%s' \
     AND LOGGER='%s' ORDER BY UPDATE_TIMESTAMP DESC; "


  def init_prc(self, prc_id, trigger):
    log_db = Db_Conn(cfg=self.sf_cfg)
    status = 'PROCESSING'
    insert_dict = { 'PROCESS_ID': prc_id, 'LOGGER' : self.LOGGER, 
                    'TRIGGER_TYPE': trigger, 'RETURN_STATUS': status }
    log_db.execute_insert_stmt( table = self.table_name, values=insert_dict)
    cur = log_db.conn.cursor()
    self.PROCESS_ID=prc_id
    get_run_id = self.get_run_id_sql()
    cur.execute(get_run_id % (self.PROCESS_ID, self.LOGGER), timeout=30)
    self.run_id=list(cur.fetchall())[0][0]
    logger.info('Run Id of the process: {}'.format(self.run_id))
    log_db.close_commit(cur=cur)

  def format_error(self, error_str):
    alphanum_str=error_str.replace("'", "\\'")
    alphanum_str=alphanum_str.replace("DROP", "DR0P")
    # alphanum_str = "".join(char for char in error_str if char.isalpha())
    return alphanum_str

# Succuss Status
  def prc_success(self):
    log_db = Db_Conn(cfg=self.sf_cfg)
    # cur = self.log_db.conn.cursor()
    # TODO: fix Injestion attack
    update_stmt="UPDATE CBF_MESSAGE_LOGS"+\
      " SET RETURN_STATUS = 'SUCCESS' , RETURN_MESSAGE = 'SUCCESS' "+ \
      " WHERE RUN_ID = " + str(self.run_id)
      # "(SELECT TOP 1 RUN_ID FROM "+ \
      # self.table_name+" WHERE PROCESS_ID= '"+self.PROCESS_ID+ \
      # "' AND LOGGER='"+self.LOGGER+"' ORDER BY UPDATE_TIMESTAMP DESC)"
    logger.debug('Updating Success: {}'.format(update_stmt))
    try_again = True
    import time
    while(try_again):
      try:
        log_db.execute_stmt(update_stmt) 
        try_again=False
      except:
        logger.info('Unable to commit to Database.Trying again in 5..')
        try_again=True
        time.sleep(5)
      # execute_commit_stmt(update_stmt)

# Failure Status
  def prc_failure(self, error):
    # TODO: fix Injestion attack
    log_db = Db_Conn(cfg=self.sf_cfg)
    format_error=self.format_error(str(error))
    if self.run_id is not None:
      update_stmt="UPDATE CBF_MESSAGE_LOGS"+\
        " SET RETURN_STATUS = 'FAILURE' , "+ \
        "RETURN_MESSAGE = '"+ format_error + \
        "' WHERE RUN_ID = (SELECT TOP 1 RUN_ID FROM " + \
        self.table_name+" WHERE PROCESS_ID= '" +self.PROCESS_ID + \
        "' AND LOGGER='"+self.LOGGER+"' ORDER BY UPDATE_TIMESTAMP DESC)"
      try_again=True
      while(try_again):
        try:
          log_db.execute_stmt(update_stmt)
          try_again=False
        except:
          try_again=True
    else:
      log_db.execute_insert_stmt(table = self.table_name, 
                                      PROCESS_ID = self.PROCESS_ID, 
                                      LOGGER = self.LOGGER,
                                      RETURN_MESSAGE= str(error),
                                      TRIGGER_TYPE = self.TRIGGER,
                                      RETURN_STATUS = 'FAILURE' )
    
class DB_Logging():
  def __init__(self, prc_info) -> None:
    try:
      self.db = DB_Logger()
    except:
      logger.exception('Unable to initiate Db logger.') 
      raise
    self.db.init_prc(prc_info['PRC_NAME'], prc_info['EVENT_SRC'])

  def __enter__(self):
    return self.db
 
  def __exit__(self, type, value, tb):
    if type != None:
      import traceback 
      error_json = {'type': type, 'value':value, 'traceback':traceback.print_tb(tb)}
      self.db.prc_failure(str(error_json))
      raise
    else:
      self.db.prc_success()