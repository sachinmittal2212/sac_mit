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
    self.LOGGER = 'EXTRACT LAMBDA'
    self.table_name = 'CBF_MESSAGE_LOGS'
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
    # special case of prod in snowflake
    db_cfg['Account']=db_cfg['Account'].replace('prd', 'prod')
    cfg['Snowflake']=db_cfg
    from helper.config.db_config import Database_Config
    db=Database_Config(db_cfg=cfg)
    db.get_cfg()
    return db.cfg_dict

  def init_prc(self, prc_id, trigger):
    log_db = Db_Conn(self.sf_cfg)
    status='PROCESSING'
    log_db.execute_insert_stmt(table = self.table_name, 
                                    PROCESS_ID = prc_id, 
                                    LOGGER = self.LOGGER,
                                    TRIGGER_TYPE = trigger,
                                    RETURN_STATUS = status
                                    )
    cur = log_db.conn.cursor()
    # cur.execute()
    self.PROCESS_ID=prc_id
    get_run_id = "SELECT TOP 1 RUN_ID FROM CBF_MESSAGE_LOGS  WHERE PROCESS_ID= '%s'  AND LOGGER='%s' ORDER BY UPDATE_TIMESTAMP DESC;"
    cur.execute(get_run_id % (self.PROCESS_ID, self.LOGGER))
    self.run_id=list(cur.fetchall())[0][0]
    logger.info('Run Id of the process: {}'.format(self.run_id))
    log_db.close_cursor(cur=cur)

  def format_error(self, error_str):
    alphanum_str=error_str.replace("'", ".")
    alphanum_str=alphanum_str.replace("DROP", "DR0P")
    # alphanum_str = "".join(char for char in error_str if char.isalpha())
    return alphanum_str

# Succuss Status
  def prc_success(self):
    # cur = self.log_db.conn.cursor()
    # TODO: fix I.njestion attack
    log_db = Db_Conn(self.sf_cfg)
    update_stmt="UPDATE "+self.table_name+\
      " SET RETURN_STATUS = 'SUCCESS' , RETURN_MESSAGE = 'SUCCESS' "+ \
      " WHERE RUN_ID = " + str(self.run_id)
      # "(SELECT TOP 1 RUN_ID FROM "+ \
      # self.table_name+" WHERE PROCESS_ID= '"+self.PROCESS_ID+ \
      # "' AND LOGGER='"+self.LOGGER+"' ORDER BY UPDATE_TIMESTAMP DESC)"
    logger.debug('Updating Success: {}'.format(update_stmt))
    log_db.execute_commit_stmt(update_stmt)
    # execute_commit_stmt(update_stmt)

# Failure Status
  def prc_failure(self, error):
    # TODO: fix Injestion attack
    log_db = Db_Conn(self.sf_cfg)
    format_error=self.format_error(str(error))
    if self.run_id is not None:
      update_stmt="UPDATE "+self.table_name+\
        " SET RETURN_STATUS = 'FAILURE' , "+ \
        "RETURN_MESSAGE = '"+ format_error + \
        "' WHERE RUN_ID = (SELECT TOP 1 RUN_ID FROM " + \
        self.table_name+" WHERE PROCESS_ID= '" +self.PROCESS_ID + \
        "' AND LOGGER='"+self.LOGGER+"' ORDER BY UPDATE_TIMESTAMP DESC)"
      logger.info(update_stmt)
      log_db.execute_commit_stmt(update_stmt)
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
