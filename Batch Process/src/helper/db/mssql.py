from helper.log.logging import logger
# from helper.db.database import Database
from numpy import array_split 
from memory_profiler import profile
from snowflake.connector import connect, pandas_tools
import pandas as pd

class Init_DB_conn():
  def __init__(self, conn):
    self.conn=conn
    self.cur = conn.cursor()
    pass

  def __enter__(self):
    return self.cur

  def __exit__(self, exc_type, exc_value, exc_traceback):
    self.conn.commit()
    self.cur.close()

class Snowflake_Database():
  def __init__(self, db_info):
    logger.info('Snowflake Database connection detected.')
    try:
      config=db_info['Snowflake']
      self.uname = config['UserName']
      self.passwd = config['Password']
      self.database = config['Database']
      self.warehouse = config['Warehouse']
      self.role = config['Role']
      self.acct = config['Account']
      self.schema = config['Schema']
      self.query = db_info.get('Query')
    except:
      logger.info('Incomplete Snowflake DB Information: {}'.format(str(config)))
      raise
  
  def connect(self):
    try:
      self.conn = connect( user=self.uname, 
                            password=self.passwd, 
                            account=self.acct, 
                            database=self.database, 
                            role=self.role,
                            warehouse=self.warehouse,
                            schema=self.schema )
    except:
      logger.error('Unable to establish connect with Snowflake DB.')
      raise
  
  def execute_stmt(self, query):
    logger.info('Executing: '+query)
    cursor = self.conn.cursor()
    cursor.execute(query)
    cursor.close()
  
  def get_df_from_db(self):
    cur = self.conn.cursor()
    cur.execute(self.query)
    # df = read_sql(self.query, self.conn)
    df = cur.fetch_pandas_all()
    cur.close()
    return df

  def get_insert_stmt(self, table, col_list):
    col_str = ' ('+','.join(col_list)+')'
    value=" ("+ ','.join('%s' for i in range(len(col_list))) + ") "
    load_sql = "insert into " + table + col_str + " values " + value +';'
    return load_sql
  
  @profile
  def insert_df_to_db(self, df, table, column_list, truncate, update, chuck):
    # Backword Compactiliblity
    if truncate=='true':
      self.truncate_table(table=table)
    if not df.empty:
      insert_sql = self.get_insert_stmt(table, column_list)
      with Init_DB_conn(self.conn) as cur:
        cur.fast_executemany=True
        df=df.astype(object).where(pd.notnull(df), None)
        if chuck != None:
          for data_chuck in array_split(df, chuck):
            i=i+1
            if data_chuck.shape[0]>0:
              cur.executemany(insert_sql, data_chuck.values.tolist())
        else:
          tidy_df = df.astype(object).where(pd.notnull(df), None)
          del df
          cur.executemany(insert_sql, tidy_df[column_list].values.tolist())
    else:
      logger.warning('No data found so no data is loaded to database.')

  def truncate_table(self, table):
    with Init_DB_conn(self.conn) as cur:
      cur.execute("TRUNCATE TABLE "+table+ ";")
