import pandas as pd
import os
import numpy as np
from pathlib import Path
from helper.log.logging import logger
# from memory_profiler import profile

class Db_Conn():
	def __init__(self, cfg=None):
		if cfg:
			self.cfg=cfg
			self.db_type=list(cfg.keys())[0]
			self.config = self.cfg[self.db_type]
			self.establish_db_conn(db_type=self.db_type)

	def establish_db_conn(self, db_type):
		if db_type == 'Snowflake':
			self.snowflake_conn()
		elif db_type == 'MSSQLServer':
			self.mssql_conn()
		elif db_type == 'Oracle':
			self.oracle_conn()
		else:
			raise('Unidentified Database')

	def mssql_conn(self):
		from pyodbc import connect
		logger.debug('MS-SQL Server connection detected.')
		self.uname = self.config.get('UserName')
		self.passwd = self.config.get('Password')
		self.server = self.config.get('Server')
		self.driver = self.config.get('Driver')
		self.database = self.config.get('Database')
		def_driver = 'ODBC Driver 17 for SQL Server'
		self.driver = def_driver if not self.driver else self.driver
		if 'Others' in list(self.config.keys()):
			self.others = ';'+self.config.get('Others')
		else:
			self.others = ';'
		connection_string = self.mssql_connection_string()
		try:
			with connect(connection_string, timeout=50) as self.conn:
				logger.info('Connection with DB '+self.database+' is established.')
		except:
			logger.error('Unable to establish connect with DB.')
			raise

	def oracle_conn(self):
		pass

	def snowflake_conn(self):
		logger.info('Snowflake Database connection detected.')
		self.uname = self.config.get('UserName')
		self.passwd = self.config.get('Password')
		self.database = self.config.get('Database')
		self.warehouse = self.config.get('Warehouse')
		self.role = self.config.get('Role')
		self.acct = self.config.get('Account')
		self.schema = self.config.get('Schema')
		from snowflake.connector import connect
		try:
			self.conn = connect( user=self.uname, 
														password=self.passwd, 
														account=self.acct, 
														database=self.database, 
														role=self.role,
														warehouse=self.warehouse,
														schema=self.schema )
			logger.info('Connected to snowflake db.')
		except:
			logger.error('Unable to establish connect with Snowflake DB.')
			raise

	def mssql_connection_string(self):
		conn_str = 'DRIVER={'+self.driver+'};SERVER=' + str(self.server)+ \
				';DATABASE='+str(self.database)+ ';UID='+str(self.uname)+ ';PWD={'+ \
				str(self.passwd)+"}"+str(self.others)
		return conn_str

	def execute_insert_stmt(self, table, values):
		from helper.utils import get_insert_stmt
		sql = get_insert_stmt(table, values)
		self.execute_stmt(sql, param=list(values.values()))

	def get_db_col_details(self, col_cfg=None, df_col=None):
		if col_cfg and col_cfg.get('Column') :
			col_list = [dict['Name'] for dict in col_cfg]
		else:
			col_list = df_col.tolist()
		return col_list

	def truncate_table(self, tablename):
		truncate_sql = "TRUNCATE TABLE " + tablename + " ; "
		self.execute_stmt(truncate_sql)

	def load_df_to_db(self, df=None, TableName=None, col_cfg=None):
		"""
		This function will load data to any database based on the config.
				= User have option of truncate and load
				= Chunk loading or not functionality. Chunk size should be mentioned 
				in config file  
		"""
		TableName = self.cfg['TableName'] if TableName is None else TableName
		# Truncating table
		if 'Truncate' in list(self.cfg.keys()) and self.cfg['Truncate'] == 'true':
			self.truncate_table(tablename=TableName)
		# Inserting into table
		if not df.empty:
			cur = self.conn.cursor()
			cur.fast_executemany = True
			col_list = self.get_db_col_details(col_cfg=col_cfg, df_col=df.columns)
			col_str = ','.join(col_list)
			load_sql = "insert into " + TableName + '(' + col_str + ") values ("  \
				+ ','.join('?' for i in range(len(col_list))) + ');'
			if 'ChunkSize' in list(self.cfg.keys()) and self.cfg['ChunkSize'] != None:
				logger.debug('Starting Chunking..')
				batch_size = self.cfg['ChunkSize']
				for chunk in np.array_split(df, batch_size):
					i = i+1
					if chunk.shape[0] > 0:
						cur.executemany(load_sql,
										chunk.values.tolist())
			else:
				# df = df.astype(str)
				df = df.astype(object).where(pd.notnull(df), None)
				cur.executemany(load_sql, df[col_list].values.tolist())
			self.close_commit(cur)
		else:
			logger.warning('No data found so no data is loaded to DB.')

	def generate_update_stmt(self, stmt_json):
		return "UPDATE "+stmt_json['Table']+" SET "+stmt_json['Set']+ " WHERE "+stmt_json['Condition']+";"
		
	def format_str_with_variable(self, str):
		col_pos = []
		final_str = ""
		for line in str.splitlines():
			import re
			x = re.findall("\${([^}]+)}", line)
			final_str = final_str+re.sub("\${([^}]+)}", "%s", line)
			col_pos.extend(x)
		return (col_pos, final_str) 

	def update_from_df(self, df):
		from helper.utils import get_list
		for statement in get_list(self.cfg['Update']['Statement']):
			update_stmt=self.generate_update_stmt(stmt_json=statement)
			(positions, update_stmt)=self.format_str_with_variable(str=update_stmt)
			for row in df.itertuples():
				vars=[]
				for position in positions:
					value=row[int(position)]
					if isinstance(value,str):
						value="'"+str(row[int(position)])+"'"
					vars.append(str(value))
				final_update_stmt=update_stmt%tuple(vars)
				self.execute_stmt(query=final_update_stmt)

	def close_commit(self, cur):
		self.conn.commit()
		cur.close()

	def get_sql_query(self, cfg, df=None):
		if cfg.get('QueryS3Location'):
			file_ext = cfg.get('QueryS3Location').split('.')[-1]
			if file_ext == 'jinja':
				from helper.utils import read_jinja_template
				script=read_jinja_template(str=cfg['Query'], df=df)
				return script
		return cfg['Query']

	def execute_on_db(self, cfg, df=None):
		sql = self.get_sql_query(cfg=cfg, df = df)
		self.execute_stmt(query=sql)

	def execute_stmt_mssql(self, query:str, param: list= None):
		logger.info('Executing: '+query)
		cursor = self.conn.cursor()
		if param:
			cursor.execute(query,param)
		else:
			cursor.execute(query)
		self.close_commit(cur=cursor)

	def execute_stmt_snowflake(self, query, param):
		logger.info('Executing: '+query)
		cursor = self.conn.cursor()
		if param:
			cursor.execute(query,param, timeout=30)
		else:
			cursor.execute(query, timeout=30)
		self.close_commit(cur=cursor)		

	def execute_stmt(self, query:str, param: list= None):
		if self.db_type == 'MSSQLServer':
			self.execute_stmt_mssql(query, param)
		elif self.db_type == 'Snowflake':
			self.execute_stmt_snowflake(query, param)
		else:
			self.execute_stmt_snowflake(query, param)