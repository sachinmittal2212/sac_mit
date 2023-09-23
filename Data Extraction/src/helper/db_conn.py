# from threading import Thread
import os
from pandas import read_sql_query, read_sql
import numpy as np
from helper.log.logging import logger
from helper.config.db_config import Database_Config
# from memory_profiler import profle

class Db_Conn():
	def __init__(self, cfg=None):
		if cfg is not None:
			self.cfg=cfg
			# print(self.cfg.keys()[0])
			self.db_type=list(self.cfg.keys())[0]
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
			raise('Unidentified Database ')

	def oracle_conn(self):
		from cx_Oracle import connect, makedsn, init_oracle_client
		logger.info('Oracle Datbase connection detected')
		self.hostname = self.config.get('HostName')
		self.uname = self.config.get('UserName')
		self.passwd = self.config.get('Password')
		self.server = self.config.get('Server')
		self.driver = self.config.get('Driver')
		self.database = self.config.get('Database')
		self.port = self.config.get('Port')
		dsn=makedsn(self.hostname, self.port, service_name=self.database)
		try:
			self.conn=connect(user=self.uname, password=self.passwd, dsn = dsn, encoding='UTF-8')
		except:
			logger.error('Unable to establish connect with DB.')
			raise

	def get_database_type(self, cfg):
		return list(cfg.keys())[0]

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
			with connect(connection_string) as self.conn:
				logger.info('Connection with DB '+self.config.get('Database')+' is established.')
		except:
			logger.error('Unable to establish connect with DB.')
			raise

	def snowflake_conn(self):
		logger.info('Snowflake Database connection detected.')
		from snowflake.connector import connect
		try:
			self.conn = connect( user=self.config.get('UserName'), 
														password=self.config.get('Password'), 
														account=self.config.get('Account'), 
														database=self.config.get('Database'), 
														role=self.config.get('Role'),
														warehouse=self.config.get('Warehouse'),
														schema=self.config.get('Schema'))
		except:
			logger.error('Unable to establish connect with Snowflake DB.')
			raise

	def mssql_connection_string(self, server = None, database = None, 
																		driver = None, uname = None, 
																		passwd = None, others = None ):
		if server == None:
			conn_str = 'DRIVER={'+self.driver+'};SERVER=' + str(self.server)+ \
				';DATABASE='+str(self.config.get('Database'))+ ';UID='+str(self.uname)+ ';PWD={'+ \
				str(self.passwd)+"}"+str(self.others)
		else:
			conn_str = 'DRIVER={'+str(driver)+'};SERVER=' + str(server)+ \
				';DATABASE='+str(database)+ ';UID='+str(uname)+ ';PWD={'+ \
				str(passwd)+"}"+str(others)
		return conn_str

	def multithread_conn_str(self):
		import urllib.parse
		substr = 'mssql://'+self.uname+':{}@'+self.server+":1433/"+self.config.get('Database')
		conn_str = substr.format(urllib.parse.quote_plus(self.passwd))
		return conn_str

	def execute_commit_stmt(self, query):
		logger.info('Executing: '+query)
		cursor = self.conn.cursor()
		cursor.execute(query, timeout=30)
		cursor.close()

	def execute_insert_stmt(self, table, **kwargs):
		cursor = self.conn.cursor()
		col_names, values = '(', []
		for column, value in kwargs.items():
			col_names = col_names+str(column) + ', '
			values.append(value)
		insert_stmt = 'INSERT INTO '+table+' ' + \
			col_names[:-2] + ') values ' + \
			str(tuple(values)) + ';'
		val = cursor.execute(insert_stmt, timeout=30)
		self.close_cursor(cursor)

	def mssql_multithread_dataextract(self, sql):
		print('Starting multi-threading')
		import connectorx as cx
		conn_str = self.multithread_conn_str()
		df = cx.read_sql(conn_str, sql)
		return df

	# @profile
	def get_df_from_db(self, query=None):
		logger.info('Starting data extraction from {}'.format(self.config.get('Database')))
		if query == None:
			sql = self.cfg['Query']
		else:
			sql = query
		if sql != None:
			sql = sql.replace('{ENV}', os.environ['ENV'])
		else:
			logger.warning('SQL query is None.')
		if str(self.config.get('Multithread')).lower() == 'true':
			logger.info('Using MSSQL Multithread.')
			df = self.mssql_multithread_dataextract(sql)
		else:
			df = read_sql(sql, self.conn, coerce_float=False)
		"""
		TODO:
		cur = self.conn.cursor()
		cur.execute(query)
		cur.query_result(cur.sfqid)
		df = cur.fetch_pandas_all()
		---------------
		# df = pd.DataFrame(cur.fetchall())
		# df.columns = cur.keys()
		"""
		logger.info('Data Extraction completed. Closing DB connection.')
		# self.conn.close()
		return df


	def execute_stmt(self, connection, query):
		cursor = connection.cursor()
		cursor.execute(query)
		cursor.close()

	def execute_query(self, connection, query):
		cursor = connection.cursor()
		cursor.execute(query)
		return cursor

	def load_df_to_local(self, query, local_key, sep=',', batch_size=0):
		logger.info('Starting data extraction from {}'.format(self.config.get('Database')))
		sql = query
		i = 0
		chunksize = int(batch_size)
		if(chunksize < 1):
			raise Exception('Chunk size has to be passed.')
		for chunk in read_sql_query(sql, self.conn, chunksize=chunksize):
			if(i > 0):
				chunk.to_csv(local_key, sep=sep, mode='a',
							 encoding='utf-8', index=False, header=False)
			else:
				chunk.to_csv(local_key, sep=sep, encoding='utf-8', index=False)
			i = i+1
		#df = read_sql(sql, self.conn)
		logger.info('Data Extraction completed. Closing DB connection.')
		self.conn.close()

	def get_data_from_db(self, query):
		sql = query
		df = read_sql(sql, self.conn)
		self.conn.close()
		return df

	def get_db_col_details(self, col_cfg=None, df_col=None):
		if col_cfg == None:
			col_list = df_col.tolist()
		else:
			col_list = [dict['Name'] for dict in col_cfg]
		return col_list

	def truncate_table(self, tablename):
		cur = self.conn.cursor()
		logger.info('Truncating table..')
		truncate_sql = "TRUNCATE TABLE " + tablename + " ; "
		cur.execute(truncate_sql)
		self.close_cursor(cur)

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
			cur.executemany(load_sql, df[col_list].values.tolist())
		self.close_cursor(cur)

	def close_cursor(self, cur):
		self.conn.commit()
		cur.close()
