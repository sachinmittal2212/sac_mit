from helper.log.logging import logger
import pandas as pd

class Validation():
	def __init__(self, df, pc):
		logger.info('Starting extract validation')
		self.df = df
		self.pc=pc
		self.issue_df, violation=pd.DataFrame(), False
		self.validate_df()
		self.is_violated()
		# logger.info('Extract validation completed.')

	def format_violation(self, violation_df, reason):
		violation_df['Original_Index']=violation_df.index
		violation_df['Reason'] = reason
		return violation_df

	def primary_key_validator(self):
		viol_df = pd.DataFrame()
		col_to_verify = list(self.pc.valid_cfg['Primary']['Column_Pos'])
		for column_pos in col_to_verify:
			dup_in_col = self.check_dup_value_in_column(column_pos)
			viol_df=pd.concat([viol_df,dup_in_col], axis=0, verify_integrity=True, ignore_index=True)
			null_in_col = self.check_null_in_column(column_pos)
			viol_df=pd.concat([viol_df,null_in_col], axis=0, verify_integrity=True, ignore_index=True)
		return viol_df

	def no_data_check(self):
		if len(self.df) == 0:
			self.violation = True

	def is_violated(self):
		if len(self.issue_df) > 0:
			return True

	def validator(self, cond):
		validators_dict = {
			'Primary': self.primary_key_validator,
			'NotNull': self.null_value_validator, 
			'NoDataCheck': self.no_data_check
		}
		try:
			return validators_dict[cond]()
		except:
			logger.info('Invalid Validator')

	def null_value_validator(self):
		viol_df = pd.DataFrame()
		col_to_verify = list(self.pc.valid_cfg['NotNull']['Column_Pos'])
		for column_pos in col_to_verify:
			null_in_col = self.check_null_in_column(column_pos)
			viol_df=pd.concat([viol_df,null_in_col], axis=0, verify_integrity=True, ignore_index=True)
		return viol_df

	def validate_df(self):
		conditions=self.pc.valid_cfg.keys()
		for cond in conditions:
			viol_df = self.validator(cond)
			self.issue_df=pd.concat([self.issue_df, viol_df], axis=0, verify_integrity=True, ignore_index=True)

	def check_dup_value_in_column(self, column_pos):
		logger.info('Duplicate Value validation: {}'.format(column_pos))
		column_name=list(self.df.columns)[int(column_pos)]
		temp_df= self.df[self.df.duplicated(column_name)]
		reason='Primary Key Validation: Duplicate Value'
		dup_in_col = self.format_violation(temp_df, reason)
		return dup_in_col

	def check_null_in_column(self, column_pos):
		logger.info('Starting nullity validation..')
		column_name=list(self.df.columns)[int(column_pos)]
		logger.info('Checking Nullility in {}'.format(column_name))
		null_rows = self.df[self.df[column_name].isna()]
		reason='Null Value Validation: Null Value'
		null_in_col = self.format_violation(null_rows, reason)
		return null_in_col

class Action_on_Violation():
	def __init__(self, validation, S3):
		self.val = validation
		self.FM = self.set_df(df=self.val.issue_df)
		self.S3_conn=S3
		self.pc = self.val.pc
		self.severity = self.pc.valid_cfg['Severity']
		self.action()
	
	def set_df(self, df):
		from helper.handler import FileManipulation
		FM=FileManipulation(df=df)
		return FM

	def action(self):
		self.load_issue_to_s3()
		if self.severity == '2':
				self.val.df.drop(self.val.issue_df.Original_Index, inplace=True)
		if self.severity == '3':
			raise Exception('Seveirty 3 Action: Invalid rows in the data.') 
	
	def load_issue_to_s3(self):
		from helper.utils import get_write_df_cfg
		from helper.utils import get_local_path_from_cfg, get_path_from_cfg
		df_cfg=get_write_df_cfg(self.pc.valid_cfg)
		local_key=get_local_path_from_cfg(self.pc.valid_cfg['S3'])
		self.FM.put_df_to_local(path=local_key, cfg=df_cfg)
		s3_key = get_path_from_cfg(self.pc.valid_cfg['S3'])
		self.S3_conn.put_file_to_s3(S3_key=s3_key, local_key=local_key)
