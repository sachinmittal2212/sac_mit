from helper.log.logging import logger

def get_py_dtype(argument):
  # TODO: SpareString 
  py_dtype={
        'String': 'object',
        'integer': 'int32',
        'largeinteger': 'int64',
        'float': 'float32',
        'largefloat': 'float64',
        'category': 'category'
  }
  return py_dtype.get(argument, 'object')

def get_dtype(col_cfg):
  """
  Return datatype of column based on the values passed in config file.
  """
  logger.debug('Retrieve datatype of column.')
  col_pos,datetime_col,dtype_dict  = 0, [], {}
  for col in col_cfg:
    dtype = col['Datatype'].lower()
    dtype_dict[col_pos]=get_py_dtype(dtype)
    if dtype == 'datetime':
        datetime_col.append(col_pos)
    col_pos = col_pos+1
  logger.debug('Datetime Columns: {}'.format(datetime_col))
  logger.debug('Datatype of column: {}'.format(dtype_dict))
  return (datetime_col, dtype_dict)

class Read_Dataframe_Config():
  def __init__(self, file_cfg, col_cfg) -> None:
    self.cfg = { 'sep': ',',
      'type': 'csv',
      'index': False,
      'header': 0,
      'names': None,
      'use_cols': None,
      'dtypes': str,
      'parse_dates': False,
      'na_values': [''], 
      'footer': None
    }
    self.file_cfg=file_cfg
    self.col_cfg=col_cfg
    self.set_config()

  def set_config(self):
    if self.file_cfg:
      self.set_df_from_file_cfg()
    if self.col_cfg:
      self.set_df_from_col_cfg()
  
  def set_df_from_file_cfg(self):
    self.cfg['sep'] = self.set_delimiter()
    self.cfg['type'] = self.set_file_type()
    self.cfg['index'] = self.set_index()
    self.cfg['na_values'] = self.set_navalues()
  
  def set_df_from_col_cfg(self):
    self.cfg['names']=self.set_col_names()
    self.cfg['use_cols']=self.set_use_columns()
    self.cfg['parse_dates']=self.set_datatype()[0]
    self.cfg['dtypes']=self.set_datatype()[1]
    self.cfg['footer']=self.set_footer()

  def set_delimiter(self):
    return self.file_cfg['Delimiter']

  def set_file_type(self):
    return self.file_cfg['FileType'].lower()

  def set_index(self):
    index_val=str(self.file_cfg.get('Index'))
    return True if index_val.lower()=='true' else False

  def set_navalues(self):
    if self.file_cfg.get('NAValues'):
      na_val=self.file_cfg['NAValues']['Value']
      na_list=na_val if na_val is list else [na_val]
      na_list.append('')
      return na_list
    return ['']

  def set_header(self):
    if self.file_cfg.get('Header'):
        return int(self.file_cfg['Header'])
    else:
        return False

  def set_col_names(self):
    return [col['Name'] for col in self.col_cfg]
  
  def set_datatype(self):
    return get_dtype(col_cfg=self.col_cfg)
  
  def set_use_columns(self):
    return [col.get('SrcColumnName') for col in self.col_cfg]

  def set_footer(self):
    return [col.get('Footer') for col in self.col_cfg]
