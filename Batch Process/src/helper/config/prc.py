from helper.log.logging import logger
from pathlib import Path
from helper.config.db import Database_Config

class Process_Config():
  def __init__(self, prc_info, S3) -> None:
    self.cfg_dict = S3.get_dict_from_xml(prc_info=prc_info)
    self.prc_info=prc_info
    self.S3=S3
    self.src_cfg=self.get_src_config()
    self.input_cfg = self.get_input_config()
    self.output_cfg = self.get_output_config()
    self.valid_cfg = self.get_validation_config()
    self.arc_cfg = self.get_arc_config()
    # self.dest = self.cfg_dict['Configuration'].get('Destination')
    self.dest_cfg = self.get_dest_config()
    logger.debug('Loaded Configuration in FileManipulation Class')

  def get_output_config(self):
    output_cfg=self.cfg_dict['Configuration'].get('OutputConfig')
    if not output_cfg:
      output_cfg={'ModifyExtract': 'false'}
    return output_cfg

  def get_arc_config(self):
    arc_cfg = {'Input':{}, 'Output': {}}
    arc_folder=Path('crd_batch_interface', self.prc_info['APP_NAME'], 'archive')
    if self.input_cfg.get('Archive'):
      arc_cfg['Input']['Datetime'] = self.input_cfg['Archive']['Datetime']
      arc_cfg['Input']['Offset'] = self.input_cfg['Archive']['Offset']
      arc_cfg['Input']['Path'] = self.input_cfg['Archive']['Path']
    else:
      arc_cfg['Input']['Datetime'] = "_%Y-%m-%d_%H-%M-%S"
      arc_cfg['Input']['Offset'] = 180
      arc_cfg['Input']['Path'] = Path(arc_folder, self.prc_info['PRC_NAME'], 'input') 
    if self.output_cfg.get('Archive'):
      arc_cfg['Output']['Datetime'] = self.output_cfg['Archive']['Datetime']
      arc_cfg['Output']['Offset'] = self.output_cfg['Archive']['Offset']
      arc_cfg['Output']['Path'] = self.output_cfg['Archive']['Path']
    else:
      arc_cfg['Output']['Datetime'] = "_%Y-%m-%d_%H-%M-%S"
      arc_cfg['Output']['Offset'] = 180
      arc_cfg['Output']['Path'] = Path(arc_folder, self.prc_info['PRC_NAME'], 'output') 
    return arc_cfg

  def get_default_validation_s3_cfg(self):
    s3_cfg={}
    from datetime import datetime
    dt=datetime.today()
    dt_str=dt.strftime("_%Y-%m-%d_%H-%M-%S") 
    file_name = self.prc_info['PRC_NAME']+'_validation'+dt_str
    s3_cfg['Location']='crd_batch_interface/ic/exception'
    file_cfg={
              'FileType': 'csv', 
              'FileName': file_name, 
              'Delimiter': ',', 
              'Header': '0', 
              'Index': True }
    s3_cfg['FileConfig']=file_cfg
    return s3_cfg


  def get_validation_config(self):
    validation_cfg = self.cfg_dict['Configuration'].get('Validation')
    if validation_cfg: 
      if 'S3' in validation_cfg:
        return validation_cfg
      else:
        s3_cfg=self.get_default_validation_s3_cfg()
        validation_cfg['S3']=s3_cfg
        return validation_cfg
    else:
      return None

  def get_src_config(self):
    src_cfg=self.cfg_dict['Configuration'].get('Source')
    src_type = list(src_cfg.keys())[0]
    if src_type.lower() == 'database':
      db=Database_Config(src_cfg['Database'], self.S3)
      db.get_cfg()
      src_cfg['Database']=db.cfg_dict
      src_cfg['Database'].move_to_end(db.db_type, last=False)
    if src_cfg.get('S3') == None:
      src_cfg['S3'] = self.default_input_cfg()
    return src_cfg

  # def set_dest_db_config

  def get_dest_config(self):
    dest_cfg=self.cfg_dict['Configuration'].get('Destination')
    dest_type = list(dest_cfg.keys())
    if 'database' in [target.lower() for target in dest_type]:
      db=Database_Config(dest_cfg['Database'], self.S3)
      db.get_cfg()
      dest_cfg['Database']=db.cfg_dict
      dest_cfg['Database'].move_to_end(db.db_type, last=False)
    return dest_cfg

  def default_input_cfg(self):
    app_name = self.prc_info['APP_NAME']
    prc_name = self.prc_info['PRC_NAME']
    path_list=['crd_batch_interface', app_name, 'input']
    default_input_cfg = { 'Location': '/'.join(path_list),
        'FileConfig': { 'FileType': 'csv',
          'FileName': prc_name,
          'Delimiter': ',',
          'Header': '0',
          'Index': None } }
    return default_input_cfg

  def get_input_config(self):
    """
    Used to set the input congig in case when no input config.
    """
    input_cfg=self.cfg_dict['Configuration'].get('InputConfig')
    if input_cfg == None:
      input_cfg={}
      logger.warning('Input config missing.Using generic config.')
      input_cfg['ModifyExtract'] = 'false'      
    logger.debug('Input config found in the config file.')
    return input_cfg

  def __str__(self) -> str:
    class_str = "Source Config : "+str(self.src_cfg) + "\n\n"
    class_str = class_str + "Input Config : "+ str(self.src_cfg) + "\n\n"
    class_str = class_str + "Output Config : "+ str(self.input_cfg) + "\n\n"
    class_str = class_str + "Validatin Config : "+ str(self.valid_cfg) + "\n\n"
    class_str = class_str + "Archive Config : "+ str(self.arc_cfg) + "\n\n"
    class_str = class_str + "Destination Config : "+ str(self.dest_cfg) + "\n\n"
    return class_str