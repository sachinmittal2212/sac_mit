from pathlib import Path

from helper.log.logging import logger
from helper.utils import path_env
from helper.utils import NVL

class Archive():
  def __init__(self, file_key, cfg, S3, dt=True) -> None:
    self.S3=S3
    self.file_key = self.S3.get_latest_s3_fpath(key=file_key)
    self.cfg=cfg
    self.dt = dt
    self.arc_key = self.get_arc_path()
  
  def arc_file_name(self, dateTime):
    filename = Path(self.file_key).name
    if self.dt:
      return filename.split('.')[0]+dateTime+'.'+filename.split('.')[1]
    else:
      return filename.split('.')[0]+'.'+filename.split('.')[1] 

  def get_arc_path(self):
    if self.file_key: 
      file_dt=self.S3.last_modified_of_file(key=self.file_key)
      datetime_format = NVL(self.cfg.get('Datetime'), "_%Y-%m-%d_%H-%M-%S")
      file_dt=file_dt.strftime(datetime_format)
      filename=self.arc_file_name(dateTime=file_dt)
      s3_path=self.S3.get_s3_path(str(self.cfg['Path']))
      s3_key=path_env(Path(s3_path, filename))
      return s3_key
    else:
      return False

  def delete_older_files(self):
    key_path = Path(self.file)
    filename = key_path.stem+'(.*)'+key_path.suffix
    key = path_env(Path(path_env(key_path.parent), filename))
    file_list = self.S3.get_file_ordered_list(key=key)
    if file_list:
      if len(file_list) > self.offset:
        shortlisted_file_list=file_list[self.offset:]
        for path, time in shortlisted_file_list:
          self.S3.delete_file(path)
        logger.info('Deleted {}'.format(len(shortlisted_file_list)))

  def archive(self):
    if self.arc_key:
      self.S3.move_file(src_key=self.file_key, dest_key=self.arc_key)
      # self.delete_older_files()
    else:
      logger.info('File not found to archive')
 

    