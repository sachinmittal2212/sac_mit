from helper.utils import get_list
from helper.utils import get_path_from_cfg
from helper.archive import Archive
from helper.utils import put_extract_to_tmp_folder


class Deliver_Extract():
  def __init__(self, pc, FM , S3) -> None:
    self.pc, self.FM, self.S3=pc, FM, S3
    self.deliver_file_to_dest()
  
  def deliver_file_to_dest(self):
    targets = list(self.pc.dest_cfg.keys())
    for dest in targets:
      if 'S3' in dest:
        self.deliver_files_to_s3()
      if 'CRDAPIAdaptor' in dest:
        self.deliver_files_using_CAA()
      if 'Email' in dest:
        self.deliver_files_using_email()
      if 'Database' in dest: 
        self.deliver_files_to_db()
      if 'IMFT' in dest:
        self.deliver_files_to_IMFT()

  def deliver_files_to_s3(self):
    for s3_target in get_list(self.pc.dest_cfg['S3']):
      self.pc.output_cfg['S3']=s3_target
      local_key = put_extract_to_tmp_folder(FM=self.FM , cfg=self.pc.output_cfg)
      s3_key = get_path_from_cfg(s3_target)
      from helper.utils import filename_regex
      s3_key=filename_regex(path=s3_key, cfg=s3_target['FileConfig'])
      arc=Archive(file_key=s3_key, S3=self.S3, cfg=self.pc.arc_cfg['Output'])
      arc.archive()
      self.S3.put_file_to_s3(S3_key=s3_key, local_key=local_key)

  def deliver_files_using_CAA(self):
    from helper.caa_conn import CRDApiAdaptor
    for caa_target in get_list(self.pc.dest_cfg['CRDAPIAdaptor']):
      CAA=CRDApiAdaptor(config=caa_target, df=self.FM.df, S3=self.S3)
      CAA.put_data_in_crd()
  
  def deliver_files_using_email(self):
    from helper.email_conn import Email
    for email_target in get_list(self.pc.dest_cfg['Email']):
      email_cfg = self.pc.dest_cfg['Email']
      email=Email(email_cfg=email_cfg, S3=self.S3, FM=self.FM)
      email.send_email()

  def deliver_files_to_db(self):
    from helper.db_conn import Db_Conn
    for db_target in get_list(self.pc.dest_cfg['Database']):
      db_cfg = db_target
      db_conn = Db_Conn(cfg=db_cfg)
      if 'OnStart' in db_cfg.keys():
        db_conn.execute_on_db(cfg=db_cfg['OnStart'], df=self.FM.df)
      if 'Update' in db_cfg.keys():
        db_conn.update_from_df(df=self.FM.df)
      elif 'Execute' in db_cfg.keys():
        db_conn.execute_on_db(cfg=db_cfg['Execute'], df=self.FM.df)
      else:
        db_conn.load_df_to_db(df=self.FM.df, col_cfg=self.pc.output_cfg)
      if 'OnFinish' in db_cfg.keys():
        db_conn.execute_on_db(cfg=db_cfg['OnFinish'], df=self.FM.df)

  def deliver_files_to_IMFT(self):
    for imft_target in get_list(self.pc.dest_cfg['IMFT']):
      self.pc.output_cfg['S3']=imft_target
      local_key = put_extract_to_tmp_folder(FM=self.FM , cfg=self.pc.output_cfg)
      self.FM.put_file_to_imft(src=local_key, imft_cfg=imft_target)