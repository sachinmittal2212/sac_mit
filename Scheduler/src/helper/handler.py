import pandas as pd
import os
import gc
from datetime import datetime
from pathlib import Path

class FileManipulation():
    """
    The FileManipualtion deals with file and dataframe handling.
      > config_dict: OrderedDict: It can be input or output config from xml 
      > cfg_name: String: Process Name variable 
      > df: Pandas DataFrame: It's the extracted dataframe from the datasource
    """

    def __init__(self, cfg_dict=None, df=None):
        self.df = df
        if cfg_dict != None:
            print('Loading configuration to variables')
            self.input_cfg = cfg_dict['Configuration'].get('InputConfig')
            self.output_cfg = cfg_dict['Configuration'].get('OutputConfig')
            self.dest = cfg_dict['Configuration'].get('Destination')
        else:
            print('config dict="None" detected in FileManipulation class.')

    def filename_regex(self, f_name, cfg):
        """
        Function to update regex expression in the filename
         > f_name: filename
         > cfg: 
        """
        DT = '[datetime]'
        date_str = datetime.today().strftime("_%Y%m%d_%H%M%S_%f")
        if DT in f_name and 'DateFormat' in list(cfg.keys()):
            if cfg['DateFormat'] is None:
                f_name = f_name.replace(DT, date_str)
            else:
                date_str = datetime.today().strftime(cfg['DateFormat'])
                f_name = f_name.replace(DT, date_str)
        return f_name

    def get_filename(self, file_dict=None, cfg=None, loc='S3', arc=False, 
    file_cfg=None):
        """
        Generate FileNames based on parameters
          > file_dict : Dict containing filename and extension
            {'name':'example', 'ext': 'csv'}
          > cfg : possible vals: output, input, None(to gen file name form dict)
          > loc : possible vals: S3, IMFT
        """
        f_name, f_ext = None, None
        date_str = datetime.today().strftime("_%Y%m%d_%H%M%S_%f")
        if file_cfg != None:
            print(file_cfg[loc]['FileConfig']['FileName'])
            f_name = file_cfg[loc]['FileConfig']['FileName']
            f_ext = file_cfg[loc]['FileConfig']['FileType']
            file_cfg=file_cfg[loc]['FileConfig']
        elif file_dict != None:
            f_name = file_dict['name']
            f_ext = file_dict['ext']
        elif cfg == 'output':
            file_cfg = self.dest[loc]['FileConfig']
            f_name = file_cfg.get('FileName')
            f_ext = file_cfg.get('FileType')
        elif cfg == 'input':
            file_cfg = self.input_cfg[loc]['FileConfig']
            f_name = file_cfg['FileName']
            f_ext = file_cfg['FileType']
        if file_cfg is not None:
            f_name = self.filename_regex(f_name=f_name, cfg=file_cfg)
        if arc == True:
            f_name = f_name + date_str
        result = f_name+'.'+f_ext
        return result

    def get_file_path(self, f_dict=None, cfg=None, loc=None, arc=False, \
        local=False, file_cfg=None, add_filename=True, unixPath=False):
        path = None
        if local == True:
            print('Local path request.')
            path = Path('/tmp/')
        else:
            if cfg == 'output':
                path = self.dest[loc]['Location']
            if cfg == 'input':
                path = self.input_cfg[loc]['Location']
            if cfg == 'exception':
                path = file_cfg[loc]['Location']
        if arc == True:
            path=Path(Path(path).parent, 'archive')
        
        if os.environ.get("AWS_EXECUTION_ENV") is None:
            tmp_path = Path('..', 'test', 'tmp')
            path = Path('..', 'test')/path if local != True else tmp_path
        if add_filename == True:
            f_name = self.get_filename(file_dict=f_dict, cfg=cfg, loc=loc, arc=arc, file_cfg=file_cfg)
        else:
            f_name=''
        full_path = Path(path, f_name)
        return str(full_path) if not unixPath else str(full_path.as_posix())

    def get_formatted_df(self, df, col_cfg):
        for col_dict in list(col_cfg['Column']):
            if 'RuntimeCreation' in col_dict.keys():
                print('Creating new column in position {} at runtime.')
                if 'Constant' in col_dict['RuntimeCreation'].keys():
                    const=col_dict['RuntimeCreation']['Constant']
                    col_pos = int(col_dict['Index'])
                    # getting datatype from get_dtype for one column using col_cfg
                    dtype=self.get_dtype([col_dict])[1][0]
                    new_col_data= pd.Series([const for x in range(len(df))],\
                        dtype=dtype)
                    df.insert(col_pos, col_dict['Name'], new_col_data)
                    print('Constant column type found.')
        return df

    def get_dtype(self, col_cfg):
        if col_cfg is None:
            col_cfg = self.output_cfg['FileName']['Column']
        col_pos = 0
        datetime_col = []
        dtype_dict = {}
        for col in col_cfg:
            dtype = col['Datatype'].lower()
            if col['Datatype'] == 'String':
                dtype_dict[col_pos]='object'
            if dtype== 'integer':
                dtype_dict[col_pos] = 'int32'
            if dtype== 'largeinteger':
                dtype_dict[col_pos] = 'int64'
            if dtype == 'float':
                dtype_dict[col_pos] = 'float32'
            if dtype == 'largefloat':
                dtype_dict[col_pos] = 'float64'
            if dtype == 'category':
                dtype_dict[col_pos] = 'category'
            if dtype == 'datetime':
                datetime_col.append(col_pos)
            # if col['Datatype'] == 'sparsestring':
            #     dtype_dict[col_pos]='Sparse[string]'
            col_pos = col_pos+1
        return (datetime_col, dtype_dict)

    def get_df_config(self, file_cfg, col_cfg):
        df_cfg = {
            'sep': ',',
            'type': 'csv',
            'index': None,
            'header': None,
            'names': [],
            'use_cols': [],
            'dtypes': {},
            'parse_dates': []
        }
        df_cfg['sep'] = file_cfg['Delimiter']
        df_cfg['type'] = file_cfg['FileType'].lower()
        (parse_dates, dtype_dict) = self.get_dtype(col_cfg=col_cfg)
        df_cfg['index'] = file_cfg['Index']
        if file_cfg['Header'] is not None:
            df_cfg['header'] = int(file_cfg['Header'])
            df_cfg['names'] = [col['Name'] for col in col_cfg]
            df_cfg['use_cols'] = [col['SrcColumnName'] for col in col_cfg \
                if col['SrcColumnName'] is not None]
        else:
            df_cfg['header'] = None
            df_cfg['header'] = None
            df_cfg['names'] = None
            df_cfg['use_cols'] = None
        df_cfg['parse_dates'] = parse_dates
        df_cfg['dtypes'] = dtype_dict
        return df_cfg

    def get_raw_data_from_local(self, key):
        df_binary = open(key, "rb")
        return df_binary.read()

    def get_df_from_raw_data(self, data, cfg={}):
        import io
        if cfg.get('type').lower().strip()=='csv':
            df = pd.read_csv(io.BytesIO(data), 
                            sep=cfg.get('sep'),
                            header=cfg.get('header'),
                            names=cfg.get('names'),
                            usecols=cfg.get('use_cols'), 
                            dtype=cfg.get('dtypes'),
                            parse_dates=cfg.get('parse_dates'),
                            keep_default_na=True,
                            # na_values=[None]
                            )
            df = df.where((pd.notnull(df)), None)
            df.info(memory_usage='deep')
            gc.collect()
        return df

    def get_df_from_S3(self, S3, file_key, file_cfg=None, col_cfg=None):
        # Getting raw data
        try:
            if os.environ.get("AWS_EXECUTION_ENV") is not None:
                raw_data = S3.get_raw_data_from_S3(file_key)
            else:
                raw_data = self.get_raw_data_from_local(file_key)
        except Exception as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                print("File not found in the S3 location {}", format(file_key))
                raise
        # File Configuration
        df_cfg = self.get_df_config(file_cfg=file_cfg, col_cfg=col_cfg)
        df = self.get_df_from_raw_data(data=raw_data, cfg=df_cfg)
        # if str(cfg.get('ModifyExtarct')).lower() == 'true':
        #     df=self.get_formatted_df(df=df, col_cfg=col_cfg)
        del raw_data
        gc.collect()
        return df

    def put_str_to_local(self, str, local_key):
        # Loading file from str
        if os.environ.get("AWS_EXECUTION_ENV") is not None:
            # to do 
            with open(local_key.as_posix(), 'w') as sch_xml:
                sch_xml.write(str)
        else:
            with open(local_key, 'w') as sch_xml:
                sch_xml.write(str)

    def get_xml_from_dict(self, dict, indent):
        from dict2xml import dict2xml
        return dict2xml(dict, indent = indent)

    def put_df_to_local(self, df, file_loc, file_cfg=None, cfg=[]):
        #TODO: Why taking complete config insteas of just col_Cfg
        print('Loading dataframe to Local', file_loc)
        col_cfg = cfg['ColumnDetails']['Column'] if cfg else []
        df_cfg = self.get_df_config(file_cfg, col_cfg)
        # if str(cfg.get('ModifyExtarct')).lower() == 'true':
        #     df=self.get_formatted_df(df=df, col_cfg=col_cfg)
        if df_cfg['type'].lower() == 'csv':
            # TODO: Multi character Separator
            # TODO: Give adding quotes to column values
            header = df_cfg['names'] if df_cfg['names'] else df_cfg['header']
            df.to_csv(file_loc, sep=df_cfg['sep'], header=header,\
                    index=False, columns=df_cfg['use_cols'])
        if df_cfg['type'] == 'paraquet':
            # TODO: Work on using Paraquet flag
            pass

    def put_file_to_imft(self, imft_cfg=None, src=None, dest=None, \
        username=None, password=None):
        print('Starting the IMFT..')
        if dest == None:
            dest = self.get_file_path(cfg='output', loc='IMFT', unixPath=True)
        print(dest)
        from paramiko.client import SSHClient
        from paramiko import AutoAddPolicy
        client = SSHClient()
        # TODO: Get the hostname and port to env variables
        hostname = 'securedata-uat.ops.invesco.net'
        # hostname = 'imft-uat.invesco.com'
        port = 1022
        client.set_missing_host_key_policy(AutoAddPolicy())
        username = imft_cfg['UserName'] if not username else username
        if password is None:
            from helper.aws_conn import SecretManagerConn
            SM = SecretManagerConn()
            secret = SM.get_sm_dict()
        #     print(secret)
            password = secret[username]
        client.connect(hostname=hostname, username=username,
                       password=password, port=port, timeout=360,
                       banner_timeout=200, auth_timeout=360)

        with client.open_sftp() as sftp:
            print('Connection established!!')
            try:
                # print(sftp.chdir('output'))
                print(sftp.listdir())
                sftp.put(src, dest)
            except:
                print('Unable to drop the file at', src,
                      'to the IMFT location', dest)
                raise


"""
This class will be used for dataframe validation.

"""
class Validate_DataFrame():
    def __init__(self, df, val_cfg):
        print('Validating ')
        self.df=df
        self.val_cfg = val_cfg
        # self.cfg_name=cfg_name
        # self.cfg=cfg['ExceptionHandling']
        self.issue_df=pd.DataFrame()
        # # from helper.File_Manager import FileManipulation
        # self.FM = FileManipulation()
        self.validate_df()

    def action_on_exception(self):
        """
        Take action on exception found in the data based on the severity 
        tag in the config file
        1: No Action drop the exception to drop location
        2: Remove the excpetion from the data and 
        load the remaining data set
        3: Break the process.
        """
        severity = self.val_cfg.get('Severity')
        self.send_exception()
        if self.issue_df.size > 0:
            if severity == '2':
                self.df.drop(self.issue_df.index, inplace=True)
                return self.df
            if severity == '3':
                raise Exception('Seveirty 3 Action: Invalid rows in the data.') 
        return self.df

    def send_exception(self):
        print('Starting send exception..')
        drop_details = self.val_cfg.keys()
        if 'S3' in drop_details: 
            from helper.aws_conn import S3Conn
            from helper.handler import FileManipulation 
            FM = FileManipulation()
            S3_path = FM.get_file_path(cfg='exception', file_cfg=self.val_cfg, loc='S3')
            S3=S3Conn('ivz-dev-0065-crd-batch-ue1')
            local_path =FM.get_file_path(local=True, cfg='exception', \
                loc='S3', file_cfg=self.val_cfg)
            # print(local_path)
            FM.put_df_to_local(df = self.issue_df, file_loc=local_path, \
                file_cfg=self.val_cfg['S3']['FileConfig'])
            S3.put_file_to_s3(S3_key=S3_path, local_key=local_path)
        if 'EmailDetails' in drop_details:
            # TODO: Add the Email code over here 
            pass

    def validate_df(self):
        (Primary, Nullility)=([], [])
        issue_dup_df=pd.DataFrame()
        if self.val_cfg.get('Primary')!=None:
            Primary=list(self.val_cfg['Primary']['Column_Pos'])
            issue_dup_df=self.duplicate_column_validation(Primary)
        if self.val_cfg.get('Nullility')!=None:
            Nullility=list(self.val_cfg['Nullility']['Column_Pos'])
        # getting list of all non-null validation columns
        Non_Null_list=list(set(Nullility+Primary))
        issue_nnull_df=self.not_null_column_validation(Non_Null_list)
        self.issue_df=pd.concat([issue_dup_df,issue_nnull_df]).drop_duplicates()
        del issue_nnull_df
        del issue_dup_df
        gc.collect()

    def duplicate_column_validation(self, col_list):
    # print("primary column .. ", primary_col)
        dup_df = pd.DataFrame()
        if col_list is not None:
            for col_no in col_list:
                col_name=list(self.df.columns)[int(col_no)]
                new_dup_df = self.df[self.df.duplicated(col_name)]
                dup_df=pd.concat([dup_df,new_dup_df], axis=0, verify_integrity=True, 
                    ignore_index=True)
        gc.collect()
        # print(non_prim_df)
        return dup_df

    def not_null_column_validation(self, col_list):
        print('Starting nullity validation..')
        null_rows = pd.DataFrame()
        for col_no in col_list:
            col_name=list(self.df.columns)[int(col_no)]
            print('Checking Nullility in', col_name)
            temp = self.df[self.df[col_name].isna()]
            null_rows = pd.concat([temp, null_rows]).drop_duplicates()
        del temp
        gc.collect()
        return null_rows# 