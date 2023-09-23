from helper.utils import get_list
from helper.log.logging import logger
import pandas as pd
from collections import OrderedDict

class Transformation:
    def __init__(self, FM, pc, S3) -> None:
        self.pc=pc
        self.S3=S3
        self.FM=FM
        self.trans_cfg=pc.trans_cfg
    
    def init_dataset(self):
        ds = OrderedDict()
        if self.pc.src_cfg.get('Name'):
            ds[self.pc.src_cfg['Name']]=self.FM.df
        else:
            ds['S0']=self.FM.df
        return ds

    def transform(self):
        self.dataset=self.init_dataset()
        for tfm in list(self.trans_cfg.keys()):
            if tfm == 'Joiner':
                joiner_cfg = self.trans_cfg['Joiner']
                j = Joiner(joiner_cfg, self.dataset, self.S3)
                self.dataset = j.join()
            if tfm == 'Header':
                header_cfg=self.trans_cfg['Header']
                self.dataset=add_header(self.dataset, header_cfg)
        last_key = list(self.dataset.keys())[-1]
        return self.dataset[last_key]


def add_header(dataset, cfg):
    ds_name = cfg.get('Alias')
    col_name = [col.strip() for col in cfg.get('Name').split(',')]
    if len(col_name) != len(dataset[ds_name].columns):
        logger.warning('Incorrect number of columns..')
    dataset[ds_name].columns=col_name
    return dataset

class Joiner:
    def __init__(self, joiner_cfg, dataset, S3) -> None:
        self.ds_code = []
        self.set_joiner_param(joiner_cfg , dataset, S3)

    def set_joiner_param(self, joiner_cfg, dataset, S3):
        self.S3=S3
        self.cfg = joiner_cfg
        self.dataset = dataset
        self.name = joiner_cfg['Name']

    def join(self):
        self.get_dataset()
        ds=self.ds_code
        join_cond = self.get_join_cond()
        merged_df = pd.merge(self.dataset[ds[0]], 
                             self.dataset[ds[1]], 
                             how=self.cfg['Type'], 
                             left_on=join_cond[ds[0]], 
                             right_on=join_cond[ds[1]], 
                             suffixes=('.'+ds[0],'.'+ds[1]))
        cols=self.get_column_list()
        col_name=self.get_column_name()
        merged_df = merged_df[cols]
        merged_df.dropna(how='all', inplace=True)
        merged_df.rename(columns=col_name, inplace=True)
        self.dataset[self.name]=merged_df
        return self.dataset

    def get_dataset(self):
        self.get_source(cfg = self.cfg['Source1'])
        self.get_source(cfg = self.cfg['Source2'])

    def get_joining_var_dict(self):
        return {ds:[] for ds in self.ds_code}

    def get_join_cond(self):
        join_var=self.get_joining_var_dict()
        self.cond=self.cfg['JoinCondition']
        conds=self.cond.split("AND")
        for condition in conds:
            join_cols = condition.split('=')
            for col_detail in join_cols:
                col = col_detail.split('.')
                join_var[col[0].strip()].append(col[1].strip())
        return join_var

    
    def get_source(self, cfg):
        cfg_key = list(cfg.keys())
        if 'Alias' in cfg_key:
            exists_ds = list(self.dataset.keys())
            if cfg['Alias'] in exists_ds:
                print('Data {} exist in datasets.'.format(cfg['Alias']))
            else:
                print('Dataframe exists')
            self.ds_code.append(cfg['Alias'])
        else:
            from lambda_function import get_extract_from_src
            FM = get_extract_from_src(cfg=cfg,S3=self.S3)
            self.dataset[cfg['Name']] = FM.df
            self.ds_code.append(cfg['Name'])

    def get_column_list(self):
        return list(map(str.strip, self.cfg['Columns'].split(',')))

    def get_column_name(self):
        init_col_name = self.get_column_list()
        new_col_names = init_col_name.copy()
        for i in range(len(init_col_name)):
            if init_col_name[i].split('.')[-1] in self.ds_code:
                new_col_names[i] = '.'.join(init_col_name[i].split('.')[:-1])
        return dict(zip(init_col_name, new_col_names))
