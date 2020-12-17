# -*- coding: utf-8 -*-
"""
Created on Wed Dec  2 22:18:26 2020

@author: renli
"""
import numpy as np
import pandas as pd
import StructuredNote as SN

structurenote_mapper={
    '雪球':SN.SnowBall,
    '固定息票':SN.FixedCoupon,
    '凤凰':SN.Phoenix,
    '鲨鱼鳍':SN.Shark,
    }

def read_excel(path):
    profiles=pd.read_excel(path,sheet_name='条款',index_col=0)
    data=pd.read_excel(path,sheet_name='标的数据').set_index('日期').sort_index()
    structurenotes=[]
    for _,profile in profiles.iterrows():
        sn_type=profile.loc['凭证类型']
        sn=structurenote_mapper[sn_type]
        structurenotes.append(sn(profile,data))
    return structurenotes

def to_excel(path,structurenotes):
    profiles=[]
    warnings=[]
    for sn in structurenotes:
        profile,warning_series=sn.to_excel()
        profiles.append(profile)
        warnings.append(warning_series)
    profiles=pd.concat(profiles,axis=1).T
    warnings=pd.concat(warnings,axis=1).T
    warnings.index=profiles.index
    with pd.ExcelWriter(path) as writer:
        profiles.to_excel(writer,sheet_name='条款')
        structurenotes[0].data.loc[:,'收盘价格'].sort_index(ascending=False).to_excel(writer,sheet_name='标的数据')
        warnings.to_excel(writer,sheet_name='事件预警')
#%%
# =============================================================================
# sqlite operation
# =============================================================================
import json
from sqlalchemy import Column, ForeignKey, Integer, Float, String, Date, Boolean, Text
from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy import inspect

_db_path=r'sqlite:///..\Data/StructuredNoteServer.db'
engine = create_engine(_db_path,connect_args={'check_same_thread': False}) #default SQLite obj
conn = engine.connect()
metadata=MetaData(bind=engine)
metadata.reflect(bind=engine)
en_profile_columns=['key',
                    'Date',
                    'name',
                    'Type',
                    'Underlying',
                    'StartPrice',
                    'StartDate',
                    'LastObserveDate',
                    'Maturity',
                    'KnockOut',
                    'Strike',
                    'KnockIn',
                    'Rate',
                    'CouponCondition',
                    'CouponEvent',
                    'EarlyTerminateEvent',
                    'KnockInDate',
                    'EarlyTerminateDate',
                    'TerminateDate',
                    'TerminateValue',
                    'ParValue',
                    'ContractNumber',
                    'TerminateTotalValue',
                    'AgreedRate',
                    'ExpectedReturn',
                    'ReturnMultiplier',
                    'MinRate',
                    'BusinessDateInfer',
                    'TradingDateInfer',
                    'KnockOutObserveDate',
                    'KnockInObserveDate',
                    'CouponObserveDate',
                    'BusinessHoliday',
                    'TradingHoliday',
                    'LastUpdate']
ch_profile_columns=['index','当前日期','名称', '凭证类型', '挂钩标的', '期初价格', '期初观察日', '期末观察日', '到期日', '敲出水平', '行权水平',
       '敲入水平', '票面利率', '付息判断基准', '付息事件', '提前终止事件', '敲入日', '提前终止日', '终止日',
       '终止份额价值', '份额面值', '收益凭证份额', '终止兑付金额', '约定收益率', '期望涨幅', '涨幅差乘数', '最低收益率',
       '自动推断营业日', '自动推断交易日', '敲出观察日', '敲入观察日', '付息观察日', '营业日节假日', '交易日节假日',
       '最后更新日期']
en_warning_columns=['key','PriceLevel','Value','WarningType','IsTerminated','DaysToKnockIn','PriceToKnockIn',
                     'IsKnockIn','DaysToKnockOut','PriceToKnockOut','IsKnockOut',
                     'DaysToCoupon','PriceToCoupon','Coupon','DaysToMaturity']
ch_warning_columns=['index','价格水平', '份额价值', '预警类型', '是否终止', '距离敲入日天数', '距离敲入价格水平', '是否敲入',
       '距离敲出日天数', '距离敲出价格水平', '是否敲出', '距离付息日天数', '距离付息基准价格水平', '付息金额',
       '距离到期日天数']
profile_en_to_ch=dict(zip(en_profile_columns,ch_profile_columns))
profile_ch_to_en=dict(zip(ch_profile_columns,en_profile_columns))

def create_profile_table(engine=engine):
    profile_table=Table('Profile',metadata,
                        Column('key',Integer,primary_key=True,nullable=False,autoincrement=True),
                        Column('Date',Date,nullable=False),
                        Column('name',String(50),nullable=False),
                        Column('Type',String(50),nullable=False),
                        Column('Underlying',String(50),nullable=False),
                        Column('StartPrice',Float),
                        Column('StartDate',Date,nullable=False),
                        Column('LastObserveDate',Date),
                        Column('Maturity',Date,nullable=False),
                        Column('KnockOut',Float),
                        Column('Strike',Float),
                        Column('KnockIn',Float),
                        Column('Rate',Float),
                        Column('CouponCondition',Float),
                        Column('CouponEvent',String(50)),
                        Column('EarlyTerminateEvent',String(50)),
                        Column('KnockInDate',Date),
                        Column('EarlyTerminateDate',Date),
                        Column('TerminateDate',Date),
                        Column('TerminateValue',Float),
                        Column('ParValue',Float,nullable=False),
                        Column('ContractNumber',Float,nullable=False),
                        Column('TerminateTotalValue',Float),
                        Column('AgreedRate',Float),
                        Column('ExpectedReturn',Float),
                        Column('ReturnMultiplier',Float),
                        Column('MinRate',Float),
                        Column('BusinessDateInfer',Boolean),
                        Column('TradingDateInfer',Boolean),
                        Column('KnockOutObserveDate',Text),
                        Column('KnockInObserveDate',Text),
                        Column('CouponObserveDate',Text),
                        Column('BusinessHoliday',Text),
                        Column('TradingHoliday',Text),
                        Column('LastUpdate',Date),
                        )
    profile_table.create(engine)
    
def create_price_table(engine=engine):
    price_table=Table('Price',metadata,
                     Column('Date',Date,primary_key=True,nullable=False),
                     Column('000905',Float)
                     )
    price_table.create(engine)
    
def create_warning_table(engine=engine):
    warning_table=Table('Warning',metadata,
                        Column('key',ForeignKey('Profile.key'),primary_key=True,nullable=False),
                        Column('PriceLevel',Float),
                        Column('Value',Float),
                        Column('WarningType',String(50)),
                        Column('IsTerminated',Boolean),
                        Column('DaysToKnockIn',Integer),
                        Column('PriceToKnockIn',Float),
                        Column('IsKnockIn',Boolean),
                        Column('DaysToKnockOut',Integer),
                        Column('PriceToKnockOut',Float),
                        Column('IsKnockOut',Boolean),
                        Column('DaysToCoupon',Integer),
                        Column('PriceToCoupon',Float),
                        Column('Coupon',Float),
                        Column('DaysToMaturity',Integer),
                        )
    warning_table.create(engine)

def clear_table(table_name, metadata, engine, clear=False):
    """
    'clear = True' means clear all tables found in metadata.
    """
    # conn = engine.connect()
    if not clear:
        table = metadata.tables[table_name]
        delete_st = table.delete()
        engine.execute(delete_st)
    else:
        for table_name in metadata.tables.keys():
            table = metadata.tables[table_name]
            delete_st = table.delete()
            engine.execute(delete_st)
            
def delete_all_table(metadata=metadata):
    metadata.drop_all()
    
def execute_sql(sql_stmt, engine, parse_dates=None, change=False):
        if change:
            engine.execute(sql_stmt)
        else:
            result_df=pd.read_sql(sql_stmt,engine,parse_dates=parse_dates)
            return result_df
        
def read_db(engine=engine):
    profiles=execute_sql('Select * From Profile',engine,parse_dates=['Date','StartDate','LastObserveDate',
                                                                     'Maturity','KnockInDate','EarlyTerminateDate',
                                                                     'TerminateDate','LastUpdate'])
    data=execute_sql('Select * From Price',engine,parse_dates=['Date'])
    warnings=execute_sql('Select * From Warning',engine)
    profiles.columns=ch_profile_columns
    profiles.set_index('index',drop=True,inplace=True)
    data.columns=['日期','收盘价格']
    data.set_index('日期',drop=True,inplace=True)
    warnings.columns=ch_warning_columns
    warnings.set_index('index',drop=True,inplace=True)
    structurenotes=[]
    for _,profile in profiles.iterrows():
        sn_type=profile.loc['凭证类型']
        sn=structurenote_mapper[sn_type]
        structurenotes.append(sn(profile,data))
    return structurenotes

def update_db(df,engine,table_name,key,auto_key=False):
    """
    append df to exsiting table without duplication.
    if auto_key: all columns except key will be appended to exsiting table, 
    but key will be generated automatically by the db.
    """
    if not auto_key:
        df.reset_index().to_sql('__temp', conn, if_exists='replace',index=False)
        engine.execute('Delete From '+table_name+' Where '+
                   key+' in (Select '+key+' From __temp)')
    else:
        df.reset_index(inplace=True)
        df[key]=None #assign None to key will make them auto-generated when inserted to the table, because key isn't nullable
        df.to_sql('__temp', conn, if_exists='replace',index=False)
    
    engine.execute('Insert Into '+table_name+' Select * From __temp')
    engine.execute('Drop Table If Exists __temp')
    
def to_db(structurenotes,auto_key=False,engine=engine):
    profiles=[]
    warnings=[]
    for sn in structurenotes:
        profile,warning_series=sn.to_excel()
        profiles.append(profile)
        warnings.append(warning_series)
    profiles=pd.concat(profiles,axis=1).T
    data=pd.DataFrame(structurenotes[0].data.loc[:,'收盘价格'])
    warnings=pd.concat(warnings,axis=1).T
    warnings.index=profiles.index
    profiles.index.name=en_profile_columns[0]
    profiles.columns=en_profile_columns[1:]
    data.index.name='Date'
    data.columns=['000905']
    warnings.index.name=en_warning_columns[0]
    warnings.columns=en_warning_columns[1:]
    warnings.loc[:,['DaysToKnockIn','DaysToKnockOut','DaysToCoupon','DaysToMaturity']]=warnings.loc[:,[
        'DaysToKnockIn','DaysToKnockOut','DaysToCoupon','DaysToMaturity']].astype(float)
    update_db(profiles,engine,'Profile','key',auto_key)
    update_db(data,engine,'Price','Date')
    update_db(warnings,engine,'Warning','key',auto_key)
    
def calc_db(key_list,untill_date=None,engine=engine):
    """
    Calculate structurenote' status until system's today and update in database.
    
    key_list: list
        which rows in Profile will be calculate
    """
    import datetime as dt
    if untill_date is None:
        untill_date=pd.to_datetime(dt.date.today())
    else:
        untill_date=pd.to_datetime(untill_date)
    key_list=list(map(str,key_list))
    profiles=execute_sql('Select * From Profile Where key In '+'('+','.join(key_list)+')',
                         engine,parse_dates=['Date','StartDate','LastObserveDate','Maturity',
                                             'KnockInDate','EarlyTerminateDate','TerminateDate','LastUpdate'])
    profiles.loc[:,'Date']=untill_date
    data=execute_sql('Select * From Price',engine,parse_dates=['Date'])
    profiles.columns=ch_profile_columns
    profiles.set_index('index',drop=True,inplace=True)
    data.columns=['日期','收盘价格']
    data.set_index('日期',drop=True,inplace=True)
    structurenotes=[]
    for _,profile in profiles.iterrows():
        sn_type=profile.loc['凭证类型']
        sn=structurenote_mapper[sn_type]
        structurenotes.append(sn(profile,data))
    SN.update_structurenotes(structurenotes)
    to_db(structurenotes,False,engine)
#%%
# =============================================================================
# sqlite operation (for flask)
# =============================================================================
def fetch_db(table_name,columns,match,order,start_i=None,end_i=None,engine=engine):
    """

    Parameters
    ----------
    table_name : str
        table_name, e.g. 'Profile' or 'select * from Profile, Warning where Profile.ID==Warning.ID'
    columns : list
        returned column, a dictionary of column names, e.g. ['Date','Type','PriceLevel']
    match : dict
        Where equality condition. E.g. {'Type':'雪球','LastUpdate':'2019-03-17'} -> 
        "where Type=='雪球' and LastUpdate=='2019-03-17'"
    order : dict
        Sorting criteria, {'LastUpdate':'asc','key':'desc'} -> "order by LastUpdate ASC,ID DESC". 
        'asc'/'desc' case doesn't matter.
    start_i : int, end_i : int
        Select [start_i, end_i) rows. "Limit start_i,(end_i-start_i)"
    engine : sqlalchemy.engine, optional
        The default is the engine defined in this file.

    Returns
    -------
    pd.DataFrame
    Example:
        df=fetch_db('select * from Profile,Warning where Profile.ID==Warning.ID',['*'],{},['LastUpdate'])
    
    """
    select_stmt=' '.join(['Select',*columns,'From','(',table_name,')'])
    if match:
        where_stmt='Where '+' And '.join((key+'=='+"'"+str(val)+"'" for key,val in match.items()))
    else:
        where_stmt=''
    if order:
        order_stmt='Order by '+','.join((' '.join([key,val.upper()]) for key,val in order.items()))
    else:
        order_stmt=''
    if (start_i is not None) and (end_i is not None):
        limit_stmt='Limit '+str(start_i)+','+str(end_i-start_i)
    else:
        limit_stmt=''
    sql_stmt=' '.join([select_stmt,where_stmt,order_stmt,limit_stmt])
    df=execute_sql(sql_stmt,engine)
    return df

def fetch_length(table_name,engine=engine):
    sql_stmt='Select Count(*) from '+table_name
    df=execute_sql(sql_stmt,engine)
    return int(df.iloc[0,0])

def delete_rows(table_name,match,engine=engine):
    """
    
    Parameters
    ----------
    table_name : str
    match : dict{str->list}
        Where equality condition to delete. E.g. {'key':[1,3]} -> "where key in (1,3)"
    engine : sqlalchemy.engine, optional
        The default is the engine defined in this file.
    """
    del_stmt='Delete From '+table_name+' Where '+' And '.join((key+' In '+'('+
                                                               ','.join(map(lambda x:"'"+str(x)+"'",val))+
                                                               ')' for key,val in match.items()))
    execute_sql(del_stmt,engine,change=True)
