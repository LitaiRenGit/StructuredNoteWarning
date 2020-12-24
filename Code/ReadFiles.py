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
        structurenotes.append(sn(profile,data.copy()))
    return structurenotes,data

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
from sqlalchemy import Column, ForeignKey, Integer, Float, String, Date, DateTime, Boolean, Text
from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy import inspect

_db_default_path=r'sqlite:///..\Data/StructuredNoteServer.db'
engine = create_engine(_db_default_path,connect_args={'check_same_thread': False}) #default SQLite obj
conn = engine.connect()
metadata=MetaData(bind=engine)
metadata.reflect()
_en_profile_columns=['key',
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
_ch_profile_columns=['index','当前日期','名称', '凭证类型', '挂钩标的', '期初价格', '期初观察日', '期末观察日', '到期日', '敲出水平', '行权水平',
       '敲入水平', '票面利率', '付息判断基准', '付息事件', '提前终止事件', '敲入日', '提前终止日', '终止日',
       '终止份额价值', '份额面值', '收益凭证份额', '终止兑付金额', '约定收益率', '期望涨幅', '涨幅差乘数', '最低收益率',
       '自动推断营业日', '自动推断交易日', '敲出观察日', '敲入观察日', '付息观察日', '营业日节假日', '交易日节假日',
       '最后更新日期']
_en_warning_columns=['key','PriceLevel','Value','WarningType','IsTerminated','DaysToKnockIn','PriceToKnockIn',
                     'IsKnockIn','DaysToKnockOut','PriceToKnockOut','IsKnockOut',
                     'DaysToCoupon','PriceToCoupon','Coupon','DaysToMaturity']
_ch_warning_columns=['index','价格水平', '份额价值', '预警类型', '是否终止', '距离敲入日天数', '距离敲入价格水平', '是否敲入',
       '距离敲出日天数', '距离敲出价格水平', '是否敲出', '距离付息日天数', '距离付息基准价格水平', '付息金额',
       '距离到期日天数']
_en_profile_date_columns=['Date','StartDate','LastObserveDate','Maturity','KnockInDate',
                         'EarlyTerminateDate','TerminateDate','LastUpdate']
profile_en_to_ch=dict(zip(_en_profile_columns,_ch_profile_columns))
profile_ch_to_en=dict(zip(_ch_profile_columns,_en_profile_columns))

def create_profile_table(metadata=metadata,engine=engine):
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
    
def create_price_table(metadata=metadata,engine=engine):
    price_table=Table('Price',metadata,
                     Column('Date',Date,primary_key=True,nullable=False),
                     Column('000905.SH',Float)
                     )
    price_table.create(engine)
    
def create_warning_table(metadata=metadata,engine=engine):
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
    
def create_flag_table(metadata=metadata,engine=engine):
    flag_table=Table('Flag',metadata,
                     Column('DateTime',DateTime,primary_key=True,nullable=False),
                     Column('000905.SH_isRetrieve',Boolean),
                     Column('000905.SH_isUpdate',Boolean),
                     extend_existing=True, #allow this table to extend if more underlying are imported
                     )
    flag_table.create(engine)

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
            
def delete_table(table_names,metadata=metadata,all_table=False):
    if all_table:
        metadata.drop_all()
        return
    else:
        for name in table_names:
            table=metadata.tables[name]
            metadata.remove(table)
            table.drop()
        return
    
def execute_sql(sql_stmt, engine, parse_dates=None, change=False):
        if change:
            engine.execute(sql_stmt)
        else:
            result_df=pd.read_sql(sql_stmt,engine,parse_dates=parse_dates)
            return result_df
        
def read_db(engine=engine):
    profiles=execute_sql('Select * From Profile',engine,parse_dates=_en_profile_date_columns)
    data=execute_sql('Select * From Price',engine,parse_dates=['Date'])
    warnings=execute_sql('Select * From Warning',engine)
    profiles.columns=_ch_profile_columns
    profiles.set_index('index',drop=True,inplace=True)
    data.columns=['日期','收盘价格']
    data.set_index('日期',drop=True,inplace=True)
    warnings.columns=_ch_warning_columns
    warnings.set_index('index',drop=True,inplace=True)
    structurenotes=[]
    for _,profile in profiles.iterrows():
        sn_type=profile.loc['凭证类型']
        sn=structurenote_mapper[sn_type]
        structurenotes.append(sn(profile,data.copy()))
    return structurenotes

def update_db(df,engine,table_name,key,auto_key=False):
    """
    append df to exsiting table without duplication.
    if auto_key: all columns except key will be appended to exsiting table, 
    but key will be generated automatically by the db.
    """
    if not auto_key:
        df.reset_index(inplace=True)
        df.to_sql('__temp', conn, if_exists='replace',index=False)
        engine.execute('Delete From '+table_name+' Where '+
                   key+' in (Select '+key+' From __temp)')
        engine.execute('Insert Into '+table_name+' Select * From __temp')
        last_key=df[key].to_list()
        engine.execute('Drop Table If Exists __temp')
    else:
        df.reset_index(inplace=True)
        df[key]=None #assign None to key will make them auto-generated when inserted to the table, because key isn't nullable
        df.to_sql(table_name, conn, if_exists='append',index=False,method='multi')
        table_length=int(execute_sql('Select Count(*) from '+table_name,engine).iloc[0,0])
        insert_length=df.shape[0]
        last_key=execute_sql('Select key From '+table_name+' Limit '+
                             str(table_length-insert_length)+','+str(insert_length),engine).iloc[:,0].to_list()
    return last_key
    
def to_db(structurenotes,auto_key=False,engine=engine):
    profiles=[]
    warnings=[]
    for sn in structurenotes:
        profile,warning_series=sn.to_excel()
        profiles.append(profile)
        warnings.append(warning_series)
    profiles=pd.concat(profiles,axis=1).T
    warnings=pd.concat(warnings,axis=1).T
    warnings.index=profiles.index
    profiles.index.name=_en_profile_columns[0]
    profiles.columns=_en_profile_columns[1:]
    warnings.index.name=_en_warning_columns[0]
    warnings.columns=_en_warning_columns[1:]
    warnings.loc[:,['DaysToKnockIn','DaysToKnockOut','DaysToCoupon','DaysToMaturity']]=warnings.loc[:,[
        'DaysToKnockIn','DaysToKnockOut','DaysToCoupon','DaysToMaturity']].astype(float)
    profile_last_key=update_db(profiles,engine,'Profile','key',auto_key)
    warning_last_key=update_db(warnings,engine,'Warning','key',auto_key)
    assert profile_last_key==warning_last_key #if crashes here, newly-inserted data get different keys in Profile and Warning tables.
    return profile_last_key

def price_to_db(data,engine=engine):
    data=data.loc[:,['收盘价格']] #data remains dataframe
    data.index.name='Date'
    data.columns=['000905.SH']
    update_db(data,engine,'Price','Date')
    
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
                         engine,parse_dates=_en_profile_date_columns)
    profiles.loc[:,'Date']=untill_date
    data=execute_sql('Select * From Price',engine,parse_dates=['Date'])
    profiles.columns=_ch_profile_columns
    profiles.set_index('index',drop=True,inplace=True)
    data.columns=['日期','收盘价格']
    data.set_index('日期',drop=True,inplace=True)
    structurenotes=[]
    for _,profile in profiles.iterrows():
        sn_type=profile.loc['凭证类型']
        sn=structurenote_mapper[sn_type]
        structurenotes.append(sn(profile,data.copy()))
    SN.update_structurenotes(structurenotes)
    to_db(structurenotes,False,engine)
#%%
# =============================================================================
# sqlite operation (for flask)
# =============================================================================
def fetch_db(table_name,columns,match,like,order,start_i=None,end_i=None,engine=engine):
    """

    Parameters
    ----------
    table_name : str
        table_name, e.g. 'Profile' or 'select * from Profile, Warning where Profile.ID==Warning.ID'
    columns : list
        returned column, a dictionary of column names, e.g. ['Date','Type','PriceLevel']
    match : dict
        Where Like condition. E.g. {'Type':'雪球','LastUpdate':'2019-03-17'} -> 
        "where Type = '雪球' and LastUpdate = '2019-03-17'"
    like : dict
        Where Like condition. E.g. {'Type':'雪球','LastUpdate':'2019-03-17'} -> 
        "where Type Like '%雪球%' and LastUpdate Like '%2019-03-17%'"
    order : dict
        Sorting criteria, {'LastUpdate':'asc','key':'desc'} -> "order by LastUpdate ASC,ID DESC". 
        'asc'/'desc' case doesn't matter.
    start_i : int, end_i : int
        Select [start_i, end_i) rows. "Limit start_i,(end_i-start_i)", start from 0.
    engine : sqlalchemy.engine, optional
        The default is the engine defined in this file.

    Returns
    -------
    pd.DataFrame
    Example:
        df=fetch_db('select * from Profile,Warning where Profile.ID==Warning.ID',['*'],{},{},{'LastUpdate':'DESC'})
    
    """
    select_stmt=' '.join(['Select',','.join(columns),'From','(',table_name,')'])
    if match or like:
        if match:
            match_stmt=' And '.join((key+'='+"'"+str(val)+"'" for key,val in match.items()))
        else:
            match_stmt=''
        if like:
            like_stmt=' And '.join((key+' Like '+"'%"+str(val)+"%'" for key,val in like.items()))
        else:
            like_stmt=''
        where_stmt=' '.join(['Where',match_stmt,like_stmt])
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
    """
    Fetch the length of the specified table.
    """
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
    
def add_row(json_dict,engine=engine):
    """
    Add one structurenote to database (1 row), params given by frontend.
    Database automatically generates the key.
    """
    profiles=pd.Series(index=_en_profile_columns)
    profiles.update(json_dict)
    profiles.loc[_en_profile_date_columns]=pd.to_datetime(profiles.loc[_en_profile_date_columns])
    profiles.loc['key']=0
    profiles=profiles.to_frame().T
    profiles.columns=_ch_profile_columns
    profiles.set_index('index',drop=True,inplace=True)
    data=execute_sql('Select * From Price',engine,parse_dates=['Date'])
    data.columns=['日期','收盘价格']
    data.set_index('日期',drop=True,inplace=True)
    structurenotes=[]
    for _,profile in profiles.iterrows():
        sn_type=profile.loc['凭证类型']
        sn=structurenote_mapper[sn_type]
        structurenotes.append(sn(profile,data.copy()))
    SN.update_structurenotes(structurenotes)
    key=to_db(structurenotes,True,engine)
    return key
#%%
# =============================================================================
# mock data
# =============================================================================
def mock_profiles(num,seed=None):
    import datetime as dt
    import random
    from itertools import repeat
    from pandas.tseries.offsets import Week
    
    def _monthly_bd_gen(start_date,end_date):
        date_series=pd.date_range(start_date,end_date,freq='M').to_series()
        date_series=date_series.apply(lambda x: x+Week(2,weekday=2))# 每月第二个周三
        date_str=','.join(date_series.apply(lambda x:x.strftime('%Y/%m/%d')))
        return date_str
    
    def _mock_profile(seed=None):
        random.seed(seed)
        profile=pd.Series(np.nan,index=_ch_profile_columns)
        profile['index']=0
        if random.random()<0.5:
            #50% 可能性当前日期为以下区间
            date_range=[dt.date(2015,1,1),dt.date(2020,12,1)]
            ordinal_range=list(map(lambda x:x.toordinal(),date_range))
            profile['当前日期']=pd.to_datetime(dt.date.fromordinal(random.randint(ordinal_range[0],ordinal_range[1])))
        else:
            #剩余可能性为系统今天
            profile['当前日期']=pd.to_datetime(dt.date.today())
        profile['挂钩标的']='中证500指数（000905.SH）'
        profile['凭证类型']=random.choice(list(structurenote_mapper.keys()))
        contract_life=random.choice([1,2])
        date=profile['当前日期'].date()
        ordinal_range=[date.toordinal()-365*contract_life,date.toordinal()]
        profile['期初观察日']=pd.to_datetime(dt.date.fromordinal(random.randint(ordinal_range[0],ordinal_range[1])))
        profile['到期日']=pd.to_datetime(dt.date.fromordinal(profile['期初观察日'].date().toordinal()+365*contract_life))
        if profile['凭证类型']=='鲨鱼鳍':
            profile['期末观察日']=profile['到期日']-Week(1,weekday=4) #到期日前一个周五
        profile['敲出水平']=1+random.random()*0.1 #in [1,1.1]
        profile['行权水平']=1
        profile['敲入水平']=0.7+random.random()*0.15 #in [0.7,0.85]
        if profile['凭证类型']=='凤凰':
            profile['票面利率']=0.015+random.random()*0.01 #in [0.015,0.025]
        else:
            profile['票面利率']=0.07+random.random()*0.02 #in [0.07,0.09]
        if profile['凭证类型']=='凤凰':
            profile['付息判断基准']=profile['敲入水平']
        profile['份额面值']=100
        profile['收益凭证份额']=random.randint(1, 10)
        if profile['凭证类型']=='鲨鱼鳍':
            profile['约定收益率']=0.02+random.random()*0.04 #in [0.02,0.06]
            profile['期望涨幅']=0.5+random.random()*1 #in [0.5,1.5]
            profile['涨幅差乘数']=1+random.random()*4 #in [1,5]
            profile['最低收益率']=0+random.random()*0.01 #in [0,0.01]
        profile['自动推断营业日']=0
        profile['自动推断交易日']=1
        if profile['凭证类型']!='鲨鱼鳍':
            profile['敲出观察日']=_monthly_bd_gen(profile['期初观察日'],profile['到期日'])
            profile['敲入观察日']='all'
        if profile['凭证类型']=='凤凰':
            profile['付息观察日']=profile['敲出观察日']
        profile['名称']=''.join(['中证',profile['凭证类型'],profile['期初观察日'].strftime('%y%m')])
        return profile
        
    profiles=pd.DataFrame([],columns=_ch_profile_columns)
    index=list(range(1,num+1))
    profiles['index']=index
    if seed is None:
        seed_series=repeat(None)
    else:
        np.random.seed(seed)
        seed_series=np.random.randint(0,100000000,size=num)
    for i,s in zip(index,seed_series):
        profiles.iloc[i-1,:]=_mock_profile(s)
    profiles['index']=index
    profiles.set_index('index',inplace=True)
    return profiles

def mock_structurenotes(num,seed=None):
    from pandas.tseries.offsets import Day
    profiles=mock_profiles(num,seed)
    data=execute_sql('Select * From Price',engine,parse_dates=['Date'])
    data.columns=['日期','收盘价格']
    data.set_index('日期',drop=True,inplace=True)
    structurenotes=[]
    for i,profile in profiles.iterrows():
        sn_type=profile.loc['凭证类型']
        sn=structurenote_mapper[sn_type]
        structurenotes.append(sn(profile,data[data.index>=(profile['期初观察日']-Day(30))]))
    return structurenotes
