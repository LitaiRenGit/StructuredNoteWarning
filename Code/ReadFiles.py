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

# =============================================================================
# sqlite operation
# =============================================================================
import json
from sqlalchemy import Column, ForeignKey, Integer, Float, String, Date, Boolean, Text
from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy import inspect

engine = create_engine(r'sqlite:///..\Data/StructuredNoteServer.db')
conn = engine.connect()
metadata=MetaData()
metadata.reflect(bind=engine)

def create_profile_table():
    profile_table=Table('Profile',metadata,
                        Column('ID',Integer,primary_key=True,nullable=False,autoincrement=True),
                        Column('Date',Date,nullable=False),
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
    
def create_price_table():
    price_table=Table('Price',metadata,
                     Column('Date',Date,primary_key=True,nullable=False),
                     Column('000905',Float)
                     )
    price_table.create(engine)
    
def create_warning_table():
    warning_table=Table('Warning',metadata,
                        Column('ID',ForeignKey('Profile.ID'),primary_key=True,nullable=False),
                        Column('PriceLevel',Float),
                        Column('Value',Float),
                        Column('Type',String(50)),
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
    conn = engine.connect()
    if not clear:
        table = metadata.tables[table_name]
        delete_st = table.delete()
        conn.execute(delete_st)
    else:
        for table_name in metadata.tables.keys():
            table = metadata.tables[table_name]
            delete_st = table.delete()
            conn.execute(delete_st)
    conn.close()
    
def execute_sql(sql_stmt, engine, parse_dates=None, change=False):
        if change:
            engine.execute(sql_stmt)
        else:
            # result_set = engine.execute(sql_stmt)
            # result_df = pd.DataFrame(result_set.fetchall())
            # if not result_df.empty:
            #     result_df.columns = result_set.keys()
            result_df=pd.read_sql(sql_stmt,engine,parse_dates=parse_dates)
            return result_df
        
def read_db():
    profiles=execute_sql('Select * From Profile',engine,parse_dates=['Date','StartDate','LastObserveDate',
                                                                     'Maturity','KnockInDate','EarlyTerminateDate',
                                                                     'TerminateDate','LastUpdate'])
    data=execute_sql('Select * From Price',engine,parse_dates=['Date'])
    warnings=execute_sql('Select * From Warning',engine)
    profiles.columns=['index','当前日期', '凭证类型', '挂钩标的', '期初价格', '期初观察日', '期末观察日', '到期日', '敲出水平', '行权水平',
       '敲入水平', '票面利率', '付息判断基准', '付息事件', '提前终止事件', '敲入日', '提前终止日', '终止日',
       '终止份额价值', '份额面值', '收益凭证份额', '终止兑付金额', '约定收益率', '期望涨幅', '涨幅差乘数', '最低收益率',
       '自动推断营业日', '自动推断交易日', '敲出观察日', '敲入观察日', '付息观察日', '营业日节假日', '交易日节假日',
       '最后更新日期']
    profiles.set_index('index',drop=True,inplace=True)
    data.columns=['日期','收盘价格']
    data.set_index('日期',drop=True,inplace=True)
    warnings.columns=['index','价格水平', '份额价值', '预警类型', '是否终止', '距离敲入日天数', '距离敲入价格水平', '是否敲入',
       '距离敲出日天数', '距离敲出价格水平', '是否敲出', '距离付息日天数', '距离付息基准价格水平', '付息金额',
       '距离到期日天数']
    warnings.set_index('index',drop=True,inplace=True)

    structurenotes=[]
    for _,profile in profiles.iterrows():
        sn_type=profile.loc['凭证类型']
        sn=structurenote_mapper[sn_type]
        structurenotes.append(sn(profile,data))
    return structurenotes

def update_db(df,engine,table_name,key):
    """
    append df to exsiting table without duplication
    """
    df.to_sql('__temp', conn, if_exists='replace')
    engine.execute('Delete From '+table_name+' Where '+
                   key+' in (Select '+key+' From __temp)')
    engine.execute('Insert Into '+table_name+' Select * From __temp')
    engine.execute('Drop Table If Exists __temp')
    
    
def to_db(structurenotes):
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
    profiles.index.name='ID'
    profiles.columns=['Date','Type','Underlying','StartPrice','StartDate','LastObserveDate',
                      'Maturity','KnockOut','Strike','KnockIn','Rate','CouponCondition',
                      'CouponEvent','EarlyTerminateEvent','KnockInDate','EarlyTerminateDate',
                      'TerminateDate','TerminateValue','ParValue','ContractNumber',
                      'TerminateTotalValue','AgreedRate','ExpectedReturn','ReturnMultiplier',
                      'MinRate','BusinessDateInfer','TradingDateInfer','KnockOutObserveDate',
                      'KnockInObserveDate','CouponObserveDate','BusinessHoliday',
                      'TradingHoliday','LastUpdate']
    data.index.name='Date'
    data.columns=['000905']
    warnings.index.name='ID'
    warnings.columns=['PriceLevel','Value','Type','IsTerminated','DaysToKnockIn','PriceToKnockIn',
                     'IsKnockIn','DaysToKnockOut','PriceToKnockOut','IsKnockOut',
                     'DaysToCoupon','PriceToCoupon','Coupon','DaysToMaturity']
    warnings.loc[:,['DaysToKnockIn','DaysToKnockOut','DaysToCoupon','DaysToMaturity']]=warnings.loc[:,[
        'DaysToKnockIn','DaysToKnockOut','DaysToCoupon','DaysToMaturity']].astype(float)
    update_db(profiles,engine,'Profile','ID')
    update_db(data,engine,'Price','Date')
    update_db(warnings,engine,'Warning','ID')
