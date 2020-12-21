# -*- coding: utf-8 -*-
"""
Created on Fri Dec 18 10:19:17 2020

@author: renli
"""
import numpy as np
import pandas as pd
import ReadFiles as RF
from WindPy import w
import backoff

w.start()
i=5
while (not w.isconnected()) and i>0:
    print('try reconnecting to Wind, {} trials left.'.format(i))
    w.start()
    i-=1

def get_closeprice_series(codes,beginTime,endTime=None):
    """
    Get one or multiple daily close price series (trading days).
    Return pd.DataFrame with {columns=codes, index=date}, all-missing row will be filtered.
    Default endTime is current date.
    """
    WindData=w.wsd(codes,'CLOSE',beginTime,endTime,"Days=Trading")
    #decode WindData
    data=np.array(WindData.Data).T
    dates=pd.to_datetime(WindData.Times)
    df=pd.DataFrame(data,columns=WindData.Codes,index=dates)
    df.index.name='Date'
    return df

def retrieve_000905(T:int=7,path=r'../Data\000905.csv'):
    df=get_closeprice_series('000905.SH','-'+str(T)+'D')
    df.to_csv(path)
    now=pd.Timestamp.now()
    flag_table=RF.metadata.tables['Flag']
    sql=flag_table.insert().values({'DateTime':now,'000905.SH_isRetrieve':True})
    RF.engine.execute(sql)
    
def is_retrieved(codes):
    """
    Check if price data corresponding to given codes have been retrieved today.
    
    Parameters
    ----------
    codes : list
        Like ['000905.SH',...].

    Returns
    -------
    List of True/False.

    """
    columns=['DateTime']
    columns.extend(['`'+ele+'_isRetrieve`' for ele in codes])
    today=pd.Timestamp.now().date()
    df=RF.fetch_db('Flag', columns, {'DATE(DateTime)':today}, {}, {}).set_index('DateTime')
    return df.any().to_list()

def is_updated(codes):
    """
    Check if price data corresponding to given codes have been updated today.
    
    Parameters
    ----------
    codes : list
        Like ['000905.SH',...].

    Returns
    -------
    List of True/False.

    """
    columns=['DateTime']
    columns.extend(['`'+ele+'_isUpdate`' for ele in codes])
    today=pd.Timestamp.now().date()
    df=RF.fetch_db('Flag', columns, {'DATE(DateTime)':today}, {}, {}).set_index('DateTime')
    return df.any().to_list()
    
def update_db_000905(path=r'../Data\000905.csv'):
    df=pd.read_csv(path).set_index('Date')
    df.index=pd.to_datetime(df.index)
    RF.update_db(df,RF.engine,'Price','Date')
    now=pd.Timestamp.now()
    flag_table=RF.metadata.tables['Flag']
    sql=flag_table.insert().values({'DateTime':now,'000905.SH_isUpdate':True})
    RF.engine.execute(sql)
    
# @backoff.on_predicate(backoff.constant, lambda x:True,interval=3600) #try once a hour to try to retrieve data
# @backoff.on_predicate(backoff.constant, lambda x:not x,max_tries=5,interval=5) #try 5 times to retrieve data
def polling_000905(T:int=7):
    # print('run polling_000905')
    retrieve_000905(T)
    is_retrieved_000905=is_retrieved(['000905.SH'])[0]
    is_updated_000905=is_updated(['000905.SH'])[0]
    if is_retrieved_000905:
        if not is_updated_000905:
            update_db_000905()
    return is_updated_000905