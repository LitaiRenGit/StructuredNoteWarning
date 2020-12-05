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