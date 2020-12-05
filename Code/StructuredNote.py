# -*- coding: utf-8 -*-
"""
Created on Mon Nov 30 10:16:53 2020

@author: renli
"""
import numpy as np
import pandas as pd
import Event
from pandas.tseries.offsets import CustomBusinessDay

class StructuredNote:
    
    def __init__(self,profile,data):
        self.profile=profile
        self.data=data #it must be ascendingly ordered.
        self.bd_holidays=self.decode_date(self.profile['营业日节假日'],'营业日节假日')
        self.td_holidays=self.decode_date(self.profile['交易日节假日'],'交易日节假日')
        self.holidays=pd.Series(list(set(self.bd_holidays).union(set(self.td_holidays)))).sort_values().reset_index(drop=True)
        self.knockout_dates=self.decode_date(self.profile['敲出观察日'],'敲出观察日')
        self.knockin_dates=self.decode_date(self.profile['敲入观察日'],'敲入观察日')
        self.coupon_dates=self.decode_date(self.profile['付息观察日'],'付息观察日').apply(self.bd_offset)#use business date to offset coupon date
        infer_bd=self.profile.loc['自动推断营业日']
        infer_td=self.profile.loc['自动推断交易日']
        if infer_bd or infer_td: self.holidays_infer(infer_bd,infer_td)
        self.start_date=self.bd_td_offset(pd.to_datetime(self.profile.loc['期初观察日']))
        self.end_date=self.bd_td_offset(pd.to_datetime(self.profile.loc['到期日']))
        if not self.knockout_dates.empty:
            if self.knockout_dates.iloc[0]=='all':
                self.isAllKockOut=True
                self.knockout_dates=self.create_all_td()
            else:
                self.isAllKockOut=False
                self.knockout_dates=self.knockout_dates.apply(self.bd_td_offset)#use business&trading date to offset knockout
        else: 
            self.isAllKockOut=False
        if not self.knockin_dates.empty:
            if self.knockin_dates.iloc[0]=='all':
                self.isAllKockIn=True
                self.knockin_dates=self.create_all_td()
            else:
                self.isAllKockIn=False
                self.knockin_dates=self.knockin_dates.apply(self.td_offset)#use trading date to offset knockin
        else: 
            self.isAllKockIn=False
        self.today=pd.to_datetime(self.profile.loc['当前日期'])
        self.start_price=self.data.loc[self.start_date,'收盘价格'] if pd.isna(
            self.profile.loc['期初价格']) else self.profile.loc['期初价格']
        self.data.loc[:,'收益表现水平']=self.data.loc[:,'收盘价格']/self.start_price
        self.is_terminated=False
        
    def decode_date(self,date_str,name):
        if pd.isna(date_str):
            return pd.Series([],name=name)
        elif date_str=='all':
            return pd.Series(['all'],name=name)
        else:
            return pd.Series(pd.to_datetime(date_str.split(',')),name=name)
        
    def encode_date(self,date_series):
        if date_series.empty:
            return np.nan
        elif date_series.iloc[0]=='all':
            return 'all'
        else:
            return ','.join(date_series.apply(lambda x:x.strftime('%Y/%m/%d')))
        
    def _init_currentclass(self):
        """
        for overloading, additional initiate function after super().__init__()
        """
        return
            
    def __str__(self):
        return str(self.profile)
    
    def bd_offset(self,date):
        """
        offset date to the next business day
        """
        bd=CustomBusinessDay(0,holidays=self.bd_holidays)
        return date+bd
    
    def td_offset(self,date):
        """
        offset date to the next trading day or itself
        """
        td=CustomBusinessDay(0,holidays=self.td_holidays)
        return date+td
    
    def td_backward_offset(self,date):
        """
        offset date to the previous trading day or itself
        """
        td0=CustomBusinessDay(0,holidays=self.td_holidays)
        td1=CustomBusinessDay(1,holidays=self.td_holidays)
        if date==date+td0:
            #itself is a trading day
            return date
        else:
            return date-td1
    
    def bd_td_offset(self,date):
        bdtd=CustomBusinessDay(0,holidays=self.holidays)
        return date+bdtd
        
    def create_all_td(self,start_date=None,end_date=None):
        """
        create all trading dates between start_date and end_date
        """
        if start_date is None: start_date=self.start_date
        if end_date is None: end_date=self.end_date
        td=CustomBusinessDay(1,holidays=self.td_holidays)
        return pd.Series(pd.date_range(start_date,end_date,freq=td))
        
    def create_event(self):
        self.events=[]
        self.warning_series=pd.Series(index=['价格水平','份额价值','预警类型','是否终止',
                                                '距离敲入日天数','距离敲入价格水平','是否敲入',
                                                '距离敲出日天数','距离敲出价格水平','是否敲出',
                                                '距离付息日天数','距离付息基准价格水平','付息金额',
                                                '距离到期日天数'])
    
    def event_warning(self):
        warning_string=[]
        for event in self.events:
            string=event.warning()
            warning_string.append(string)
        return pd.Series(warning_string,name='预警')
    
    def print_keyinfo(self):
        """
        for overloading
        """
        print('未定义当前收益凭证的print_keyinfo方法。')
    
    def print_warning(self):
        warning=self.event_warning()
        if warning.empty: return
        self.print_keyinfo()
        for ele in warning:
            print(ele)
    
    def to_excel(self):
        self.profile.loc['期初观察日']=self.start_date
        self.profile.loc['到期日']=self.end_date
        self.profile.loc['期初价格']=self.start_price
        knockout_dates=self.knockout_dates if not self.isAllKockOut else pd.Series(['all'],name='敲出观察日')
        knockin_dates=self.knockin_dates if not self.isAllKockIn else pd.Series(['all'],name='敲入观察日')
        self.profile.loc['敲出观察日']=self.encode_date(knockout_dates)
        self.profile.loc['敲入观察日']=self.encode_date(knockin_dates)
        #make sure '价格水平','份额价值','是否终止','是否敲入','是否敲出' are always filled.
        if pd.isna(self.warning_series.loc['价格水平']):
            td_pre_today=self.td_backward_offset(self.today)
            self.warning_series.loc['价格水平']=self.data.loc[td_pre_today,'收益表现水平']
        if pd.isna(self.warning_series.loc['份额价值']):
            self.warning_series.loc['份额价值']=self.mature()
        if pd.isna(self.warning_series.loc['是否终止']):
            if not pd.isna(self.profile.loc['终止日']):
                self.warning_series.loc['是否终止']=self.today>self.profile.loc['终止日']
            else:
                self.warning_series.loc['是否终止']=False
        if pd.isna(self.warning_series.loc['是否敲入']):
            self.warning_series.loc['是否敲入']=not pd.isna(self.profile.loc['敲入日'])
        if pd.isna(self.warning_series.loc['是否敲出']):
            self.warning_series.loc['是否敲出']=not pd.isna(self.profile.loc['提前终止日'])
        #change all dates to purly date (without time)
        profile=self.profile.apply(lambda x:x if type(x)!=pd._libs.tslibs.timestamps.Timestamp else x.date())
        warning_series=self.warning_series.apply(lambda x:x if type(x)!=pd._libs.tslibs.timestamps.Timestamp else x.date())
        return profile,warning_series
        
    
    def mature(self):
        #for overloading, calculate maturity payoff based on min(today,terminate day)
        print('请定义终止份额价值计算方法。')
        
    def update(self,start_date=None):
        """
        If the excel doesn't contain history events' info, e.g. if already knockout/knockin...
        You can use this function to update status day by day (all natural day) from start_date.
        This function could be slow, please be cautious to use it.
        """
        if start_date is None: start_date=self.start_date
        dates=pd.date_range(start_date,self.today)
        for date in dates:
            self.today=date
            self._init_currentclass()
            self.create_event()
            # self.print_warning()
        self.profile.loc['最后更新日期']=dates[-1]
            
    def holidays_infer(self,infer_bd=False,infer_td=False):
        """
        infer holidays from data (excluding weekend), and merge it with manaully defined holidays
        """
        all_dates=pd.date_range(self.data.index[0],self.data.index[-1],freq='B')
        holidays=set(all_dates)-set(self.data.index)
        if infer_bd:
            self.bd_holidays=pd.Series(list(holidays|set(self.bd_holidays)),
                                       name=self.bd_holidays.name).sort_values().reset_index(drop=True)
        if infer_td:
            self.td_holidays=pd.Series(list(holidays|set(self.td_holidays)),
                                       name=self.td_holidays.name).sort_values().reset_index(drop=True)
        self.holidays=pd.Series(list(set(self.bd_holidays).union(set(self.td_holidays)))).sort_values().reset_index(drop=True)

class SnowBall(StructuredNote):
    
    def __init__(self,profile,data):
        super().__init__(profile,data)
        self._init_currentclass()
        
    def _init_currentclass(self):
        if pd.isna(self.profile.loc['提前终止日']):
            self.profile.loc['终止日']=self.end_date
        else:
            self.profile.loc['终止日']=self.profile.loc['提前终止日']
            
    def print_keyinfo(self):
        print('当前日期：{}'.format(self.today.date()))
        td_pre_today=self.td_backward_offset(self.today)
        print('价格水平:{:.2%}\t敲入水平：{:.2%}\t敲出水平：{:.2%}'.format(self.data.loc[td_pre_today,'收益表现水平'],
                                                             self.profile.loc['敲入水平'],
                                                             self.profile.loc['敲出水平']))
            
    def create_event(self):
        super().create_event()
        if self.is_terminated: return
        knockin=Event.KnockIn(self)
        if knockin.isTriggered():
            knockin.effect()
            self.events.append(knockin)
        knockout=Event.KnockOut(self)
        if knockout.isTriggered():
            knockout.effect()
            self.events.append(knockout)
        maturity=Event.Maturity(self)
        if maturity.isTriggered():
            maturity.effect()
            self.events.append(maturity)
        terminate=Event.Terminate(self)
        if terminate.isTriggered():
            terminate.effect()
            self.events.append(terminate)
    
    def mature(self):
        if not pd.isna(self.profile.loc['提前终止日']):
            #if knock out is triggered
            duration=(self.profile.loc['终止日']-self.profile.loc['期初观察日']).days
            return self.profile.loc['份额面值']*(1+self.profile.loc['票面利率']*duration/365)
        elif pd.isna(self.profile.loc['敲入日']):
            #if knock in is not triggered
            duration=(self.profile.loc['终止日']-self.profile.loc['期初观察日']).days
            return self.profile.loc['份额面值']*(1+self.profile.loc['票面利率']*duration/365)
        else:
            td=CustomBusinessDay(0,holidays=self.td_holidays)
            td_pre_today=self.today-td
            end_date=min(td_pre_today,self.end_date)
            end_price_level=self.data.loc[end_date,'收益表现水平']
            return self.profile.loc['份额面值']*min(end_price_level,self.profile.loc['行权水平'])
        
class FixedCoupon(StructuredNote):
    
    def __init__(self,profile,data):
        super().__init__(profile,data)
        
    def print_keyinfo(self):
        print('当前日期：{}'.format(self.today.date()))
        td_pre_today=self.td_backward_offset(self.today)
        print('价格水平:{:.2%}\t敲入水平：{:.2%}\t敲出水平：{:.2%}'.format(self.data.loc[td_pre_today,'收益表现水平'],
                                                             self.profile.loc['敲入水平'],
                                                             self.profile.loc['敲出水平']))
        
    def create_event(self):
        super().create_event()
        if self.is_terminated: return
        knockin=Event.KnockIn(self)
        if knockin.isTriggered():
            knockin.effect()
            self.events.append(knockin)
        knockout=Event.KnockOut(self)
        if knockout.isTriggered():
            knockout.effect()
            self.events.append(knockout)
        maturity=Event.Maturity(self)
        if maturity.isTriggered():
            maturity.effect()
            self.events.append(maturity)
        terminate=Event.Terminate(self)
        if terminate.isTriggered():
            terminate.effect()
            self.events.append(terminate)
            
    def mature(self):
        #causion, we assume duration is 1 year
        if not pd.isna(self.profile.loc['提前终止日']):
            #if not terminated in advance
            return self.profile.loc['份额面值']*(1+self.profile.loc['票面利率'])
        else:
            td=CustomBusinessDay(0,holidays=self.td_holidays)
            td_pre_today=self.today-td
            end_date=min(td_pre_today,self.end_date)
            end_price_level=self.data.loc[end_date,'收益表现水平']
            if end_price_level>=self.profile.loc['敲出水平']:
                return self.profile.loc['份额面值']*(1+self.profile.loc['票面利率'])
            elif pd.isna(self.profile.loc['敲入日']):
                #if knock in is not triggered
                return self.profile.loc['份额面值']*(1+self.profile.loc['票面利率'])
            else:
                return self.profile.loc['份额面值']*min(end_price_level,self.profile.loc['行权水平'])
            
class Phoenix(StructuredNote):
    
    def __init__(self,profile,data):
        super().__init__(profile,data)
        
    def print_keyinfo(self):
        print('当前日期：{}'.format(self.today.date()))
        td_pre_today=self.td_backward_offset(self.today)
        print('价格水平:{:.2%}\t敲入水平：{:.2%}\t敲出水平：{:.2%}\t付息基准:{:.2%}'.format(self.data.loc[td_pre_today,'收益表现水平'],
                                                             self.profile.loc['敲入水平'],
                                                             self.profile.loc['敲出水平'],
                                                             self.profile.loc['付息判断基准']))
        
    def create_event(self):
        super().create_event()
        if self.is_terminated: return
        knockin=Event.KnockIn(self)
        if knockin.isTriggered():
            knockin.effect()
            self.events.append(knockin)
        knockout=Event.KnockOut(self)
        if knockout.isTriggered():
            knockout.effect()
            self.events.append(knockout)
        coupon=Event.NotKnockInCoupon(self)
        if coupon.isTriggered():
            coupon.effect()
            self.events.append(coupon)
        maturity=Event.Maturity(self)
        if maturity.isTriggered():
            maturity.effect()
            self.events.append(maturity)
        terminate=Event.Terminate(self)
        if terminate.isTriggered():
            terminate.effect()
            self.events.append(terminate)
    
    def mature(self):
        if not pd.isna(self.profile.loc['提前终止日']):
            #if not terminated in advance
            return self.profile.loc['份额面值']
        else:
            td=CustomBusinessDay(0,holidays=self.td_holidays)
            td_pre_today=self.today-td
            end_date=min(td_pre_today,self.end_date)
            end_price_level=self.data.loc[end_date,'收益表现水平']
            if end_price_level>=self.profile.loc['敲出水平']:
                return self.profile.loc['份额面值']
            elif pd.isna(self.profile.loc['敲入日']):
                #if knock in is not triggered
                return self.profile.loc['份额面值']
            else:
                return self.profile.loc['份额面值']*min(end_price_level,self.profile.loc['行权水平'])
            
class Shark(StructuredNote):
    
    def __init__(self,profile,data):
        super().__init__(profile,data)
        self.profile.loc['期末观察日']=self.bd_td_offset(self.profile.loc['期末观察日'])
        self.knockout_dates=self.create_all_td(self.start_date,
                                               self.profile.loc['期末观察日'])
        
    def print_keyinfo(self):
        print('当前日期：{}'.format(self.today.date()))
        td_pre_today=self.td_backward_offset(self.today)
        print('价格水平:{:.2%}\t敲出水平：{:.2%}'.format(self.data.loc[td_pre_today,'收益表现水平'],
                                                             self.profile.loc['敲出水平']))
        
    def create_event(self):
        super().create_event()
        if self.is_terminated: return
        knockout=Event.NotTerminateKnockOut(self)
        if knockout.isTriggered():
            knockout.effect()
            self.events.append(knockout)
        maturity=Event.Maturity(self)
        if maturity.isTriggered():
            maturity.effect()
            self.events.append(maturity)
        terminate=Event.Terminate(self)
        if terminate.isTriggered():
            terminate.effect()
            self.events.append(terminate)
    
    def mature(self):
        if not pd.isna(self.profile.loc['提前终止日']):
            #if not knockout
            rate=self.profile.loc['约定收益率']
        else:
            td=CustomBusinessDay(0,holidays=self.td_holidays)
            td_pre_today=self.today-td
            end_date=min(td_pre_today,self.profile.loc['期末观察日'])
            end_price=self.data.loc[end_date,'收盘价格']
            rate=(max(0,end_price/self.start_price-self.profile.loc['期望涨幅'])*
                  self.profile.loc['涨幅差乘数']+self.profile.loc['最低收益率'])
        
        duration=(self.end_date-self.start_date).days
        return self.profile.loc['份额面值']*(1+rate*duration/365)