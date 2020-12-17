# -*- coding: utf-8 -*-
"""
Created on Mon Nov 30 10:13:52 2020

@author: renli
"""
import numpy as np
import pandas as pd
from scipy.stats import norm
from pandas.tseries.offsets import CustomBusinessDay

class Event:
    THRESHOLD_CONFIDENCE_LEVEL=0.75 #it means how much chance to not knockout/knockin so that we can skip warning
    
    def __init__(self,StructuredNote):
        self.StructuredNote=StructuredNote
        self.name='事件基类'
        
    def calc_vol(self):
        #use previous 21 days' price to calculate vol (1 month)
        today_index=self.StructuredNote.data.index.searchsorted(self.StructuredNote.today,side='right')
        price=self.StructuredNote.data['收盘价格'].iloc[today_index-21:today_index]
        vol=np.std(np.diff(np.log(price)))#logreturn vol
        if np.isnan(vol):
            #if vol can't be calculated due to lack of data, assume 15% annual vol
            return 0.15*np.sqrt(1/365)
        else:
            return vol
        
    def isTriggered(self):
        #for overloading
        #judge if this event is triggered.
        print('发生未定义事件，isTriggered。')
        return False
    
    def warning(self):
        #for overloading
        print('发生未定义事件，warning。')
        
    def effect(self):
        #for overloading
        #do something in self.StructuredNote
        return
    
    def td_diff(self,start_date,end_date):
        #calculate the trading day differece between 2 dates
        # start_date,end_date=np.datetime64(self.StructuredNote.td_backward_offset(start_date),
        #                                   'D'),np.datetime64(end_date,'D')
        start_date,end_date=np.datetime64(start_date,'D'),np.datetime64(end_date,'D')
        holidays=list(map(lambda x:np.datetime64(x,'D'),self.StructuredNote.td_holidays))
        diff=np.busday_count(start_date,end_date,holidays=holidays)
        if diff==0:
            #np.busday_count will consider diff as 0 when start_date is a post-adjacent holiday, rectify it
            if start_date>end_date:
                return -1
            elif start_date<end_date:
                return 1
            else:
                return 0
        else:
            return diff
        
    def record_type(self,is_inherited=False):
        #once take effect, '预警类型' must be filled with event name
        #if the event(self) is inhirited from another event, is_inherited=True
        if not is_inherited:
            if pd.isna(self.StructuredNote.warning_series.loc['预警类型']):
                self.StructuredNote.warning_series.loc['预警类型']=self.name
            else:
                self.StructuredNote.warning_series.loc['预警类型']+=','+self.name
        else:
            if pd.isna(self.StructuredNote.warning_series.loc['预警类型']):
                self.StructuredNote.warning_series.loc['预警类型']=self.name
            else:
                str_list=self.StructuredNote.warning_series.loc['预警类型'].split(',')
                str_list[-1]=self.name
                self.StructuredNote.warning_series.loc['预警类型']=','.join(str_list)
    
class KnockIn(Event):
    
    def __init__(self,StructuredNote):
        super().__init__(StructuredNote)
        self.name='敲入'
        if self.StructuredNote.knockin_dates.empty: return
        self.already_triggered=not pd.isna(self.StructuredNote.profile.loc['敲入日'])
        nextday_index=self.StructuredNote.knockin_dates.searchsorted(self.StructuredNote.today)
        if nextday_index==len(self.StructuredNote.knockin_dates): nextday_index-=1
        self.day_left=self.td_diff(self.StructuredNote.today,self.StructuredNote.knockin_dates.iloc[nextday_index])
        self.knockin_level=self.StructuredNote.profile.loc['敲入水平']
        vol=self.calc_vol()*np.sqrt(max(1,self.day_left))
        self.warning_threshold=(1/(1-norm.ppf(self.THRESHOLD_CONFIDENCE_LEVEL,0,vol))-1)*self.knockin_level
        #choose warning_threshold corresponding to confidence level of how likely we are not going to knock in
    
    def isTriggered(self):
        if self.StructuredNote.knockin_dates.empty: return False
        if self.already_triggered: return False
        if self.day_left<0 or self.day_left>=5: return False
        td_pre_today=self.StructuredNote.td_backward_offset(self.StructuredNote.today)
        self.current_price_level=self.StructuredNote.data.loc[td_pre_today,'收益表现水平']
        self.kockin_distance=self.current_price_level-self.knockin_level
        return self.kockin_distance<=self.warning_threshold
    
    def warning(self):
        head='预警类型：'+self.name+'\n'
        if self.kockin_distance>0:
            string='请注意：当前价格水平为{:.2%}，距离下个敲入观察日还有{:d}个交易日,距离敲入仅差{:.2%}。'.format(self.current_price_level,
                                                                      max(1,self.day_left),self.kockin_distance)
        else:
            if self.day_left>0:
                string='请注意：当前价格水平为{:.2%}，距离下个敲入观察日还有{:d}个交易日,已低于敲入水平{:.2%}。'.format(self.current_price_level,
                                                                      self.day_left,-self.kockin_distance)
            else:
                string='请注意：今天是敲入观察日,当前价格水平为{:.2%}，已敲入。'.format(self.current_price_level)
        string=head+string
        return string
    
    def effect(self):
        #once take effect, '预警类型' must be filled with event name
        self.record_type()
        self.StructuredNote.warning_series.loc['价格水平']=self.current_price_level
        if self.kockin_distance<=0:
            if self.day_left==0:
                #already knock in, record the knock in date
                self.StructuredNote.profile.loc['敲入日']=self.StructuredNote.today
                self.StructuredNote.warning_series.loc['距离敲入日天数']=self.day_left
                self.StructuredNote.warning_series.loc['是否敲入']=True
            elif self.day_left>0:
                self.StructuredNote.warning_series.loc['距离敲入日天数']=max(1,self.day_left)
                self.StructuredNote.warning_series.loc['是否敲入']=False
                self.StructuredNote.warning_series.loc['距离敲入价格水平']=self.kockin_distance
        else:
            self.StructuredNote.warning_series.loc['距离敲入日天数']=max(1,self.day_left)
            self.StructuredNote.warning_series.loc['是否敲入']=False
            self.StructuredNote.warning_series.loc['距离敲入价格水平']=self.kockin_distance
            

class KnockOut(Event):
    
    def __init__(self,StructuredNote):
        super().__init__(StructuredNote)
        self.name='敲出'
        if self.StructuredNote.knockout_dates.empty: return
        self.already_triggered=not pd.isna(self.StructuredNote.profile.loc['提前终止日'])
        nextday_index=self.StructuredNote.knockout_dates.searchsorted(self.StructuredNote.today)
        if nextday_index==len(self.StructuredNote.knockout_dates): nextday_index-=1
        self.day_left=self.td_diff(self.StructuredNote.today,self.StructuredNote.knockout_dates.iloc[nextday_index])
        vol=self.calc_vol()*np.sqrt(max(1,self.day_left))
        self.knockout_level=self.StructuredNote.profile.loc['敲出水平']
        self.warning_threshold=(1-1/(1+norm.ppf(self.THRESHOLD_CONFIDENCE_LEVEL,0,vol)))*self.knockout_level
        #choose warning_threshold corresponding to confidence level of how likely we are not going to knock out
        
    def isTriggered(self):
        if self.StructuredNote.knockout_dates.empty: return False
        if self.already_triggered: return False
        if self.day_left<0 or self.day_left>=5: return False
        td_pre_today=self.StructuredNote.td_backward_offset(self.StructuredNote.today)
        self.current_price_level=self.StructuredNote.data.loc[td_pre_today,'收益表现水平']
        self.kockout_distance=self.knockout_level-self.current_price_level
        return self.kockout_distance<=self.warning_threshold
    
    def warning(self):
        head='预警类型：'+self.name+'\n'
        if self.kockout_distance>0:
            string='请注意：当前价格水平为{:.2%}，距离下个敲出观察日还有{:d}个交易日，距离敲出仅差{:.2%}。'.format(
                self.current_price_level,max(1,self.day_left),self.kockout_distance)
        else:
            if self.day_left>0:
                string='请注意：当前价格水平为{:.2%}，距离下个敲出观察日还有{:d}个交易日,已超出敲出水平{:.2%}。'.format(self.current_price_level,
                                                                         self.day_left,-self.kockout_distance)
            else:
                payment=self.StructuredNote.mature()
                string='请注意：今天是敲出观察日，当前价格水平为{:.2%}，已敲出，终止份额价值为{:.4f}。'.format(
                    self.current_price_level,payment)
        string=head+string
        return string
    
    def effect(self):
        #once take effect, '预警类型' must be filled with event name
        self.record_type()
        self.StructuredNote.warning_series.loc['价格水平']=self.current_price_level
        if self.kockout_distance<=0:
            if self.day_left==0:
                #already knock out, record the knock in date
                self.StructuredNote.profile.loc['提前终止日']=self.StructuredNote.today
                self.StructuredNote.profile.loc['终止日']=self.StructuredNote.today
                self.StructuredNote.warning_series.loc['距离敲出日天数']=self.day_left
                self.StructuredNote.warning_series.loc['是否敲出']=True
            elif self.day_left>0:
                self.StructuredNote.warning_series.loc['距离敲出日天数']=max(1,self.day_left)
                self.StructuredNote.warning_series.loc['是否敲出']=False
                self.StructuredNote.warning_series.loc['距离敲出价格水平']=self.kockout_distance
        else:
            self.StructuredNote.warning_series.loc['距离敲出日天数']=max(1,self.day_left)
            self.StructuredNote.warning_series.loc['是否敲出']=False
            self.StructuredNote.warning_series.loc['距离敲出价格水平']=self.kockout_distance
                

class Coupon(Event):
    
    def __init__(self,StructuredNote):
        super().__init__(StructuredNote)
        self.name='付息'
        if self.StructuredNote.coupon_dates.empty: return
        nextday_index=self.StructuredNote.coupon_dates.searchsorted(self.StructuredNote.today)
        if nextday_index==len(self.StructuredNote.coupon_dates): nextday_index-=1
        self.next_coupon_date=self.StructuredNote.coupon_dates.iloc[nextday_index]
        self.day_left=self.td_diff(self.StructuredNote.today,self.next_coupon_date)
        self.payment=(self.StructuredNote.profile.loc['份额面值']*
                      self.StructuredNote.profile.loc['票面利率'])
        
    def isTriggered(self):
        if self.StructuredNote.coupon_dates.empty: return False
        return 0<=self.day_left<5
    
    def warning(self):
        head='预警类型：'+self.name+'\n'
        string='请注意：下一付息日为{}，距离今天有{:d}个交易日，付息金额为{:.4f}/份。'.format(self.next_coupon_date.date(),
                                                          self.day_left,self.payment)
        string=head+string
        return string
    
    def effect(self):
        #once take effect, '预警类型' must be filled with event name
        self.record_type()
        self.StructuredNote.warning_series.loc['距离付息日天数']=self.day_left
        self.StructuredNote.warning_series.loc['付息金额']=self.payment
    
class NotKnockInCoupon(Coupon):
    #coupon condition on ">= knock in level"
    def __init__(self,StructuredNote):
        super().__init__(StructuredNote)
        self.name='付息（大于等于付息基准）'
        vol=self.calc_vol()*np.sqrt(self.day_left)
        knockin_level=self.StructuredNote.profile.loc['付息判断基准']
        self.warning_lb=knockin_level/(1+norm.ppf(self.THRESHOLD_CONFIDENCE_LEVEL,0,vol))
        self.warning_ub=knockin_level/(1-norm.ppf(self.THRESHOLD_CONFIDENCE_LEVEL,0,vol))
        
    def isTriggered(self):
        td_pre_today=self.StructuredNote.td_backward_offset(self.StructuredNote.today)
        self.current_price_level=self.StructuredNote.data.loc[td_pre_today,'收益表现水平']
        self.knockin_distance=self.StructuredNote.profile.loc['付息判断基准']-self.current_price_level
        return super().isTriggered()
    
    def warning(self):
        head='预警类型：'+self.name+'\n'
        if self.current_price_level>self.warning_ub:
            string='下一付息日为{}，距离今天有{:d}个交易日，付息金额为{:.4f}/份，当前价格较高,有较大概率付息。'.format(self.next_coupon_date.date(),
                                                                    self.day_left,self.payment)
        elif self.current_price_level>0:
            string='下一付息日为{}，距离今天有{:d}个交易日，付息金额为{:.4f}/份，当前价格高于付息基准{:.2%}。'.format(self.next_coupon_date.date(),
                                                                    self.day_left,self.payment,self.knockin_distance)
        elif self.current_price_level>=self.warning_lb:
            string='下一付息日为{}，距离今天有{:d}个交易日，付息金额为{:.4f}/份，当前价格低于付息基准{:.2%}。'.format(self.next_coupon_date.date(),
                                                                    self.day_left,self.payment,-self.knockin_distance)
        else:
            string='下一付息日为{}，距离今天有{:d}个交易日，付息金额为{:.4f}/份，当前价格较低,有较大概率不付息。'.format(self.next_coupon_date.date(),
                                                                    self.day_left,self.payment)
        string=head+string
        return string
    
    def effect(self):
        super().effect()
        #once take effect, '预警类型' must be filled with event name
        self.record_type(True)
        self.StructuredNote.warning_series.loc['距离付息基准价格水平']=self.knockin_distance
    
class Maturity(Event):
    
    def __init__(self,StructuredNote):
        super().__init__(StructuredNote)
        self.name='到期'
        self.day_left=self.td_diff(self.StructuredNote.today,self.StructuredNote.end_date)
        
    def isTriggered(self):
        if not pd.isna(self.StructuredNote.profile.loc['终止日']):
            if self.StructuredNote.today>self.StructuredNote.profile.loc['终止日']:
                #if already terminated in advance
                return False
        return self.day_left<5
    
    def warning(self):
        head='预警类型：'+self.name+'\n'
        payment=self.StructuredNote.mature()
        if self.day_left>=0:
            string='请注意：距离到期还有{:d}个交易日，当前标的价格对应的终止份额价值为{:.4f}。'.format(self.day_left,payment)
        else:
            string='请注意：已到期，终止份额价值为{:.4f}。'.format(payment)
        string=head+string
        return string
    
    def effect(self):
        payment=self.StructuredNote.mature()
        #once take effect, '预警类型' must be filled with event name
        self.record_type()
        self.StructuredNote.warning_series.loc['份额价值']=payment
        if self.day_left==0:
            if pd.isna(self.StructuredNote.profile.loc['终止日']): #only if terminated date hasn't been defined, define it here as end_date
                self.StructuredNote.profile.loc['终止日']=self.StructuredNote.end_date
            self.StructuredNote.warning_series.loc['距离到期日天数']=self.day_left
        else:
            self.StructuredNote.warning_series.loc['距离到期日天数']=self.day_left
            

class Terminate(Event):

    def __init__(self,StructuredNote):
        super().__init__(StructuredNote)
        self.name='终止'
        
    def isTriggered(self):
        if not pd.isna(self.StructuredNote.profile.loc['终止日']):
            return self.StructuredNote.today>=self.StructuredNote.profile.loc['终止日']
        else:
            return False
    
    def warning(self):
        head='预警类型：'+self.name+'\n'
        if pd.isna(self.StructuredNote.profile.loc['终止份额价值']):
            payment=self.StructuredNote.mature()
        else:
            payment=self.StructuredNote.profile.loc['终止份额价值']
        string='合约已终止，终止份额价值价值为{:.4f}。'.format(payment)
        string=head+string
        return string
    
    def effect(self):
        self.StructuredNote.is_terminated=True
        #once take effect, '预警类型' must be filled with event name
        self.StructuredNote.warning_series.loc[:]=np.nan #clear any other warnings since termination makes them not important
        self.record_type()
        if pd.isna(self.StructuredNote.profile.loc['终止份额价值']):
            payment=self.StructuredNote.mature()
        else:
            payment=self.StructuredNote.profile.loc['终止份额价值']
        self.StructuredNote.profile.loc['终止份额价值']=payment
        self.StructuredNote.profile.loc['终止兑付金额']=(self.StructuredNote.profile.loc['终止份额价值']*
                                                        self.StructuredNote.profile.loc['收益凭证份额'])
        self.StructuredNote.warning_series.loc['是否终止']=True
        
class NotTerminateKnockOut(KnockOut):
    #knockout but not terminate the note
    def __init__(self,StructuredNote):
        super().__init__(StructuredNote)
        self.name='敲出（不终止）'
        
    def effect(self):
        super().effect()
        #once take effect, '预警类型' must be filled with event name
        self.record_type(True)
        if self.kockout_distance<=0:
            if self.day_left==0:
                self.StructuredNote.profile.loc['终止日']=np.nan