# -*- coding: utf-8 -*-
"""
Created on Tue Dec  8 21:32:50 2020

@author: renli
"""
from flask import Flask, render_template, request, jsonify
import ReadFiles as RF
import werkzeug
import ReadFiles as RF
import json
import numpy as np
import pandas as pd
import WindDB as wind

"""
BASH start command:
    
export FLASK_APP=app.py
export FLASK_ENV=development
flask run --host localhost --port 8000
"""
app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False
app.config['ENV'] = 'development'

# app.logger.warning('testing warning log')
# app.logger.error('testing error log')
# app.logger.info('testing info log')

@app.route('/api/test',methods=['GET'])
def test():
    app.logger.info(str(request.args))
    app.logger.info('test')
    start_i=0
    end_i=20
    df=RF.fetch_db('select * from Profile Left Join Warning On Profile.key==Warning.key',['*'],{},{},{'key':'DESC'},start_i,end_i)
    df['Status']=1
    df.loc[df['WarningType'].isna(),'Status']=2
    df.loc[df['IsTerminated']==1,'Status']=0
    df=df.where(df.notna(), None)#convert np.nan to None
    data=[row.to_dict() for _,row in df.iterrows()]
    return json.dumps({'data':data,
                    'total':100,
                    'success':True,
                    'pageSize':20,
                    'current':1})

def _post_process(df):
    #process df retreived from db to match the dtype required by frontend
    df['Status']=1
    df.loc[df['WarningType'].notna(),'Status']=2
    df.loc[df['IsTerminated']==1,'Status']=0
    df=df.where(df.notna(), None)#convert np.nan to None, otherwise frontend doesn't recognize
    return df

@app.route('/api/search/rule',methods=['GET'])
def getRule():
    args=dict(request.args)
    current=int(args.pop('current'))
    pagesize=int(args.pop('pageSize'))
    _sorter=json.loads(args.pop('sorter')) #dict{column->ascend/descend}
    for key,val in _sorter.items():
        if val.lower()=='ascend':
            _sorter[key]='ASC'
        elif val.lower()=='descend':
            _sorter[key]='DESC'
    _filter=json.loads(args.pop('filter'))
    if not _sorter: _sorter={'key':'DESC'} #default descending ordered by key
    total=RF.fetch_length('Profile')
    start_i=(current-1)*pagesize
    end_i=current*pagesize
    match={}
    for key in list(args.keys()):
        if args[key]=='':
            args.pop(key)
    if 'key' in args:
        #we can only match key, not like key
        match['key']=args.pop('key')
    df=RF.fetch_db('select * from Profile Left Join Warning On Profile.key==Warning.key',['*'],match,args,_sorter,start_i,end_i)
    df=_post_process(df)
    data=[row.to_dict() for _,row in df.iterrows()]
    return jsonify(data=data,total=total,success=True,pageSize=pagesize,current=current)

@app.route('/api/search/remove',methods=['POST'])
def removeRule():
    key=request.get_json()['key']# can't decode json into request.form, so use .get_json()
    RF.delete_rows('Profile',{'key':key})
    RF.delete_rows('Warning',{'key':key})
    df=RF.fetch_db('select * from Profile Left Join Warning On Profile.key==Warning.key',['*'],{},{},{'key':'DESC'})
    total=RF.fetch_length('Profile')
    df=_post_process(df)
    data=[row.to_dict() for _,row in df.iterrows()]
    pagination={'total':total}
    return jsonify(list=data,pagination=pagination)

@app.route('/api/search/calculate',methods=['POST'])
def calcRule():
    json_dict=request.get_json()
    if json_dict['method']=='calculate':
        key=[json_dict['key']]
    elif json_dict['method']=='multicalculate':
        key=json_dict['key']
    RF.calc_db(key)
    return jsonify(key=key,success=True)

@app.route('/api/search/add',methods=['POST'])
def addRule():
    json_dict=request.get_json()
    for field in ['KnockOut','Strike','KnockIn','Rate','ParValue','ContractNumber']:
        if field in json_dict:
            json_dict[field]=float(json_dict[field])
    for field in ['BusinessDateInfer','TradingDateInfer']:
        if field in json_dict:
            json_dict[field]=bool(json_dict[field])
    key=RF.add_row(json_dict)[0]
    app.logger.info(key)
    df=RF.fetch_db('select * from Profile Left Join Warning On Profile.key==Warning.key',['*'],{'key':key},{},{})
    df=_post_process(df)
    data=[row.to_dict() for _,row in df.iterrows()]
    data=data[0]
    return jsonify(**data)

@app.route('/api/search/update',methods=['POST'])
def updatePrice():
    json_dict=request.get_json()
    code=json_dict['code']
    if code=='000905.SH':
        res=wind.polling_000905()
    return jsonify(success=res)

def my_hist(x,bins=None):
    # x is 1D array
    import numpy as np
    from scipy.stats import gaussian_kde
    if bins is None:
        bins=np.arange(0.4,1.5+1e-8,0.05)
    hist,edges=np.histogram(x,bins=bins)
    center=(edges[:-1]+edges[1:])/2
    pdf=gaussian_kde(x).pdf(center)
    return center,hist,pdf
    
@app.route('/api/chart/rule',methods=['GET','POST'])
def chartRule():
    from pandas.tseries.offsets import Day
    json_dict=request.get_json()
    app.logger.info(json_dict)
    method=json_dict.pop('method')
    if method == 'price':
        codes=json_dict.pop('codes')
        dates=json_dict.pop('dates')
        if not dates: #when first open the page, dates=[]
            d2=pd.Timestamp.today()
            d1=d2-Day(2*365)
            dates=[str(d1.date()),str(d2.date())]
        df=RF.execute_sql("Select Date,"+','.join(('`'+ele+'`' for ele in codes))+
                          ' From Price Where Date Between '+'"'+dates[0]+'" And "'+dates[1]+'" Order by Date',
                          RF.engine,'Date')
        df.set_index('Date',inplace=True)
        date=df.index.to_list()
        price=[col.to_list() for _,col in df.iteritems()]
        price_level=df/df.iloc[0,:]
        price_level=[col.to_list() for _,col in price_level.iteritems()]
        return jsonify(date=date,price=price,price_level=price_level,success=True)
    elif method == 'statistics_1':
        df=RF.fetch_db('select * from Profile Left Join Warning On Profile.key==Warning.key',['*'],{'isTerminated':0},{},{'key':'DESC'})
        pie_series=df.groupby('Type')['Type'].count()
        data_1={'x':pie_series.index.to_list(),'y':pie_series.to_list()}
        df=RF.execute_sql('select StartDate,Type from Profile Left Join Warning On Profile.key==Warning.key',RF.engine,
                          'StartDate')
        df['year']=df['StartDate'].apply(lambda x:x.year)
        # df['year']=df['year'].apply(lambda x:2018+np.random.randint(4))#mock data
        df.drop(columns='StartDate',inplace=True)
        df['num']=1
        hist_count=pd.pivot_table(df,values='num',index='Type',columns='year',aggfunc=np.sum)
        hist_count=hist_count.where(hist_count.notna(), 0)
        data_2={'x':hist_count.columns.to_list(),'y':hist_count.to_numpy().tolist(),
                'legend':hist_count.index.to_list()}
        return jsonify(data_1=data_1,data_2=data_2,success=True)
    elif method == "statistics_2":
        df=RF.execute_sql('select * from Profile Left Join Warning On Profile.key==Warning.key Where IsTerminated=0',RF.engine,
                          RF._en_profile_date_columns)
        # df['KnockOut']=df['KnockOut'].apply(lambda x:1+0.2*np.random.randn())#mock data
        # df['KnockIn']=df['KnockIn'].apply(lambda x:0.8-0.2*np.random.randn())#mock data
        df['KnockOut']=df['KnockOut']/df['PriceLevel'] #adjust it to current pricelevel
        df['KnockIn']=df['KnockIn']/df['PriceLevel'] #adjust it to current pricelevel
        center,hist,pdf=my_hist(df['KnockOut'].dropna().to_numpy())
        center,hist,pdf=center.tolist(),hist.tolist(),pdf.tolist()
        KnockOut=dict(center=center,hist=hist,pdf=pdf)
        center,hist,pdf=my_hist(df['KnockIn'].dropna().to_numpy())
        center,hist,pdf=center.tolist(),hist.tolist(),pdf.tolist()
        KnockIn=dict(center=center,hist=hist,pdf=pdf)
        #calculate return distribution
        df=RF.execute_sql('select Date, `000905.SH` from Price order by Date',RF.engine,'Date').set_index('Date')
        def return_pdf(series):
            from scipy.stats import gaussian_kde
            def _f(series,diff_num):
                temp=np.log(series.iloc[-2*diff_num:].to_numpy())
                temp=temp[diff_num:]-temp[:-diff_num]
                return np.exp(temp)
            daynum_1m=21
            return_1m=_f(series,daynum_1m)
            return_3m=_f(series,3*daynum_1m)
            return_6m=_f(series,6*daynum_1m)
            return_12m=_f(series,12*daynum_1m)
            return_axis=np.arange(0.4,1.5+1e-8,0.001)
            # return_axis=(return_axis[:-1]+return_axis[1:])/2
            pdf_1m=gaussian_kde(return_1m).pdf(return_axis)
            pdf_3m=gaussian_kde(return_3m).pdf(return_axis)
            pdf_6m=gaussian_kde(return_6m).pdf(return_axis)
            pdf_12m=gaussian_kde(return_12m).pdf(return_axis)
            return return_axis,pdf_1m,pdf_3m,pdf_6m,pdf_12m
        return_axis,pdf_1m,pdf_3m,pdf_6m,pdf_12m=return_pdf(df['000905.SH'])
        return_axis,pdf_1m,pdf_3m,pdf_6m,pdf_12m=return_axis.tolist(),pdf_1m.tolist(),pdf_3m.tolist(),pdf_6m.tolist(),pdf_12m.tolist()
        price_level_pdf=dict(x=return_axis,pdf_1m=pdf_1m,pdf_3m=pdf_3m,pdf_6m=pdf_6m,pdf_12m=pdf_12m)
        return jsonify(KnockOut=KnockOut,KnockIn=KnockIn,price_level_pdf=price_level_pdf,success=True)
    elif method == 'statistics_3':
        df=RF.execute_sql('select Date,StartDate,TerminateDate,Type from Profile Left Join Warning On Profile.key==Warning.key',RF.engine,
                          ['Date','StartDate','TerminateDate'])
        # today=pd.Timestamp.today()
        df['life']=(df['Date']-df['StartDate']).apply(lambda x:x.days)
        df.loc[df['TerminateDate'].notna(),'life']=(df.loc[df['TerminateDate'].notna(),'TerminateDate']-
                                                    df.loc[df['TerminateDate'].notna(),'StartDate']).apply(lambda x:x.days)
        # df['life']=df['life'].apply(lambda x:np.random.randint(800)) #mock data
        df.drop(columns=['StartDate','TerminateDate'],inplace=True)
        data=df.groupby('Type').apply(lambda x:x['life'].to_list())
        key,val=data.index.to_list(),data.to_list()
        return jsonify(x=key,y=val,success=True)
    return jsonify(success=False)

@app.route('/api/monitor/rule',methods=['GET'])
def getRule2():
    args=dict(request.args)
    current=int(args.pop('current'))
    pagesize=int(args.pop('pageSize'))
    _sorter=json.loads(args.pop('sorter')) #dict{column->ascend/descend}
    for key,val in _sorter.items():
        if val.lower()=='ascend':
            _sorter[key]='ASC'
        elif val.lower()=='descend':
            _sorter[key]='DESC'
    _filter=json.loads(args.pop('filter'))
    if not _sorter: _sorter={'key':'DESC'} #default descending ordered by key
    total=RF.fetch_length('Profile')
    start_i=(current-1)*pagesize
    end_i=current*pagesize
    match={}
    for key in list(args.keys()):
        if args[key]=='':
            args.pop(key)
    if 'key' in args:
        #we can only match key, not like key
        match['key']=args.pop('key')
    df=RF.fetch_db('select * from Profile Left Join Warning On Profile.key==Warning.key Where WarningType is not NULL',
                   ['*'],match,args,_sorter,start_i,end_i)
    app.logger.info(df.shape)
    app.logger.info(df.shape)
    df=_post_process(df)
    data=[row.to_dict() for _,row in df.iterrows()]
    return jsonify(data=data,total=total,success=True,pageSize=pagesize,current=current)

@app.route('/api/monitor/remove',methods=['POST'])
def removeRule2():
    key=request.get_json()['key']# can't decode json into request.form, so use .get_json()
    RF.delete_rows('Profile',{'key':key})
    RF.delete_rows('Warning',{'key':key})
    df=RF.fetch_db('select * from Profile Left Join Warning On Profile.key==Warning.key Where WarningType is not NULL',
                   ['*'],{},{},{'key':'DESC'})
    total=RF.fetch_length('Profile')
    df=_post_process(df)
    data=[row.to_dict() for _,row in df.iterrows()]
    pagination={'total':total}
    return jsonify(list=data,pagination=pagination)
    
if __name__ == "__main__":
    # app.run(host='localhost',port=8000,debug=True)
    df=RF.fetch_db('select * from Profile Left Join Warning On Profile.key==Warning.key',['*'],{},{},{'key':'DESC'})
    