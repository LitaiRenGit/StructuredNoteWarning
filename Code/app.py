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
    df=RF.fetch_db('select * from Profile Left Join Warning On Profile.key==Warning.key',['*'],{},['key'],start_i,end_i)
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
    df.loc[df['WarningType'].isna(),'Status']=2
    df.loc[df['IsTerminated']==1,'Status']=0
    df=df.where(df.notna(), None)#convert np.nan to None
    return df

@app.route('/api/monitor/rule',methods=['GET'])
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
    # if not _sorter: _sorter={'key':'DESC'} #default descending ordered by key
    total=RF.fetch_length('Profile')
    start_i=(current-1)*pagesize
    end_i=current*pagesize
    for key in list(args.keys()):
        if args[key]=='':
            args.pop(key)
    df=RF.fetch_db('select * from Profile Left Join Warning On Profile.key==Warning.key',['*'],args,_sorter,start_i,end_i)
    df=_post_process(df)
    data=[row.to_dict() for _,row in df.iterrows()]
    return jsonify(data=data,total=total,success=True,pageSize=pagesize,current=current)

@app.route('/api/monitor/remove',methods=['POST'])
def removeRule():
    key=request.get_json()['key']# can't decode json into request.form, so use .get_json()
    RF.delete_rows('Profile',{'key':key})
    RF.delete_rows('Warning',{'key':key})
    df=RF.fetch_db('select * from Profile Left Join Warning On Profile.key==Warning.key',['*'],{},['key'])
    total=RF.fetch_length('Profile')
    df=_post_process(df)
    data=[row.to_dict() for _,row in df.iterrows()]
    pagination={'total':total}
    return jsonify(list=data,pagination=pagination)

@app.route('/api/monitor/calculate',methods=['POST'])
def calcRule():
    json_dict=request.get_json()
    if json_dict['method']=='calculate':
        key=[json_dict['key']]
    elif json_dict['method']=='multicalculate':
        key=json_dict['key']
    RF.calc_db(key,pd.to_datetime('2020-11-30')) #temporarily use 11-30 as latest date
    return jsonify(key=key,success=True)

if __name__ == "__main__":
    app.run(host='localhost',port=8000,debug=True)