# -*- coding: utf-8 -*-
"""
Created on Tue Dec  8 21:32:50 2020

@author: renli
"""
from flask import Flask, render_template, request, jsonify
import ReadFiles as RF
import werkzeug

"""
export FLASK_APP=app.py
export FLASK_ENV=development
flask run --host localhost --port 8000
"""
app = Flask(__name__)
# app.config['ENV'] = 'development'

# app.logger.warning('testing warning log')
# app.logger.error('testing error log')
# app.logger.info('testing info log')
@app.route('/',methods=['GET','POST'])
def home():
    app.logger.info('home')
    return 'home'

@app.route('/api/monitor/rule',methods=['GET'])
def getRule():
    app.logger.info('hello get')
    return 'getRule'

@app.route('/api/monitor/rule',methods=['POST'])
def postRule():
    app.logger.info('hello post')
    return 'postRule'
    
@app.route('/api/monitor/111',methods=['GET'])
def getRule111():
    print(str(request.form))
    return 'getRule111'

@app.route('/api/test',methods=['GET','POST'])
def test():
    app.logger.info(str(request.args))
    # _temp='1'+2
    app.logger.info('test')
    return jsonify('test')

# if __name__ == "__main__":
#     app.run(host='localhost',port=8000,debug=True)