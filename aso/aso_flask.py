#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
功能：关键词拓词页面查询后台文件
数据源：levledb
数据周期：一周
版本：
（1）2017-05-25  初稿
'''

from flask import Flask
from flask import request, abort, make_response, render_template
import json
import sys
import os
import level_db
import logging


##################
####（1）初始化app,log
##################
reload(sys)
sys.setdefaultencoding('utf8')# solve the UnicodeEncodeError

def init_logger(name, filename):
    logger = logging.getLogger(name)
    log_file = filename
    log_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
    log_formatter = logging.Formatter(
        '%(asctime)s [%(levelname)s] %(name)s: %(message)s')
    log_handler.setFormatter(log_formatter)
    logger.addHandler(log_handler)
    logger.propagate = False
    logger.setLevel(logging.INFO)
    return logger

logger = init_logger('mylogger', 'log.txt')
app = Flask(__name__)

##################
####（2）异常处理
##################
#200 请求成功！ 
@app.errorhandler(Exception)
def my_error_handler(error):
    if '201' in str(error):
        response = dict(status=201, message='拓词查询无结果！')
        return make_response(json.dumps(response, ensure_ascii=False))
    elif '101' in str(error):
        response = dict(status=101, message='参数解析错误！')
        return make_response(json.dumps(response, ensure_ascii=False))
    elif '102' in str(error):
        response = dict(status=102, message='关键词个数超出指定范围！')
        return make_response(json.dumps(response, ensure_ascii=False))
    elif '103' in str(error):
        response = dict(status=103, message='权重范围不在0~1之间！')
        return make_response(json.dumps(response, ensure_ascii=False))

##################
####（3）数据交互

@app.route('/', methods=['GET', 'POST'])
def home():
    return render_template('index.html')

@app.route('/getData', methods=['GET', 'POST'])
def fetchData(max_num_of_kw=5):
    '''
    与前端数据交互
    '''
    print 'ha!'
    logger.info('start query')
    try:
        weight = float(request.form.get('weight'))
        if weight < 0 or weight > 1:
            logger.error(str(er), exc_info=True)
            abort(103)
    except Exception as er :
        logger.error(str(er), exc_info=True)
        abort(103)
    try:
        keyword = request.form.get('keyword').strip('\r\n').replace('[', '').replace(']', '')
        method = request.form.get('method')
    except Exception as er:
        logger.error(str(er), exc_info=True)
        abort(101)
    try:
        mean_sim_list = []
        text_sim_list = []
        if method == 'mean':
            mean_sim_list = level_db.getSimWord(keyword, weight=weight)
            if mean_sim_list is None:
                abort(201)
            return_dict = {
                    'status': 200,
                    'message': u'请求成功',
                    'result': mean_sim_list
                }
        if method == 'text':
            text_sim_list = level_db.getReWord(keyword, weight=weight)
            if text_sim_list is None:
                abort(201)
            return_dict = {
                            'status': 200,
                            'message': u'请求成功',
                            'result': text_sim_list
                        }
    except Exception as e:
        logger.error(str(e), exc_info=True)
        abort(201)
    if len(mean_sim_list) == 0 and len(text_sim_list) == 0:
        abort(201)
    return make_response(json.dumps(return_dict, ensure_ascii=False, indent=4))

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5003, debug=True)


