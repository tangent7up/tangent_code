#!/usr/bin/python
#-*- coding:utf8 -*-

"""
利用EMR和Spark的ML库构建LR模型
"""
import sys
import math
import traceback
from operator import add
from pyspark import SparkConf
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.ml.linalg import SparseVector
from pyspark.ml.classification import LogisticRegression

TRAIN_CLICKID_LOOKUP = None
TEST_CLICKID_LOOKUP = None
FEATURE_IDX_LOOKUP = None
COLUMNS = None
KEYCOL = None
MODE_TRAIN_ONLY = 'train_only'
MODE_TRAIN_TEST = 'train_test'

def parse_feature_data(onerow):
    """
    onerow: Row(cid=u'f406021b-9610-46ef-90be-19b10a1a8d08', adx=u's', ssp=None)
    """
    result = []
    for eachcol in COLUMNS:
        if eachcol != KEYCOL:
            value = onerow[eachcol]
            if value is None:
                value = u'unknown'
            result.append((eachcol + ':' + value, 1))
    return result

def generate_feature_idx_dict(feature_list):
    """
    feature_list: [(u'yob:1984', 2), ...]
    """
    idx = 0
    feature_idx = {}
    idx_feature = {}
    for ele in feature_list:
        feature_idx[ele[0]] = idx
        idx_feature[idx] = ele[0]
        idx += 1
    return feature_idx, idx_feature

def generate_features(lab, onerow):
    size = len(FEATURE_IDX_LOOKUP.value)
    non_zero_idx = []
    for eachcol in COLUMNS:
        if eachcol != KEYCOL:
            value = onerow[eachcol]
            if value is None:
                value = u'unknown'
            key = eachcol + ':' + value
            if key in FEATURE_IDX_LOOKUP.value:
                non_zero_idx.append(FEATURE_IDX_LOOKUP.value[key])
    return Row(label=lab, features=SparseVector(size, sorted(non_zero_idx), [1]*len(non_zero_idx)))

def generate_labeledpoint_train(onerow):
    """
    onerow: Row(cid=u'f406021b-9610-46ef-90be-19b10a1a8d08', adx=u's', ssp=None)
    Output: Row(label=0, features=SparseVector(100, [0,4,77,98], [1,1,1,1]))
    """
    label = 0
    if onerow[KEYCOL] in TRAIN_CLICKID_LOOKUP.value:
        label = 1
    return generate_features(label, onerow)

def generate_labeledpoint_test(onerow):
    label = 0
    if onerow[KEYCOL] in TEST_CLICKID_LOOKUP.value:
        label = 1
    return generate_features(label, onerow)

def calculate_logloss(onerow):
    if onerow.label == 0:
        return math.log(onerow.probability[0])
    else:
        return math.log(onerow.probability[1])

def ctr(spark, mode, train_featuredata_path, train_clickdata_path, test_featuredata_path, test_clickdata_path, key, para_path, header=True, schema=None):
    global TRAIN_CLICKID_LOOKUP, TEST_CLICKID_LOOKUP, FEATURE_IDX_LOOKUP
    global COLUMNS, KEYCOL

    KEYCOL = key

    sc = spark.sparkContext
    train_clickid_set = set(sc.textFile(train_clickdata_path).collect())
    TRAIN_CLICKID_LOOKUP = sc.broadcast(train_clickid_set)
    test_clickid_set = set(sc.textFile(test_clickdata_path).collect())
    TEST_CLICKID_LOOKUP = sc.broadcast(test_clickid_set)

    feature_train_data_df = None
    data_schema = None
    if not header:
        COLUMNS = schema.split(',')
        data_schema = StructType([StructField(ele, StringType(), True) for ele in COLUMNS])
        feature_train_data_df = spark.read.csv(train_featuredata_path, schema=data_schema)
    else:
        feature_train_data_df = spark.read.csv(train_featuredata_path, header=True)
        COLUMNS = feature_train_data_df.columns

    #生成 特征取值_序号 字典
    feature_value_list = feature_train_data_df.rdd.flatMap(parse_feature_data).reduceByKey(add).collect()
    feature_idx_dict, idx_feature_dict = generate_feature_idx_dict(feature_value_list)
    FEATURE_IDX_LOOKUP = sc.broadcast(feature_idx_dict)
    print 'total features: ', len(feature_idx_dict)
    # for eachf in feature_idx_dict:
    #     print eachf, feature_idx_dict[eachf]

    train_data_df = feature_train_data_df.rdd.map(generate_labeledpoint_train).toDF().cache()
    blor = LogisticRegression(maxIter=100, regParam=0.01, family='binomial')
    blorModel = blor.fit(train_data_df)
    coefficients = blorModel.coefficients  # DenseVector([5.5...])
    intercept = blorModel.intercept

    # 利用测试集/训练集评估模型的效果
    eval_result = None
    if mode == MODE_TRAIN_ONLY:
        eval_result = blorModel.transform(train_data_df)
    else:
        feature_test_data_df = None
        if not header:
            feature_test_data_df = spark.read.csv(train_featuredata_path, schema=data_schema)
        else:
            feature_test_data_df = spark.read.csv(train_featuredata_path, header=True)
        test_data_df = feature_test_data_df.rdd.map(generate_labeledpoint_test).toDF()
        eval_result = blorModel.transform(test_data_df)

    # firstrow = eval_result.first()
    # print firstrow
    # indices = firstrow['features'].indices
    # print '=============='
    # for ind in indices:
    #     print idx_feature_dict[ind], float(coefficients[ind])
    # print '=============='

    logloss = eval_result.rdd.map(calculate_logloss).reduce(add)
    logloss = -logloss/train_data_df.count()
    print 'logloss:', logloss

    # pair_based_auc = 0
    # tmp = eval_result.select('label', 'probability').collect()
    # postive_pred = []
    # negative_pred = []
    # for ele in tmp:
    #     print ele.label, ele.probability[1]
    #     if ele.label > 0:
    #         postive_pred.append(ele.probability[1])
    #     else:
    #         negative_pred.append(ele.probability[1])
    # print len(postive_pred), len(negative_pred)
    # up = 0.0
    # for x in postive_pred:
    #     for y in negative_pred:
    #         if x > y:
    #             up += 1
    #         elif x == y:
    #             up += 0.5
    #         else:
    #             continue
    # pair_based_auc = up / (len(postive_pred) * len(negative_pred))
    # print 'pair-based auc: ', up, pair_based_auc

    metrics = blorModel.evaluate(train_data_df)
    print 'auc:', metrics.areaUnderROC

    # 返回结果
    evals = {'logloss': float(logloss), 'auc': float(metrics.areaUnderROC)}
    paras = {}
    for i in xrange(len(coefficients)):
        paras[idx_feature_dict[i]] = float(coefficients[i])
    paras['model_intercept'] = float(intercept)
    parainfo = {'evaluation_metrics': evals, 'parameters': paras}
    spark.createDataFrame([parainfo]).repartition(1).write.mode('append').json(para_path)
    print 'Done. Output: ', para_path
    sc.stop()

def gen_labels():
    istream = open(sys.argv[1], 'r')
    count = 0
    for line in istream:
        con = line.strip('\r\n').split(',')
        if count > 0 and count % 10 == 0:
            print con[0]
        count += 1

def main():
    if len(sys.argv) < 2:
        print 'Usage: spark-submit ctr_spark.py [options] conf_file'
        return
    spark = SparkSession.builder.config(conf=SparkConf()).getOrCreate()
    try:
        config_list = spark.read.text(sys.argv[1]).collect()
        config = {}
        for ele in config_list:
            tmp = ele.value.strip('\r\n')
            if tmp.startswith('#') or tmp.startswith('[') or '=' not in tmp:
                continue
            key, value = tmp.split('=')
            config[key.strip()] = value.strip()
        mode = config['mode']
        train_featuredata_path = config['train_featuredata_path']
        train_clickdata_path = config['train_clickdata_path']
        key_col = config['key']
        para_path = config['output']
        test_featuredata_path = train_featuredata_path
        test_clickdata_path = train_clickdata_path
        if 'test_featuredata_path' in config:
            test_featuredata_path = config['test_featuredata_path']
            test_clickdata_path = config['test_clickdata_path']
        header = True
        schema = None
        conf_header = config['header']
        if conf_header == 'no':
            header = False
            schema = config['schema']
    except:
        print 'Bad Configuration file.'
        traceback.print_exc()
        return
    print mode, train_featuredata_path, train_clickdata_path, key_col, para_path, test_featuredata_path, test_clickdata_path
    ctr(spark, mode, train_featuredata_path, train_clickdata_path, test_featuredata_path, test_clickdata_path, key_col, para_path, header, schema)

if __name__ == "__main__":
    main()
