#!/usr/bin/env python 
# -*- coding: utf-8 -*- 
# @Time : 2020/11/18 17:06 
# @Author : magician 
# @File : py_test.py 
# @Software: PyCharm
from pyspark.shell import sqlContext
from pyspark.sql import SparkSession, HiveContext

_SPARK_HOST = "local[6]"
_APP_NAME = "test"

import sys
read_txt=sys.argv[1]
result_table=sys.argv[2]
spark = SparkSession.builder.master(_SPARK_HOST).appName(_APP_NAME).getOrCreate()
# import pandas as pd

df = spark.read.csv('file://%s'%read_txt,header=False)#使用指定的schema

# method one，default是默认数据库的名字，write_test 是要写到default中数据表的名字
df.registerTempTable('test_hive')
sqlContext.sql("create table %s select * from test_hive"%(result_table))