from pyspark import SparkContext, SparkConf
from pyspark.sql import DataFrameReader, SQLContext
import os

# sparkClassPath = os.getenv('SPARK_CLASSPATH', r'C:\tools\postgresql-42.2.5.jar')
# print( sparkClassPath )
# Populate configuration

conf = SparkConf()
# conf.setAppName('application')
# conf.set('spark.jars', 'file:%s' % sparkClassPath)
# conf.set('spark.executor.extraClassPath', sparkClassPath)
# conf.set('spark.driver.extraClassPath', sparkClassPath)
# Uncomment line below and modify ip address if you need to use cluster on different IP address
# conf.set('spark.master', 'spark://127.0.0.1:7077')

sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

url = 'postgresql://192.168.175.3:5432/entrata44_dev'
properties = {'user':'psDba', 'password':'dba'}

df = DataFrameReader(sqlContext).jdbc(url='jdbc:%s' % url, table='lease_interval_types', properties=properties)
df.show()