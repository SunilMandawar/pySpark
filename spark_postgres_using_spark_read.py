# Refer: https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html
from pyspark import SparkContext
from pyspark.sql import SparkSession

sc = SparkContext()
spark = SparkSession(sc)

# Extract

df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://192.168.175.3:5432/entrata44_dev") \
    .option("user", "psDba") \
    .option("password", "dba") \
    .option( "query", "select * from public.ar_transactions where cid = 235 AND id between 11104169 AND (11104169+6)" )\
    .load()

df.show()

# .option("dbtable", "public.ar_transactions") \
# .option( "query", "select  min(id), max(id) from ar_transactions where cid = 235" )\
# .option("customSchema", "id DECIMAL(38, 0), name STRING") \

# Transform
# df.registerTempTable('temp_ar')
# df_1 = spark.sql( "select id from temp_ar" )
#
# df_1.show()

# Load

# Saving data to a JDBC source
# df.write \
#     .mode("append")\
#     .format("jdbc") \
#     .option("url", "jdbc:postgresql://192.168.175.3:5432/entrata44_dev") \
#     .option("user", "psDba") \
#     .option("password", "dba") \
#     .option("dbtable", "backup.ar_transactions_back_temp_1") \
#     .save()