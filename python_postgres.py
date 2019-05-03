import psycopg2
from pyspark import SparkContext
from pyspark.sql import SparkSession

sc = SparkContext()
spark = SparkSession(sc)

conn = psycopg2.connect( """host=192.168.175.3 
                            port=5432 
                            dbname=entrata44_dev 
                            user=psDba 
                            password=dba""" )

cursor = conn.cursor()
# cursor. execute( "SELECT * FROM subsidy_certifications LIMIT 1000" )

sql = """SELECT
                at.id,
                at.cid,
                at.post_month,
                at.transaction_amount,
                dense_rank() OVER( PARTITION BY at.post_month, at.cid ORDER BY at.transaction_amount )
            FROM
                ar_transactions at
            WHERE
            at.transaction_amount < 0;"""


import time

start_time = time.clock()
cursor.execute( "SELECT * FROM subsidy_certifications" )
# cursor.execute( sql )

rows = cursor.fetchall()
rdd = sc.parallelize( rows )
print( type( rdd ) )

df = rdd.toDf()
print( type( df ) )
# df.registerTempTable("transactipons")
#
# start_time = time.clock()
# df2 = spark.sql( sql )
# print ( "spark process time: ", time.clock() - start_time, "seconds" )
#
#
# # counter = 0
# # for item in df.collect():
# #   counter += 1
# print( type( df ) )
# print( type( df2 ) )

# print( counter )
# print( "{}{}".format( df.__sizeof__(), " bytes" ) )




