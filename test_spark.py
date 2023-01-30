import findspark
findspark.find()    
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[1]").getOrCreate()
   
from pyspark.sql.functions import split
df = spark.createDataFrame([('oneAtwoBthreeC',)], ['s',])

df.select(split(df.s, '[ABC]', 0).alias('s')).show()

df.select(split(df.s, '[ABC]', 1).alias('s')).show()

df.select(split(df.s, '[ABC]', 2).alias('s')).show()

df.select(split(df.s, '[ABC]', -1).alias('s')).show()
