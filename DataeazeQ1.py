#!/usr/bin/python3

from pyspark.sql import SparkSession
import pyspark.sql.functions as func
from pyspark.sql.types import IntegerType

# create SparkSession object
spark = SparkSession.builder\
            .appName("confidential")\
            .config("spark.sql.shuffle.partitions", "2")\
            .getOrCreate()


#converting the parquet file into dataframe
df1 = spark.read.format("parquet")\
    .load("/home/ShardaW/Downloads/confidential.snappy.parquet")
# df.show()
# df.printSchema()

#converting the csv file into dataframe
df2 = spark.read\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .csv("/home/ShardaW/Downloads/nonConfidential.csv")

# combine the above tw.o dataframe
df3 = df1.union(df2)
df3.printSchema()
print(df3.schema)
# df3.show()


result1 = df3\
    .select("ProjectTypes","LEEDSystemVersionDisplayName")\
    .groupby("ProjectTypes","LEEDSystemVersionDisplayName").count()

# result1.show()

result2 = df3\
    .select("OwnerTypes","LEEDSystemVersionDisplayName")\
    .filter(df3.City=="Virginia")\
    .groupby("OwnerTypes").agg(func.count("LEEDSystemVersionDisplayName"))

# result2.show()




result4 = df3\
      .select("Zipcode","ProjectName")\
      .filter(df3.City=="Virginia")\
      .groupby("Zipcode").agg(func.count("ProjectName"))\
      .orderBy(func.count("ProjectName"), ascending=False)\
      .limit(1)

# result4.show()




# result3 = df3\
#     .select("Virginia")\
#     .withColumn("GrossSqFoot_float", df2["GrossSqFoot"].cast('float')).drop("GrossSqFoot")\
#     .filter(df3.IsCertified=="Yes")\
#     .groupby("Virginia").agg(sum("GrossSqFoot_float").alias("total Gross Square Feet"))
#
# result3.show()



input("press enter to exit ...")

spark.stop()
