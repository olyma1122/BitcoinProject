"""
讀取 2017-11 到 2018-5 的所有 .parquet
並將join
"""
from collections import Counter

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, functions
from pyspark.sql.functions import to_date, unix_timestamp, date_format, from_unixtime, month, udf, countDistinct
from pyspark.sql.types import DateType, TimestampType, ArrayType, StringType, StructField, StructType, IntegerType

import csv
import time

def su(list):
    newlist = ','.join(list)
    return newlist

if __name__ == "__main__":
    start = time.time()
    """
    #讀取檔案
    with open("data.csv") as f:
        reader = csv.reader(f)
        count = 0
        for row in reader:
            count = count + 1

    print(count, "rows")
    """

    #初始化物件
    conf = SparkConf().setMaster("local").setAppName("My APP")
    sc = SparkContext(conf = conf)

    #撰寫程式
    sqlContext = SQLContext(sc)
    print(">>>>>> Loading data (.parquet) ")

    path1 = "Data/Nov2017Apr2018_part1/data.parquet/part*.parquet" #檔案路徑
    path2 = "Data/Nov2017Apr2018_part3/data.parquet/part*.parquet" #檔案路徑


    df1 = sqlContext.read.parquet(path1)
    df1.registerTempTable("df1table")
    df2 = sqlContext.read.parquet(path2)
    df2.registerTempTable("df2table")
    df2 = df2.drop("timestamp")
    #df2 = df2.distinct()
    df1.show()
    df2.show()

    df2groupby = df2.groupBy("transaction_id").agg(functions.collect_set("transactions_input_pubkey_base58"), functions.collect_set("transactions_input_pubkey_base58_error"))
    df2groupby.registerTempTable("groupbytable")
    df2groupby.show()
    df2groupby.printSchema()

    #gb = sqlContext.sql("select * from newtable where transaction_id='acd203e8ad5adda542131d6c75417011c1d0f9a64ebd57b43579b226cc6a608b'")
    #gb.show()

    df1rows = df1.count()
    df2rows = df2.count()
    df2groupbyrows = df2groupby.count()

    print("df1rows:", df1rows)
    print("df2rows:", df2rows)
    print("df2groupbyrows: ", df2groupbyrows)


    #df1.select("transaction_id").distinct().count()
    #df2.select("transaction_id").distinct().count()

    #print(df1.select("transaction_id").distinct().count())
    #print(df2.select("transaction_id").distinct().count())

    #print(distcount)
    str = udf(lambda x: su(x), StringType())

    df2preprocess = df2groupby.select("transaction_id", str("collect_set(transactions_input_pubkey_base58)").alias("transactions_input_pubkey_base58"), str("collect_set(transactions_input_pubkey_base58_error)").alias("transactions_input_pubkey_base58_error"))
    print(df2preprocess)
    df2preprocess.show()
    df2preprocess.printSchema()

    print(">>>>>> Dataframe Join")
    dfjoin = df1.join(df2preprocess, "transaction_id", "left_outer")
    dfjoin.show()
    dfjoin.printSchema()

    dfjoinrows = dfjoin.count()
    print("dfjoinrows: ", dfjoinrows)


    print(">>>>>> .parquent")
    dfjoin.write.parquet("Data/DateSet/data.parquet")

    """
    df.registerTempTable("dfTable")
    df.withColumn("date_again", functions.from_unixtime("timestamp")).show()

    te = sqlContext.sql("select cast(from_unixtime(timestamp/1000)as date) as ee from dfTable")

    te.show()

    te.withColumn("month", month("ee")).show() #取月份
    
    """

    end = time.time()
    print("TOTAL RUN:", end-start, "secs.")
