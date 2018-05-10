"""
讀取 2017-11 到 2018-5 的所有 .csv
並轉換格式為 .parquent
"""
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

import csv
import time


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
    print(">>>>>> Loading data ")

    path = "Data/set1/data*.csv" #檔案路徑
    original_df = sqlContext.read.format("csv").options(header="true", inferSchema="true").load(path)
    df = original_df.toDF("timestamp", "block_id", "previous_block", "difficultyTarget", "nonce", "work_terahash", "work_error", "transaction_id", "transactions_output_satoshis", "transactions_output_pubkey_base58", "transactions_output_pubkey_base58_error")

    df.show()
    df.printSchema()
    end = time.time()
    print("Loading data runs:", end-start, "secs.")

    rows = df.count()
    print("rows:", rows)

    print(">>>>>> transform .csv --> .parquent ")
    df.write.parquet("Data/Nov2017Apr2018_part1/data.parquet")

    end = time.time()
    print("TOTAL RUN:", end-start, "secs.")

