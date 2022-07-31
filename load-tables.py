#!/usr/bin/env python3

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import sys
import os
import time
import argparse
import re
import socket


def init_spark(is_dpp_enabled):

    host = socket.gethostname()
    hostname = host + ".neuroblade.corp"
    master_port = "7077"
    master_node="spark://{}:{}".format(hostname, master_port)
    print("master_node: "+ master_node)

    config = SparkConf()
    spark_conf_path = "/opt/spark/conf/spark-defaults.conf"
    spark_config = get_spark_conf(spark_conf_path)
    spark_config["spark.sql.optimizer.dynamicPartitionPruning.enabled"] = is_dpp_enabled
    print(spark_config)
    print()
    
    for key in spark_config:
        config2 = config.set(key, spark_config[key])
    
    print(config2.getAll())
    sc = SparkContext(conf=config2, master=master_node)
    spark = SQLContext(sc)
    return spark

    sc = SparkContext(conf=conf2, master=master_node)

def get_spark_conf(conf_file):
    result = {}
    if os.path.exists(conf_file):
        with open(conf_file, 'r') as f:
            for line in f:
                kv = line.split('=', 1)
                if line.startswith('#') or len(kv) < 2:
                    continue
                print(line)
                result[kv[0]] = kv[1].strip()
    return result

def read_tpcds_data(spark, data_path):
    tables = ["call_center",
            "catalog_page",
            "catalog_returns",
            "customer",
            "customer_address",
            "customer_demographics",
            "date_dim",
            "household_demographics",
            "income_band",
            "inventory",
            "item",
            "promotion",
            "reason",
            "ship_mode",
            "store",
            "store_returns",
            "store_sales",
            "time_dim",
            "warehouse",
            "web_page",
            "web_returns",
            "web_sales",
            "catalog_sales",
            "web_site"]

    for table_name in tables:
        print("creating view: ", table_name, os.path.join(data_path, table_name))
        df = spark.read.parquet(os.path.join(data_path, table_name))
        df.createTempView(table_name)

    return


def run_queries(spark, queries):
    for file_path in queries:
        run_query(spark, file_path)

    return

def run_query(spark, query_file):
    print("------------------------------------------------------------------------------------------------")
    print("------------------------------------------------------------------------------------------------")
    print("RUNNING QUERIES FROM FILE: " + query_file)
    print("------------------------------------------------------------------------------------------------")
    print("------------------------------------------------------------------------------------------------")
    print("\n\n\n")

    with open(query_file, "rt") as fp:
        query = fp.read()
        try:
            start_time = time.time()
            results = spark.sql(query)
            results.show()
            end_time = time.time()
        except Exception as e:
            print(e)
            return None
    
    execution_time = end_time - start_time
    print("Running time: " + str(execution_time))
    return results


def main():
    global options
    parser = argparse.ArgumentParser("Queries Executer")
    parser.add_argument("--query_file_path", "--query", help = "query file path to run")
    parser.add_argument("--list_of_queries", "--query_list", help= "list of queries files")
    parser.add_argument("--data_dir", "--data", help= "the data directory of all parquets")
    parser.add_argument("--dpp", default="true", help= "if true, dpp is enabled")
    options = parser.parse_args()

    spark = init_spark(options.dpp)
    read_tpcds_data(spark, options.data_dir)
    
    if options.list_of_queries:
        queries = options.list_of_queries.split(" ")
        ret_value = run_queries(spark, queries)
    else:
        print(options.query_file_path)
        ret_value = run_query(spark, options.query_file_path)

    if ret_value:
        print("Running passed!")
    else:
        print("Running failed!")



if __name__ == '__main__':
    main()

