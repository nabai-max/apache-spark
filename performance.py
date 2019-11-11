#!/usr/bin/python3

from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc
import subprocess

import sys

class Performance():
    def __init__(self):
        self.conf = SparkConf().setAppName('Performance App')
        self.sc=SparkContext(conf=self.conf)
        
    def load_year(self, year):        
        spark = SparkSession.builder.appName("performance-app").config("spark.config.option", "value").getOrCreate()
        self.df = spark.read.option("header", "true").csv(year)
        return self.df
    
    def originated_airport_with_most_flights(self, out_dir):
        orig_airports = self.df.groupBy('Origin').count().orderBy(desc('count'))
        self.sc.parallelize(orig_airports.collect()).saveAsTextFile(out_dir)
        return orig_airports.first()
        
def delete_out_dir(out_dir):
        subprocess.call(["hdfs", "dfs", "-rm", "-R", out_dir])           
        
def main(argv):
    delete_out_dir(argv[1])
    perf = Performance()
    perf.load_year(argv[0])
    most_count = perf.originated_airport_with_most_flights(argv[1])
    print('{} has the most originated flights at {}'.format(most_count['Origin'], most_count['count']))

if __name__ == '__main__':
    main(sys.argv[1:])