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

    def originated_airport_with_least_flights(self, out_dir):
        orig_airports = self.df.groupBy('Origin').count().orderBy('count',ascending=True)
        self.sc.parallelize(orig_airports.collect()).saveAsTextFile(out_dir)
        return orig_airports.first()

    def departure_airport_with_most_flights(self, out_dir):
        dep_airports = self.df.groupBy('Dest').count().orderBy(desc('count'))
        self.sc.parallelize(dep_airports.collect()).saveAsTextFile(out_dir)
        return dep_airports.first()
    
    def departure_airport_with_least_flights(self, out_dir):
        dep_airports = self.df.groupBy('Dest').count().orderBy('count',ascending=True)
        self.sc.parallelize(dep_airports.collect()).saveAsTextFile(out_dir)
        return dep_airports.first()

   
        
def delete_out_dir(out_dir):
    subprocess.call(["hdfs", "dfs", "-rm", "-R", out_dir])           
        
def main(argv):
    delete_out_dir(argv[1])
    perf = Performance()
    perf.load_year(argv[0])
    most_count = perf.originated_airport_with_most_flights(argv[1])
    print('{} has the most originated flights at {}'.format(most_count['Origin'], most_count['count']))

    ### New code to be implemented
def main(argv):
    delete_out_dir(argv[1])
    perf = Performance()
    perf.load_year(argv[0])
    least_count = perf.originated_airport_with_least_flights(argv[1])
    print('{} has the least originated flights at {}'.format(least_count['Origin'], least_count['count']))

def main(argv):
    delete_out_dir(argv[1])
    perf = Performance()
    perf.load_year(argv[0])
    most_count = perf.departure_airport_with_most_flights(argv[1])
    print('{} has the most departure flights at {}'.format(most_count['Dest'], most_count['count']))

def main(argv):
    delete_out_dir(argv[1])
    perf = Performance()
    perf.load_year(argv[0])
    most_count = perf.departure_airport_with_least_flights(argv[1])
    print('{} has the least departure flights at {}'.format(most_count['Dest'], most_count['count']))




'''    
    delete_out_dir(argv[1])
    least_count = perf.originated_airport_with_least_flights(argv[1])
    print('{} has the least originated flights at {}'.format(least_count['Origin'], most_count['count']))

    delete_out_dir(argv[1])
    least_count = perf.destination_airport_with_least_flights(argv[1])
    print('{} has the least destination flights at {}'.format(least_count['Dest'], most_count['count']))

    delete_out_dir(argv[1])
    most_count = perf.destination_airport_with_most_flights(argv[1])
    print('{} has the most destination flights at {}'.format(most_count['Dest'], most_count['count']))
'''
    
if __name__ == '__main__':
    main(sys.argv[1:])
