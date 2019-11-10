from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import SparkSession

import sys

class Performance():
    def __init__(self):
        self.conf = SparkConf().setAppName('Performance App')
        self.sc=SparkContext(conf=self.conf)
        
    def load_year(self, year):        
        spark = SparkSession.builder.appName("performance-app").config("spark.config.option", "value").getOrCreate()
        self.df = spark.read.option("header", "true").csv(year)
        # self.df = spark.read.option("header", "true").csv(year).select('*').toPandas()
        return self.df
    
    
def main(argv):
    perf = Performance()
    df = perf.load_year(argv[0])
    df[0]
    print(df)
    
    # for row in df.itertuples():
    #     buf = ''
    #     for col in range(1, len(row)):
    #         buf += row[col] + ' '
    #     print(buf)


if __name__ == '__main__':
    main(sys.argv[1:])