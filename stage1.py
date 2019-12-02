import sys
from pyspark.sql import SparkSession
from csv import reader
from pyspark import SparkContext
import gzip
from pyspark.sql import *
from pyspark.sql.functions import *
import os
from os import listdir
from os.path import isfile, join

mypath= "NYCOpenData"
gzFiles = [f for f in listdir(mypath) if isfile(join(mypath, f))]
sc = SparkContext()
spark = SparkSession.builder.appName("finak").config("spark.some.config.option", "some-value").getOrCreate()	

"""
for file in gzFiles:
	fileName= "/user/hm74/NYCOpenData/"+file
	df = spark.read.format('tsv').options(delimiter= '\t', header= 'true', inferschema= 'true', delimiter='\t').load(fileName)
	df.createOrReplaceTempView("df")
	rdd = sc.textFile(fileName, 1).mapPartitions(lambda x: reader(x))
"""

fileName= "/user/hm74/NYCOpenData/"+"97pn-acdf.tsv.gz"
df= spark.read.format('csv').options(header='true',inferschema='true', delimiter='\t').load(fileName)
df.createOrReplaceTempView("df")
rdd= df.rdd


#number of non empty cells and empty cell
columns= df.columns
for col in columns:
	sql1= "SELECT count("+col+") as total FROM df"
	total = spark.sql(sql1)
	total= total.select(format_string("%s",total.total))

	sq1= "SELECT count("+col+") as empty FROM df where "+col+"<>''"
	empty = spark.sql(sql1)
	empty= total.select(format_string("%s",empty.empty))

	sql1= "SELECT count(distinct "+col+") as dist FROM df"
	dist = spark.sql(sql1)
	dist= total.select(format_string("%s",dist.dist))

	sql1= "select * from (SELECT "+col+",count(*) as total FROM df group by "+col+") as temp order by temp.total desc limit 5 "
	top = spark.sql(sql1)
	top= total.select(format_string("%s",dist.dist))

	