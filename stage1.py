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
import math

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

fileName= "/user/hm74/NYCOpenData/"+"yhuu-4pt3.tsv.gz"
df= spark.read.format('csv').options(header='true',inferschema='true', delimiter='\t').load(fileName)
df.createOrReplaceTempView("df")
rdd= df.rdd.map(list)
columns= df.columns



#type sum
i= 0
col= columns[i]
rSingleColumn= rdd.map(lambda x: str(x[i]))
rSingleColumn= rSingleColumn.map(lambda x: (str(x), 1))

rNonEmpty= rSingleColumn.filter(lambda x: x[0]!= "")
rNonEmpty= rNonEmpty.map(lambda x: x[1])
nonEmpty= rNonEmpty.reduce(lambda x, y: x+y)
print(nonEmpty)

rRmpty= rSingleColumn.filter(lambda x: x[0]== "")
rRmpty= rRmpty.map(lambda x: x[1])
if rRmpty.isEmpty()== False:
	rRmpty= rRmpty.reduce(lambda x, y: x+y)
else:
	rRmpty= 0


freqEach= rSingleColumn.reduceByKey(lambda x, y: x[1]+y[1])
freqEach= freqEach.sortBy(lambda x: x[1], False)

dist= rSingleColumn.map(lambda x: x[0]).distinct()
dist.collect()





#min max value
import re
def getType(x):
	if type(x)!= type("a"):
		return type(x)
	else:
		p1 = r"\d{4}\/\d{1,2}\/\d{1,2}"
		pattern1 = re.compile(p1)
		find = pattern1.findall(x)
		if(len(find)!= 0):
			return 'Date'

		p1 = r"\d{1,2}\/\d{1,2}\/\d{4}"
		pattern1 = re.compile(p1)
		find = pattern1.findall(x)
		if(len(find)!= 0):
			return 'Date'

		p1 = r"\d{4}\:\d{1,2}\:\d{1,2}"
		pattern1 = re.compile(p1)
		find = pattern1.findall(x)
		if(len(find)!= 0):
			return 'Date'

		return type("a")

#i= -1
i= 3
rSingleColumn= rdd.map(lambda x: x[i])
typeList= rSingleColumn.map(getType).distinct()

import math

def MapFuncInt(x):
	maxValue= x[0]
	minValue= x[0]
	total= 0
	count= 0
	for i in x:
		total+= i 
		count+= 1
		if i> maxValue:
			maxValue= i 
		if i< minValue:
			minValue= i 
	mean= total/count
	stdSum= 0
	for i in x:
		stdSum+= (i-mean)**2
	std= math.sqrt(stdSum)/count
	return (maxValue, minValue, mean, std)

import datetime

def MapFuncDate(x):
	minDate= ""
	maxDate= ""
	for i in x:
		date= ""
		p1 = r"\d{4}\/\d{1,2}\/\d{1,2}"
		pattern1 = re.compile(p1)
		find = pattern1.findall(i)
		if(len(find)!= 0):
			date= datetime.datetime.strptime(i, '%Y/%m/%d').strftime('%Y/%m/%d')

		p1 = r"\d{1,2}\/\d{1,2}\/\d{4}"
		pattern1 = re.compile(p1)
		find = pattern1.findall(x)
		if(len(find)!= 0):
			date= datetime.datetime.strptime(i, '%m/%d/%Y').strftime('%Y/%m/%d')

		p1 = r"\d{4}\:\d{1,2}\:\d{1,2}"
		pattern1 = re.compile(p1)
		find = pattern1.findall(x)
		if(len(find)!= 0):
			date= datetime.datetime.strptime(i, '%Y:%m:%d').strftime('%Y/%m/%d')

		if minDate== "":
			minDate= date 
		elif minDate>date:
			minDate= date 

		if maxDate== "":
			maxDate= date 
		elif maxDate<date:
			maxDate= date 

	return (minDate, maxDate)



def groupByType(x):
	if type(x)!= type("a"):
		return (type(x),x)
	else:
		p1 = r"\d{4}\/\d{1,2}\/\d{1,2}"
		pattern1 = re.compile(p1)
		find = pattern1.findall(x)
		if(len(find)!= 0):
			return ('Date', x)

		p1 = r"\d{1,2}\/\d{1,2}\/\d{4}"
		pattern1 = re.compile(p1)
		find = pattern1.findall(x)
		if(len(find)!= 0):
			return ('Date', x)

		p1 = r"\d{4}\:\d{1,2}\:\d{1,2}"
		pattern1 = re.compile(p1)
		find = pattern1.findall(x)
		if(len(find)!= 0):
			return ('Date', x)

		return (type("a"),x)

t1= rSingleColumn.map(groupByType).groupByKey().mapValues(list)
intFloatValue= t1.filter(lambda x: x[0]== type(1) or x[0]== type(1.0)).map(lambda x: x[1]).map(MapFuncInt)
dateValue= t1.filter(lambda x: x[0]== 'Date').map(lambda x: x[1]).map(MapFuncDate)

	
"""	
for i in range(len(columns)):
	col= columns[i]
	rSingleColumn= rdd.map(lambda x: str(x[i]))
	rSingleColumn= rSingleColumn.map(lambda x: (str(x), 1))

	rNonEmpty= rSingleColumn.filter(lambda x: x[0]!= "")
	rNonEmpty= rNonEmpty.map(lambda x: x[1])
	nonEmpty= rNonEmpty.reduce(lambda x, y: x+y)

	rRmpty= rSingleColumn.filter(lambda x: x[0]== "")
	rRmpty= rRmpty.map(lambda x: x[1])
	rRmpty= rRmpty.reduce(lambda x, y: x+y)

	freqEach= rSingleColumn.reduceByKey(lambda x, y: x[1]+y[1])
	freqEach.sortBy(lambda x: x[1], False)

	dist= rSingleColumn.map(lambda x: x[0]).distinct()

	rSingleColumn= rdd.map(lambda x: x[i])
	typeList= rSingleColumn.map(lambda x: type(x)).distinct()
	


	def MapFunc(x):
		maxValue= x[0]
		minValue= x[0]
		total= 0
		count= 0
		for i in x:
			total+= i 
			count+= 1
			if i> maxValue:
				maxValue= i 
			if i< minValue:
				minValue= i 
		mean= total/count
		stdSum= 0
		for i in x:
			stdSum+= (i-mean)**2
		std= math.sqrt(stdSum)/count
		return (maxValue, minValue, mean, std)

	t1= rSingleColumn.map(lambda x: (type(x), x)).groupByKey().mapValues(list)
	maxValue= t1.map(lambda x: x[1]).map(MapFunc)

"""


"""
sql 
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
"""
	