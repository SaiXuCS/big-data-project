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



all transfer to lower case

● Person name (Last name, First name, Middle name, Full name)
r"[A-Z][a-z]+,?\s+(?:[A-Z][a-z]*\.?\s*)?[A-Z][a-z]+"

● Business name
r"[a-zA-Z\s]+([inc|llc|LLC|Inc])?"

● Phone Number
r"(\d{3}[-\.\s]??\d{3}[-\.\s]??\d{4}|\(\d{3}\)\s*\d{3}[-\.\s]??\d{4}|\d{3}[-\.\s]??\d{4})"

● Address
r"^[0-9]* (.*), (.*) [a-zA-Z]{2} [0-9]{5}(-[0-9]{4})?$"

● Street name
r"(\d+).*?\s+(st)"

● City
pip install geotext
from geotext import getText
places = GeoText(str)
places.cities


● Neighborhood
no method


● LAT/LON coordinates
r"^([-+]?([1-8]?\d(\.\d+)?|90(\.0+)?))?(,)?(\s*[-+]?(180(\.0+)?|((1[0-7]\d)|([1-9]?\d))(\.\d+)?))?$"

● Zip code
r"^\d{5}$"


● Borough
no method

● School name (Abbreviations and full names)
r"[\w\W]* ((School|center)[\w -]*)[\w\W]*$"

● Color
from matplotlib.colors import is_color_like
is_color_like(str)

● Car make
no method

● City agency (Abbreviations and full names)
no method

● Areas of study (e.g., Architecture, Animal Science, Communications)
no method 

● Subjects in school (e.g., MATH A, MATH B, US HISTORY)
no method

● School Levels (K-2, ELEMENTARY, ELEMENTARY SCHOOL, MIDDLE)
r"(ELEMENTARY|MIDDLE|Middle|Elementary)\s*(SCHOOL|School)*"

● College/University names
r"[\w\W]* ((University|Law School|College|Academy)[\w -]*)[\w\W]*$"

● Websites (e.g., ASESCHOLARS.ORG)
r"((http|https)\:\/\/)?[a-zA-Z0-9\.\/\?\:@\-_=#]+\.([a-zA-Z]){2,6}([a-zA-Z0-9\.\&\/\?\:@\-_=#])*"
● Building Classification (e.g., R0-CONDOMINIUM, R2-WALK-UP)
no method

● Vehicle Type (e.g., AMBULANCE, VAN, TAXI, BUS)
r"(AMBULANCE|VAN|TAXI|TAXI)"

● Type of location (e.g., ABANDONED BUILDING, AIRPORT TERMINAL, BANK,
CHURCH, CLOTHING/BOUTIQUE)
r"(ABANDONED BUILDING|AIRPORT TERMINAL|BANK|CHURCH)"

● Parks/Playgrounds (e.g., CLOVE LAKES PARK, GREENE PLAYGROUND)
r"[a-zA-Z]+\s+(PARK|PLAYGROUND)"


def columnNameType(x):
	arr= []
	res= re.match(r"[A-Z][a-z]+,?\s+(?:[A-Z][a-z]*\.?\s*)?[A-Z][a-z]+", x)
	if res:
		temp= ("Person Name", 1)
		arr.append(temp)
	res= re.match(r"[a-zA-Z\s]+([inc|llc|LLC|Inc])?", x)
	if res:
		temp= ("Business Name", 1)
		arr.append(temp)
	res= re.match(r"(\d{3}[-\.\s]??\d{3}[-\.\s]??\d{4}|\(\d{3}\)\s*\d{3}[-\.\s]??\d{4}|\d{3}[-\.\s]??\d{4})", x)
	if res:
		temp= ("Phone Number", 1)
		arr.append(temp)
	res= re.match(r"^[0-9]* (.*), (.*) [a-zA-Z]{2} [0-9]{5}(-[0-9]{4})?$", x)
	if res:
		temp= ("Address", 1)
		arr.append(temp)
	res= re.match(r"(\d+).*?\s+(st)", x)
	if res:
		temp= ("Street Name", 1)
		arr.append(temp)
	res= re.match(r"^([-+]?([1-8]?\d(\.\d+)?|90(\.0+)?))?(,)?(\s*[-+]?(180(\.0+)?|((1[0-7]\d)|([1-9]?\d))(\.\d+)?))?$", x)
	if res:
		temp= ("LAT/LON", 1)
		arr.append(temp)
	res= re.match(r"^\d{5}$", x)
	if res:
		temp= ("Zip Code", 1)
		arr.append(temp)
	res= re.match(r"[\w\W]* ((School|center)[\w -]*)[\w\W]*$", x)
	if res:
		temp= ("School Name", 1)
		arr.append(temp)
	res= re.match(r"(ELEMENTARY|MIDDLE|Middle|Elementary)\s*(SCHOOL|School)*", x)
	if res:
		temp= ("School Level", 1)
		arr.append(temp)

	res= re.match(r"[\w\W]* ((University|Law School|College|Academy)[\w -]*)[\w\W]*$", x)
	if res:
		temp= ("University Name", 1)
		arr.append(temp)

	res= re.match(r"((http|https)\:\/\/)?[a-zA-Z0-9\.\/\?\:@\-_=#]+\.([a-zA-Z]){2,6}([a-zA-Z0-9\.\&\/\?\:@\-_=#])*", x)
	if res:
		temp= ("Website", 1)
		arr.append(temp)

	res= re.match(r"[a-zA-Z]+\s+(PARK|PLAYGROUND)", x)
	if res:
		temp= ("Playground", 1)
		arr.append(temp)

	if len(arr)== 0:
		temp= ("Other", 1)
		arr.append(temp)
	"""
	res= re.match(r"", x)
	if res:
		arr.append("")
	"""
	return arr

for i in range(len(columns))
	rSingleColumn= rdd.map(lambda x: x[i])
	columnLabel= rSingleColumn.flatMap(columnNameType).reduceByKey(lambda x, y: x+y)
	