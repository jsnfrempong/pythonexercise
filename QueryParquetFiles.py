#python -m pip install --upgrade pip
#pip install pyarrow
#pip install pandas
#pip install uuid
#from pyspark.sql import dataframe as psdf
#from pyspark.sql import SparkSession
#pip install tabulate

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
import calendar
from datetime import datetime as dt
import time as t 
	
def get_latest_dataset_location():
	#os.chdir(path)
	filepath = os.getcwd() +"\\dataset_location.txt"
	with open(filepath) as f:
		filename = f.read()
		return filename

filename = 	get_latest_dataset_location()

def get_top_temperature(filename):
	table = pq.read_table(filename, columns=['ScreenTemperature']).to_pandas()
	result = table.loc[table['ScreenTemperature'].idxmax()]
	temperature = result['ScreenTemperature']
	return temperature


def get_hottest_day(filename):
	table = pq.read_table(filename, columns=['ObservationDate','ScreenTemperature']).to_pandas()
	result = table.loc[table['ScreenTemperature']==get_top_temperature(filename)]
	day = result['ObservationDate'].to_string(index=False)
	day = day.split("T", 1)
	day = str(day[0]).rstrip().lstrip()
	return day


def get_region(filename):
	table = pq.read_table(filename, columns=['Region','ScreenTemperature']).to_pandas()
	result = table.loc[table['ScreenTemperature']==get_top_temperature(filename)]
	region = result['Region'].to_string(index=False)

	return region.rstrip().lstrip()



'''
result = result.rename(index={"ScreenTemperature":"Temperature","ObservationDate":"Hottest_Day"})
answer = result.to_string(index=True)
print ('Below is the answers to the test qestions')
print('-------------------------------------------')
print(answer)
print('-------------------------------------------')
print ('Alternatively use the pyspark commands provided to query the parquet dataset :)')
'''



#'''	