#python -m pip install --upgrade pip
#pip install pyarrow
#pip install pandas

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
import calendar
from datetime import datetime as dt
import time as t


starttime = calendar.timegm(t.gmtime())
path = input("To re/load  to paraquet, type a local directory store files. E.g. C:\dlgTest >> ") #'C:\dlg'#
os.chdir(path)

def convertto_parquet():
	starttime = calendar.timegm(t.gmtime())
	global t_count
	t_count = 0
	#''' #convert all csv to paraquet then combine the parquet files.
	#find all csv files and iterate through converting each to parquet file
	filename = 'parquet-dataset-' + str(starttime)
	for file in os.listdir(path):
		if file.endswith(".csv"):
			df = pd.read_csv(file)
			row_count = len(df)
			t_count = t_count + row_count
			tb = pa.Table.from_pandas(df)
			pq.write_to_dataset(tb, root_path=filename)
			print('****** '+ str(file) +' with '+ str(row_count) +' rows converted to parquet *******' )
	print ('****** ' + str(t_count) +' is the total number of rows in CSV received **************')
	with open("dataset_location.txt","w") as text_file:
		text_file.write("{}".format(filename))		
	location = str(path) + "\\" + str(filename)
	table = pq.read_table(filename).to_pandas()
	p_count= len(table)
	print ('****** ' + str(p_count)  +' is the total number of rows converted to parquet  *******'+"\n")
	print('*************************************************************')
	print('All files converted from CSV to parquet completed and stored in ' + location +"\n")	
	print('*************************************************************')
	return path

#run conversion
convertto_parquet()

def get_latest_dataset_location(path):
	os.chdir(path)
	filepath = path +"\\dataset_location.txt"
	with open(filepath) as f:
		return f.read()
filename = 	get_latest_dataset_location(path)


def get_all_results():
	table = pq.read_table(filename, columns=['ObservationDate','ScreenTemperature','Region']).to_pandas()
	result = table.loc[table['ScreenTemperature'].idxmax()]
	temperature = result['ScreenTemperature']
	hottestday = result['ObservationDate']
	hottestday = hottestday.split("T",1)
	region = result['Region']
	print ('The temperature on the hottest day was ' + str(temperature) + ' Celsuis')
	print ('The hottest day was ' + str(hottestday[0]))
	print('The region with the hottest day was ' + region)
	print('*************************************************************')
	print ('Alternatively use the pyspark commands provided to query the parquet dataset :)')
'''
	result = result.rename(index={"ScreenTemperature":"Temperature","ObservationDate":"Hottest_Day"})
	answer = result.to_string(index=True)
	print ('Below is the answers to the test qestions')
	print('-------------------------------------------')
	print(answer)
	print('-------------------------------------------')
	

'''
#get_all_results()

	