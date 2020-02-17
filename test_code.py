import unittest
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import QueryParquetFiles as qpf
import os

path = input("Type local directory where the python files are stored. E.g. C:\dlgTest >> ") #'C:\dlg' ##

def get_latest_dataset_location(path):
	os.chdir(path)
	filepath = path +"\\dataset_location.txt"
	with open(filepath) as f:
		return f.read()
filename = 	get_latest_dataset_location(path)	
#print (filename)

class TestCode(unittest.TestCase):
	def test_get_top_temperature(self):
		#test case 1
		self.assertEqual(qpf.get_top_temperature(filename), 15.8)
		#test case2
		table = pq.read_table(filename, columns=['ScreenTemperature']).to_pandas()
		t= table.mask(table== 15.8, 20)
		result = t.loc[t['ScreenTemperature'].idxmax()]
		result = result['ScreenTemperature']
		
		self.assertEqual(result, 20)
		
		#test case3
		table = pq.read_table(filename, columns=['ScreenTemperature']).to_pandas()
		t= table.mask(table==-99, 99)
		result = t.loc[t['ScreenTemperature'].idxmax()]
		result = result['ScreenTemperature']
		#print (result)
		self.assertEqual(result, 99)
		#...
		
	def test_get_hottest_day(self):
		#test case 1
		self.assertEqual(qpf.get_hottest_day(filename), '2016-03-17')
		#test case 2
		table = pq.read_table(filename, columns=['ObservationDate','ScreenTemperature']).to_pandas()
		result = table.loc[table['ScreenTemperature']==15.3]
		day = result['ObservationDate']
		day = day.max()
		day = day.split("T", 1)
		day = str(day[0]).rstrip().lstrip()
		#print (day)
		self.assertEqual(day, '2016-03-16')
		#...
		
	def test_get_region(self):
		self.assertEqual(qpf.get_region(filename), 'Highland & Eilean Siar')
		#...
if __name__=='__main__':
	unittest.main()