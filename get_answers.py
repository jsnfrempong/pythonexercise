import QueryParquetFiles as qpf
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
import calendar
from datetime import datetime as dt
import time as t

def get_answers():
	#get results
	temperature = qpf.get_top_temperature(qpf.filename)
	answer = 'The temperature on the hottest day was ' + str(temperature) + ' Celsuis \n'
	print(answer)
	day = qpf.get_hottest_day(qpf.filename)
	answer = 'The hottest day was ' + day + '\n'
	print (answer)
	region = qpf.get_region(qpf.filename)
	answer = 'The region with the hottest day was ' + region +'\n'
	print (answer)

print('***********************************************************')
get_answers()
print('***********************************************************')