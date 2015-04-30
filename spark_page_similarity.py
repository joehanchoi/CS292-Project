import pyspark
from pyspark import SparkContext, SparkConf
import numpy as np
import happybase
import json
import re
from DB import DB, DB_NAME, Hbase

class Pages:
	def __init__(self):
		self.hbase = Hbase(DB_NAME)
		self.create_similarity_hbase_table()
		self.table = self.hbase.table("spark_pages_similarity_umls_subset")

	def create_similarity_hbase_table(self):
		self.hbase_connection = self.hbase.get_connection()
		try:
			self.hbase_connection.create_table("spark_pages_similarity_umls_subset", {'paired_page':dict()})
		except happybase.hbase.ttypes.AlreadyExists:
			pass

	def insert(self, pid1, pid2, sim):
		self.table.put(str(pid1), {'paired_page:'+str(pid2):str(sim)})

def spark_similarity(data, stop_words):
	spark_pages = Pages()
	conf = SparkConf().setAppName('J_Similarity').setMaster('local[4]')#.set("spark.storage.memoryFraction","0.1")#.set("spark.cores.max", "1")
	sc = SparkContext(conf=conf)
	input = [(pid, set(re.findall(r"[\w']+", data[pid].lower())).difference(stop_words)) for pid in data.keys() if len(set(re.findall(r"[\w']+", data[pid].lower())).difference(stop_words))>0]
	step = int(1.0*len(input)/1000)
	for i in xrange(0,len(input),step):
		full = sc.parallelize(input) \
			.keyBy(lambda x:1)
		sub = sc.parallelize(input[i:i+step]) \
			.keyBy(lambda x:1)
		#result = sc.parallelize(input) \
			#.keyBy(lambda x:1)
		#result = result.join(result) \
		result = sub.join(full) \
			.map(lambda x:x[1]) \
			.map(lambda x:((x[0][0], x[1][0]), (1.0 * len(x[0][1].intersection(x[1][1]))) / len(x[0][1].union(x[1][1]))))
		result = result.collect()
		print 'Finished partition. Terms finished:', i
		for pid, sim in result:
			if sim > 0:
				spark_pages.insert(pid[0],pid[1],sim)

def spark_test(data, stop_words):
	spark_pages = Pages()
	conf = SparkConf().setAppName('J_Similarity').setMaster('local[4]').set("spark.cores.max","1")
	sc = SparkContext(conf=conf)
	input = [(pid, set(re.findall(r"[\w']+", data[pid].lower())).difference(stop_words)) for pid in data.keys() if len(set(re.findall(r"[\w']+", data[pid].lower())).difference(stop_words))>0]
	result = sc.parallelize(input) \
		.keyBy(lambda x:1)
	result = result.join(result) \
		.map(lambda x:x[1]) \
		.map(lambda x:((x[0][0], x[1][0]), (1.0 * len(x[0][1].intersection(x[1][1]))) / len(x[0][1].union(x[1][1]))))
	result = result.collect()

def read_sql():
	with open('stop_words.txt', 'rb') as f:
		stop_list = f.readlines()
	stop_words = set()
	for line in stop_list:
		stop_words.update(line.split(','))

	data = {}
	db = DB(DB_NAME)

	result = db.query("select page_id, summary from med_pages_umls_subset")
	for row in result:
		data[row[0]] = row[1]

	return data, stop_words

def read_json(file_name):
	with open('stop_words.txt', 'rb') as f:
		stop_list = f.readlines()
	stop_words = set()
	for line in stop_list:
		stop_words.update(line.split(','))

	data={}
	with open(file_name, 'rb') as data_file:
	    for line in data_file:
			d = dict(json.loads(line))
			data[d['page_id']]=d['summary']
	return data, stop_words

def main():
	#data, stop_words = read_json('pages.json')
	data, stop_words = read_sql()
	spark_test(data, stop_words)
	#spark_similarity(data, stop_words)

if __name__=='__main__':
	main()
