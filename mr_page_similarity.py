from mrjob.job import MRJob
import happybase
from DB import DB, DB_NAME, Hbase
import re
import json
import time


#Import stop words and table of page summaries as global variables
file = open('stop_words.txt', 'r')
file_list = []
for r in file:
        file_list.extend(r.split(','))
stop_words = set(file_list)

contentHash={}
with open('pages.json') as data_file:
    for line in data_file:
                d = dict(json.loads(line))
                contentHash[d['page_id']]=d['summary']

#Create an Hbase for similarity values
hbase = Hbase(DB_NAME)
hbase_connection = hbase.get_connection()
try:
        hbase_connection.create_table("mr_pages_similarity_umls", {'paired_page':dict()})
except happybase.hbase.ttypes.AlreadyExists:
        pass

#Start the timer
start = time.time()
startMap = 0
finish = 0

class MRPageAssociate(MRJob):
        def mapper(self,_,line):
                j= json.loads(line)
                d = dict(j)
                page1_summary = set(re.findall(r"[\w']+", d['summary'].lower())).difference(stop_words)
                for key in contentHash.keys():
                        if key == d['page_id']: # don't output a page's similarity to itself
                                continue
                        page2_summary = set(re.findall(r"[\w']+", contentHash[key].lower())).difference(stop_words)
                        if len(page2_summary.union(page1_summary)) == 0:
                                continue
                        jacc_similarity = (1.0 * len(page2_summary.intersection(page1_summary))) / len(page2_summary.union(page1_summary))
                        if jacc_similarity == 0: # do not include pages that have similarity values = 0
                                continue
                        else:
                                yield (d['page_id'],key), jacc_similarity

        def reducer(self,key,value):
                value = list(value)
                hbase = Hbase(DB_NAME)
                sim_pages = hbase.table("mr_pages_similarity_umls")
                #insert keys and similarity values in hbase
                sim_pages.put(str(key[0]), {'paired_page:'+str(key[1]):str(value[0])})
                sim_pages.put(str(key[1]), {'paired_page:'+str(key[0]):str(value[0])})
                yield key,value

def main():
        # test to make sure values made it into hbase
        hbase = Hbase(DB_NAME)
        sim_pages = hbase.table("mr_pages_similarity_umls")
        print sim_pages.cells('752675','paired_page',versions=20000,include_timestamp=True)

if __name__ == '__main__':
        startMap = time.time() - start
        MRPageAssociate.run()
        finish = time.time() - start
        print "load time: " + str(startMap)
        print "total time: " + str(finish)
        #main()
