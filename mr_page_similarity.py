from mrjob.job import MRJob
import happybase
from DB import DB, DB_NAME, Hbase
import re
import json

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
                
class MRPageAssociate(MRJob):
        def mapper(self,_,line):        
                j= json.loads(line)
                d = dict(j)
                page1_summary = set(re.findall(r"[\w']+", d['summary'].lower())).difference(stop_words)
                for key in contentHash.keys():
                        if key == d['page_id']:
                                continue
                        page2_summary = set(re.findall(r"[\w']+", contentHash[key].lower())).difference(stop_words)
                        jacc_similarity = (1.0 * len(page2_summary.intersection(page1_summary))) / len(page2_summary.union(page1_summary))
                        yield (d['page_id'],key), jacc_similarity

        def reducer(self,key,value):
                value = list(value)
                yield key,value

if __name__ == '__main__':
        MRPageAssociate.run()
