from DB import DB, DB_NAME, Hbase
import collections
import json

class Table:
        def __init__(self, db_name, table_name, selection_condition=None, limit=None):
                self.table_name = table_name
                self.selection_condition = selection_condition
                self.limit = limit
                self.db = DB(db_name)
                self.cur = self.db.cursor()

        def get_next(self):
                q = 'select * from %s ' % self.table_name
                if self.selection_condition != None:
                        q += 'WHERE %s ' % self.selection_condition
                if self.limit != None:
                        q += 'LIMIT %d ' % self.limit
        #       print 'Executing: %s ' % q
                self.cur.execute(q)
                for row in self.cur.fetchall():
                        yield row




db = DB(DB_NAME)
pages = Table(DB_NAME, 'med_pages',None)
with open('pages.json', 'w') as outfile:
                objects_list=[]
                for line in pages.get_next():
                        d = collections.OrderedDict()
                        d['page_id']=line[0]
                        d['title']=line[1]
                        d['summary']=line[2]
                        d['categories']=line[3]
                        d['content']=line[4]
                        objects_list.append(d)
                        j = json.dump(d,outfile,ensure_ascii=True)
                        outfile.write('\n')
