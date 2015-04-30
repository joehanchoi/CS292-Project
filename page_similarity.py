import happybase
from DB import DB, DB_NAME, Hbase
import re
class Pages:
	def __init__(self):
		#initiate all databases and tables
		self.postgres_obj = DB(DB_NAME)
		self.con = self.postgres_obj.connection()
		self.cur = self.postgres_obj.cursor()
		self.hbase = Hbase(DB_NAME)
		self.create_similarity_hbase_table()
		self.table = self.hbase.table("spark_pages_similarity_umls_subset")
	
	def create_similarity_hbase_table(self):
		#create connection to hbase table
		self.hbase_connection = self.hbase.get_connection()
		try:
			self.hbase_connection.create_table("spark_pages_similarity_umls_subset", {'paired_page':dict()})
		except happybase.hbase.ttypes.AlreadyExists:
			pass
		
	def readCSV(self, file_name):
		#read CSV file
		file = open(file_name, 'r')
		file_list = []
		for r in file:
			file_list.extend(r.split(','))
		return file_list

	def loadPages(self):
		#function to load the data from sql
		sql = "select page_id, summary from med_pages_umls_subset"
		self.pages_row = self.postgres_obj.query(sql)
		self.pages_rows_list = list()
		#keep the page id and summary
		for row in self.pages_row:
			self.pages_rows_list .append([row[0], row[1]])
		
	def pagesJaccSimilarity(self):
		#original unparallelized function for pilot project to calculate the Jaccard similarity between every page
		set_stop_words = set(self.readCSV('stop_words.txt'))
		sim_pages = self.hbase.table("pages_similarity")
		for i in range(len(self.pages_row)):
			for j in range(i+1, len(self.pages_row)):
				#treat summary as a set of words with all stop words removed
				page1_summary = set(re.findall(r"[\w']+", self.pages_row[i][1].lower())).difference(set_stop_words)
				page2_summary = set(re.findall(r"[\w']+", self.pages_row[j][1].lower())).difference(set_stop_words)	
				jacc_similarity = (1.0 * len(page2_summary.intersection(page1_summary))) / len(page2_summary.union(page1_summary))
				#load pages into hbase
				sim_pages.put(str(self.pages_row[i][0]), {'paired_page:'+str(self.pages_row[j][0]):str(jacc_similarity)})
				sim_pages.put(str(self.pages_row[j][0]), {'paired_page:'+str(self.pages_row[i][0]):str(jacc_similarity)})

	def get_id(self,term):
		#get id of page with title
		q = "SELECT distinct(page_id) FROM med_pages_umls_subset WHERE lower(title) LIKE '%%%s%%'"%(term.lower())
		return self.postgres_obj.query(q)[0][0]

	def get_title(self,id):
		#get title of page with id
		q = "SELECT distinct(title) FROM med_pages_umls_subset WHERE page_id = %d"%(id)
		return self.postgres_obj.query(q)[0][0].lower()

	def query_page(self,id):
		#for a page with id, get all pages and their similarities to page
		result = []
		row = self.table.row(str(id))
		for page in row:
			result.append((page[12:],row[page]))
		return result

	def getPageLinksList(self, page_id):
		#get links from link table for a page
		q ="SELECT links FROM med_pages_umls_subset_links WHERE page_id = %d" % (page_id)
		links = self.postgres_obj.query(q)
		if len(list(links)) > 0:
			return [link.lower() for link in links[0][0].split(',')]
		else:
			return []

	def getPageSummaryContent(self, page_id):
		#get summary and content from table for a page
		q = "SELECT summary, content FROM med_pages_umls_subset WHERE page_id = %d" % (page_id)
		page_info = self.postgres_obj.query(q)
		if len(list(page_info)) > 0:
			return page_info[0][0], page_info[0][1]
		else:
			return []

def main():
	load_page = Pages()
	load_page.loadPages()
	load_page.pagesJaccSimilarity()

if __name__ == '__main__':
	main()
