import happybase
from DB import DB, DB_NAME, Hbase
import re
class Pages:
	def __init__(self):
		self.postgres_obj = DB(DB_NAME)
		self.con = self.postgres_obj.connection()
		self.cur = self.postgres_obj.cursor()
		self.hbase = Hbase(DB_NAME)
		self.create_similarity_hbase_table()
	
	def create_similarity_hbase_table(self):
		self.hbase_connection = self.hbase.get_connection()
		try:
			self.hbase_connection.create_table("pages_similarity", {'paired_page':dict()})
		except happybase.hbase.ttypes.AlreadyExists:
			pass
		
	def readCSV(self, file_name):
		file = open(file_name, 'r')
		file_list = []
		for r in file:
			file_list.extend(r.split(','))
		return file_list

	def loadPages(self):
		sql = "select page_id, summary from med_pages"
		self.pages_row = self.postgres_obj.query(sql)
		self.pages_rows_list = list()
		for row in self.pages_row:
			self.pages_rows_list .append([row[0], row[1]])
		
	def pagesJaccSimilarity(self):
		set_stop_words = set(self.readCSV('stop_words.txt'))
		sim_pages = self.hbase.table("pages_similarity")
		for i in range(len(self.pages_row)):
			for j in range(i+1, len(self.pages_row)):
				page1_summary = set(re.findall(r"[\w']+", self.pages_row[i][1].lower())).difference(set_stop_words)
				page2_summary = set(re.findall(r"[\w']+", self.pages_row[j][1].lower())).difference(set_stop_words)	
				jacc_similarity = (1.0 * len(page2_summary.intersection(page1_summary))) / len(page2_summary.union(page1_summary))
				sim_pages.put(str(self.pages_row[i][0]), {'paired_page:'+str(self.pages_row[j][0]):str(jacc_similarity)})
				sim_pages.put(str(self.pages_row[j][0]), {'paired_page:'+str(self.pages_row[i][0]):str(jacc_similarity)})
def main():
	load_page = Pages()
	load_page.loadPages()
	load_page.pagesJaccSimilarity()

if __name__ == '__main__':
	main()
