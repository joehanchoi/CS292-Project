from DB import DB, DB_NAME
import wikipedia

def create_table(db):
	con = db.connection()
	cur = db.cursor()

	d = 'drop table if exists med_pages;'
	q = 'create table med_pages(page_id bigint, title varchar, summary varchar, categories varchar, content text);'

	for q in [d, q]:
		cur.execute(q)
	con.commit()

def load_terms():
	pass

def load_wiki():
	test = wikipedia.page("Cancer")
	print test.title

def main():
	db = DB(DB_NAME)
	load_terms()
	load_wiki()

if __name__=='__main__':
	main()
