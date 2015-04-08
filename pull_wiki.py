from DB import DB, DB_NAME
import wikipedia
import re

def create_table(db):
	con = db.connection()
	cur = db.cursor()

	d = 'drop table if exists med_pages;'
	q = 'create table med_pages(page_id bigint, title varchar, summary varchar, categories varchar, content text);'

	for q in [d, q]:
		cur.execute(q)
	con.commit()

def load_wiki(db):
	med_terms = []
	with open('snomed_cleaned_term.txt','rb') as f:
		text = f.readlines()
	for line in text:
		med_terms.extend(line.split(','))

	con = db.connection()
	cur = db.cursor()
	missed = 0
	i = 0

	for term in med_terms:
		try:
			page = wikipedia.page(term)
		except wikipedia.exceptions.DisambiguationError as e:
			e.options = [t.encode('utf-8') for t in e.options]
			possible = [x for x in e.options if re.search('medic', x.lower())]
			if possible:
				try:
					page = wikipedia.page(possible[0])
				except:
					missed += 1
					continue
			else:
				try:
					page = wikipedia.page(e.options[0])
				except:
					missed += 1
					continue
			#print page.title
		except wikipedia.exceptions.PageError:
			missed += 1
			continue

		try:
			categories = ",".join(page.categories)
		except:
			categories = ""

		cur.execute("insert into med_pages VALUES (%s, %s, %s, %s, %s)",(int(page.pageid),page.title,page.summary,categories,page.content))
		i += 1
	con.commit()
	print '# unidentifiable pages:', missed
	print 'Inserted:', i

def main():
	db = DB(DB_NAME)
	create_table(db)
	load_wiki(db)

if __name__=='__main__':
	main()
