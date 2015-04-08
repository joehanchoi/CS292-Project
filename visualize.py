import networkx as nx
import numpy as np
from DB import DB, DB_NAME, Hbase
from page_similarity import Pages
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

def main():
	test_term = 'allergy'
	graph(test_term)

def add_x_nodes(pages, G, id, i):
	title = pages.get_title(int(id))
	sim_pages = pages.query_page(id)
	short_sim_pages = sorted(sim_pages, key=lambda x: x[1], reverse=True)[1:i]
	for page in short_sim_pages:
		page_name = pages.get_title(int(page[0]))
		G.add_node(page_name)
		G.add_edge(title,page_name)

def graph(term):
	pages = Pages()
	id = pages.get_id(term)
	sim_pages = pages.query_page(id)

	G = nx.Graph()
	title = pages.get_title(int(id))
	G.add_node(title)

	short_sim_pages = sorted(sim_pages, key=lambda x: x[1], reverse=True)[1:11]
	for page in short_sim_pages:
		page_name = pages.get_title(int(page[0]))
		G.add_node(page_name)
		G.add_edge(title,page_name)
		add_x_nodes(pages, G, page[0], 10)

	nx.draw(G)
	plt.savefig("test.png")

if __name__=='__main__':
	main()
