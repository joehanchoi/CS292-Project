import networkx as nx
import numpy as np
from DB import DB, DB_NAME, Hbase
from page_similarity import Pages
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import sys

def main():
	#default term
	test_term = 'embolism'
	if len(sys.argv) > 1:
		test_term = sys.argv[1]
	print 'Generating graph for', test_term
	graph(test_term)

def add_x_nodes(pages, G, id, i):
	nodes = []
	title = pages.get_title(int(id))
	sim_pages = pages.query_page(id)
	short_sim_pages = sorted(sim_pages, key=lambda x: x[1], reverse=True)[1:i+1]
	for page in short_sim_pages:
		page_name = pages.get_title(int(page[0]))
		G.add_node(page_name)
		nodes.append(page_name)
		G.add_edge(title,page_name)
	return nodes

def graph(term):
	pages = Pages()
	id = pages.get_id(term)
	sim_pages = pages.query_page(id)

	G = nx.Graph()
	node_dict = {}
	title = pages.get_title(int(id))
	G.add_node(title)
	node_dict[title] = ('red', 0.0)

	short_sim_pages = sorted(sim_pages, key=lambda x: x[1], reverse=True)[1:11]
	for page in short_sim_pages:
		page_name = pages.get_title(int(page[0]))
		G.add_node(page_name)
		node_dict[page_name] = ('blue', float(page[1]))
		G.add_edge(title,page_name)
		addl_nodes = add_x_nodes(pages, G, page[0], 10)
		for node in addl_nodes:
			if node not in node_dict:
				node_dict[node] = ('green', float(page[1]))

	node_list = []
	node_color = []
	node_size = []
	for node in node_dict:
		node_list.append(node)
		node_color.append(node_dict[node][0])
		node_size.append(node_dict[node][1])
	node_size = [x*300/max(node_size) for x in node_size]
	node_size[node_size.index(0.0)]=300

	nx.draw_networkx(G=G,with_labels=True,nodelist=node_list,node_size=node_size,node_color=node_color,font_size=8)
	fig_name = "%s.png" %(term)
	plt.savefig(fig_name)

if __name__=='__main__':
	main()
