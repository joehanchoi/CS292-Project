from __future__ import print_function

import re
import sys
from operator import add, itemgetter
import numpy as np
from DB import DB, DB_NAME, Hbase
from page_similarity import Pages

from pyspark import SparkContext


def computeContribsAssociation(page, rank):
    """Calculates similarity ranks contributions to the similarity rank of other pages."""
    for values in page:
    	value = values.split(',')
    	yield (value[0], float(value[1])*rank)

def parseAssociationMatrix(association_line):
	# parsing the page-page similarity entry
	# input: page1_id,page2_id,page1-page2-similarityrank
	page1, page2, rank = re.split(r',', association_line)
	# formatiing the RDD entry (key value) as (page1_id page2_id,age1-page2-similarityrank)
	return page1, page2+','+str( rank)

# get the top10 similar pages
def getTopSimilarPagesByID(page_id):
	pages = Pages()
	sim_pages = pages.query_page(page_id)
	top_similar_pages = sorted(sim_pages, key=lambda x: x[1], reverse=True)[1:11]
	return top_similar_pages

def readPagesByTerm(term):
	pages = Pages()
	# getting the top similar pages for the searched term
	main_term_id = str(pages.get_id(term))
	sim_pages = pages.query_page(main_term_id)
	top_similar_pages = sorted(sim_pages, key=lambda x: x[1], reverse=True)[1:21]
	possible_page_ids = set()
	possible_page_ids.add(main_term_id)
	value_of_list_item = main_term_id +',' + main_term_id + ',0.0'
	# add the possible page to the set
	page_page_set = set()
	page_page_set.add(value_of_list_item)
	# Save the page-page similarity values in dict
	pages_pages_association_dict = dict()
	pages_pages_association_dict[main_term_id] = dict()
	for page_vals in sim_pages:
		pages_pages_association_dict[main_term_id][str(page_vals[0])] = page_vals[1]
		pages_pages_association_dict[page_vals[0]] = {	str(page_vals[0]): page_vals[1]}
	print ("main term", main_term_id)
	# for the pages that are similar to the search term(first layer similar pages), get the top similar page 
	for page in top_similar_pages:
		possible_page_ids.add(str(page[0]))
		if str(page[0]) not in pages_pages_association_dict:
			pages_pages_association_dict[str(page[0])] = dict()
		second_layer_pages = pages.query_page(page[0])
		second_layer_pages_top = sorted(second_layer_pages, key=lambda x: x[1], reverse=True)[1:21]
		for associated_page in second_layer_pages_top:
			possible_page_ids.add(str(associated_page[0]))
		for associated_page in second_layer_pages:
			if str(associated_page[0]) not in pages_pages_association_dict:
				pages_pages_association_dict[str(associated_page[0])] = dict()
			pages_pages_association_dict[str(page[0])][str(associated_page[0])] = associated_page[1]
			pages_pages_association_dict[str(associated_page[0])][str(page[0])] = associated_page[1]
	# form all the page-page similarity values	
	for page1 in possible_page_ids:
		for page2 in possible_page_ids:
			if page1 == page2:
				# if page1 and page2 doesn't have any similarty value, the similarity values is assigned to 0
				value = page1 + ',' + page2 +',' + '0.0'
				page_page_set.add(value)
			else:
				if page1  in pages_pages_association_dict and page2 in pages_pages_association_dict[page1]:
					value = str(page1) + ',' + str(page2) +',' + str(pages_pages_association_dict[str(page1)][str(page2)])
				else:
					value = str(page1) + ',' + str(page2) +',0.0'
				page_page_set.add(value)
				if page2 in pages_pages_association_dict and page1 in pages_pages_association_dict[page2]:
					value = str(page2) + ',' + str(page1) + ','+ str(pages_pages_association_dict[str(page2)][str(page1)])
				else:
					value = str(page2) + ',' + str(page1) + ',0.0'
				page_page_set.add(value)
	# printing out the pages in similarity matrx
	print (possible_page_ids)
	print (len(pages_pages_association_dict))
	print (pages_pages_association_dict.keys())
	# Return the page-page similarity matrx
	return list( page_page_set)

def getDifferenceWikiPageRankLists(search_term, rank_list):
	pages = Pages()
	page_id = pages.get_id(search_term)
	wiki_page_links = set(pages.getPageLinksList(int(page_id)))
	rank_name_list = [pages.get_title(int(ranked_page[0])).lower() for ranked_page in rank_list]
	rank_set = set(rank_name_list)
	# get the links in both ranked list and wikipedea links
	print ("Intersection", rank_set.intersection(wiki_page_links))
	# get the links that exist only in ranked list
	print ("Difference", rank_set.difference(wiki_page_links))
	page_summary, page_content = pages.getPageSummaryContent(page_id)
	# get ranked pages that exist in summary and content of page
	links_in_summary_only = set()
	links_in_content_only = set()
	links_in_both = set()
	for rank_page in rank_name_list:
		in_summary = False
		in_content = False
		if re.search(r'\b%s\b' % rank_page, page_summary):
			in_summary = True
		if re.search(r'\b%s\b' % rank_page, page_content):
			in_content = True
		if in_summary and in_content:
			links_in_both.add(rank_page)
		elif in_summary:
			links_in_summary_only.add(rank_page)
		elif in_content:
			links_in_content_only.add(rank_page)
	# print out the summary for the comparison between the wikilinks, content and the ranked list
	print ("inks_in_both", links_in_both)
	print ("links_in_summary_only", links_in_summary_only)
	print ("links_in_content_only", links_in_content_only)
	PAGES_IN_WIKI_LINKS_AND_CONTENT = links_in_both.union(rank_set.intersection(wiki_page_links))
	print ("pages in both links and the content", PAGES_IN_WIKI_LINKS_AND_CONTENT)
	top_ranked_50 = set(rank_name_list[0:50])
	print ("print intersection with top ranks 50 and wiki", top_ranked_50.intersection(PAGES_IN_WIKI_LINKS_AND_CONTENT))
	print ("pages in top 50 and not in common set", top_ranked_50.difference(PAGES_IN_WIKI_LINKS_AND_CONTENT))


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: pagerank <file> <iterations>", file=sys.stderr)
        exit(-1)

    # Initialize the spark context.
    logFile = "/home/suliemlm/spark/README.md"  # Should be some file on your system
    sc = SparkContext("local", appName="PythonPageRank")
    search_term = sys.argv[1] # getting the searh term from the argumment variable
    # get the two layers of similar pages for the given term
    pages_read = readPagesByTerm(search_term) 
    lines = sc.parallelize(pages_read)
    # For the page-page similarity matrix, create the key-value pairs for RDD object
    # caching the RDD object because it will be used in the entire process
    links = lines.map(lambda urls: parseAssociationMatrix(urls)).distinct().groupByKey().cache()
    pages_num =  links.count() # get the number of the pages
    print ("links.count()", pages_num)
    # initialize all the pages rank to same uniform initial value 
    ranks = links.map(lambda url_neighbors: (url_neighbors[0], 1.0/pages_num))

    # Calculates and updates pages similarity ranks continuously using PageRank algorithm.
    for iteration in range(int(sys.argv[2])):
        # Calculates URL similarity contributions to the rank of other ranges.
        contribs = links.join(ranks).flatMap(
            lambda url_urls_rank: computeContribsAssociation(url_urls_rank[1][0], url_urls_rank[1][1]))

        # Re-calculates page similarity ranks based on neighbor contributions.
        ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15/pages_num)
	# loop until it converge or it reaches the maximum number of iteration 
	if iteration > 0:
		print ("ieteration", iteration)
		converged_count = 0
		converge_norm = 0
		for (page_id, rank_value) in ranks.collect():
			if rank_value - prev_rank.lookup(page_id)[0] < 0.001:
				converged_count += 1
			converge_norm += (rank_value - prev_rank.lookup(page_id)[0])
		print ("converge.count()", converged_count)
		# If it converges, break
		if converge_norm <= 0.001:
			print ("Converged after ", iteration)
			break
	# store the current ranks to compare tem to the ranks of the next iterations		
	prev_rank = ranks
    # sort the pages based on the ranks similarity
    sorted_ranks = sorted(ranks.collect(), key = itemgetter(1), reverse=True)
    # Output the similarity ranks
    load_page = Pages()
    max_rank = sorted_ranks[0][1]
    for (link, rank) in sorted_ranks:
        print("%s,%s" % (load_page.get_title(int(link)), rank/max_rank))
    # get the difference between wikipedia links and similarty ranking list, and check if the ranked links exist in the content
    getDifferenceWikiPageRankLists(search_term, sorted_ranks)
    sc.stop()
