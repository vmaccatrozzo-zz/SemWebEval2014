from SPARQLWrapper import SPARQLWrapper, JSON, XML
import re
import csv
import urllib

sparql = SPARQLWrapper("http://dbpedia.org/sparql")

def get_paths(Book1,Book2,User,Rating,out):
	Book1 = re.sub("'","%27",Book1)
	Book2 = re.sub("'","%27",Book2)
	
	query_paths_L1 ="SELECT DISTINCT ?prop WHERE {<"+Book1+"> ?prop <"+Book2+"> .}"
	query_paths_L2_1 ="SELECT DISTINCT ?prop1 ?t2 ?prop2 WHERE {<"+Book1+"> ?prop1 ?v1 . <"+Book2+"> ?prop2 ?v1. ?v1 rdf:type ?t2 .}"
	query_paths_L2_2 ="SELECT DISTINCT ?prop1 ?t2 ?prop2 WHERE {<"+Book1+"> ?prop1 ?v1 . ?v1 ?prop2 <"+Book2+">. ?v1 rdf:type ?t2 .}"
	
	
	sparql.setQuery(query_paths_L1)
	sparql.setReturnFormat(JSON)
	L1 = sparql.query().convert()
	
	for row in L1["results"]["bindings"]:
		res = Book1 +',' + Book2+',' +User+',' +Rating+',' 
		res += row["prop"]["value"] +'\n'	
		out.write(res)
	
	try:
		sparql.setQuery(query_paths_L2_1)
		sparql.setReturnFormat(JSON)
		L2 = sparql.query().convert()
	
		for row in L2["results"]["bindings"]:
			res = Book1 +',' + Book2+',' +User+',' +Rating+',' 
			res += row["prop1"]["value"] +','	
			res += row["t2"]["value"] +','
			res += row["prop2"]["value"] +'\n'
			out.write(res)
	except:
		pass

	sparql.setQuery(query_paths_L2_2)
	sparql.setReturnFormat(JSON)
	L2 = sparql.query().convert()
	
	for row in L2["results"]["bindings"]:
		res = Book1 +',' + Book2+',' +User+',' +Rating+',' 
		res += row["prop1"]["value"] +','	
		res += row["t2"]["value"] +','
		res += row["prop2"]["value"] +'\n'
		out.write(res)
	

def read_tsv():
	ratings ={}
	with open("DBbook_train_ratings.tsv","U") as tsv:
		for row in csv.reader(tsv, delimiter="\t"):	
			if row[0] not in ratings.keys():
				ratings[row[0]]={}
			ratings[row[0]][row[1]]=row[2]
		
	return ratings	

def get_book_uris():
	book_uris ={}
	with open("DBbook_Items_DBpedia_mapping.tsv","U") as tsv:
		for row in csv.reader(tsv, delimiter="\t"):
			book_uris[row[0]]=row[2]
	return book_uris	
	
def get_user_profile():
	book_uris = get_book_uris()
	ratings = read_tsv()
	for user in ratings.keys():
		file = "UPs/user_"+ user +'.csv'
		out = open(file,"w")
		for book1 in ratings[user].keys():
			for book2 in ratings[user].keys():
				if book1 != book2:
					get_paths(book_uris[book1],book_uris[book2],user,ratings[user][book2],out)
		out.close()
