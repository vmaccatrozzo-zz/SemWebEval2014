from SPARQLWrapper import SPARQLWrapper, JSON, XML
import re
import csv
import os
import pandas as pd

#book_uris=[]

def get_book_uris():
	book_uris ={}
	with open("/Users/dceolin/Dropbox/ESWC Challenge/DBbook_Items_DBpedia_mapping.tsv","U") as tsv:
		for row in csv.reader(tsv, delimiter="\t"):
			book_uris[row[0]]=row[2]
	return book_uris	

def get_all_patterns():
	out = open('/Users/dceolin/Dropbox/ESWC Challenge/book_patterns.csv','a')
	book_paths=read_book_patterns()
	book_uris=get_book_uris()
	for id1 in book_uris.keys():
		for id2 in book_uris.keys():
			if id1 != id2:	
				try:
					book_paths[id1][id2]
				except:
					get_paths(book_uris[id1],book_uris[id2],id1,id2,out)
	out.close()
		
def read_book_patterns():
	book_paths={}
	cr = csv.reader(open("/Users/dceolin/Dropbox/ESWC Challenge/book_patterns.csv","rb"))
	for row in cr:    
		if row[0] not in book_paths.keys():
			book_paths[row[0]]={}
		if row[1] not in book_paths[row[0]].keys():
			book_paths[row[0]][row[1]]=[]
		#print row[2]
		book_paths[row[0]][row[1]].append(row[2])
	return book_paths

def profile(user):
	#print user
	df = pd.read_csv(user,header=None)
	s = df.loc[:,[0,1,4]].values
	s = [[str(s[x][2]),s[x][0],s[x][1]] for x in range(0,len(s))]
	return s
 
def get_patterns_from_profiles():    
	missings_profiles=[]
	book_paths={}
	files = os.listdir("UPs_binary/")
	for file in files:	
		if file[0] != '.':
			path="UPs_binary/"+file
			try:
				patterns = profile(path)
				for row in patterns:
					if row[1] not in book_paths.keys():
						book_paths[row[1]]={}
					if row[2] not in book_paths[row[1]].keys():
						book_paths[row[1]][row[2]]=[]
					if row[0] not in book_paths[row[1]][row[2]]:
						book_paths[row[1]][row[2]].append(row[0])
			except:
				continue


	c = csv.writer(open("/Users/dceolin/Dropbox/ESWC Challenge/binary_book_patterns.csv", "wb"))
	for key in book_paths.keys():
		for value in book_paths[key]:
			for i in book_paths[key][value]:
				c.writerow([key, value,i])

def get_paths(Book1,Book2,id1,id2,out):
	sparql = SPARQLWrapper("http://eculture2.cs.vu.nl:6543/sparql/")
	paths=[]
	Book1 = re.sub("'","%27",Book1)
	Book2 = re.sub("'","%27",Book2)
	query_paths_L1 ="SELECT DISTINCT ?prop WHERE {<"+Book1+"> ?prop <"+Book2+"> .}"
	query_paths_L2_1 ="SELECT DISTINCT ?prop1 ?t2 ?prop2 ?v1 WHERE {<"+Book1+"> ?prop1 ?v1 . <"+Book2+"> ?prop2 ?v1. ?v1 rdf:type ?t2 .}"
	query_paths_L2_2 ="SELECT DISTINCT ?prop1 ?t2 ?prop2 ?v1 WHERE {<"+Book1+"> ?prop1 ?v1 . ?v1 ?prop2 <"+Book2+">. ?v1 rdf:type ?t2 .}"
	query_paths_L3_1 = "SELECT distinct ?prop1 ?t2 ?prop2 ?t3 ?prop3WHERE {<"+Book1+"> ?prop1 ?v1 .  ?v1 ?prop2 ?v2 .  ?v2 ?prop3 <"+Book2+"> .?v1 rdf:type ?t2 . ?v2 rdf:type ?t3 .}"

	#print 'L1'
	#print query_paths_L1
	sparql.setQuery(query_paths_L1)
	sparql.setReturnFormat(JSON)
	L1 = sparql.query().convert()
	
	for row in L1["results"]["bindings"]:
		res1 = '"'+id1+'","'+id2+'",'
		res = '"'+row["prop"]["value"] +'"\n'	
		#if id1 not in book_paths.keys():
		#	book_paths[id1]={}
		#if id2 not in book_paths[id1].keys():
		#	book_paths[id1][id2]=[]
		#book_paths[id1][id2].append(res)
		paths.append(res1+res)
		#out.write(res)
	try:
		sparql.setQuery(query_paths_L2_1)
		sparql.setReturnFormat(JSON)
		L2 = sparql.query().convert()
	
		for row in L2["results"]["bindings"]:
			res1 = '"'+id1+'","'+id2+'",'
			res = '"'+row["prop1"]["value"] +','	
			res += row["t2"]["value"] +','
			res += row["prop2"]["value"] +'"\n'
			#out.write(res)
			#if id1 not in book_paths.keys():
			#	book_paths[id1]={}
			#if id2 not in book_paths[id1].keys():
			#	book_paths[id1][id2]=[]
			#book_paths[id1][id2].append(res)
			paths.append(res1+res)
	except:
		print 'L2_1 wrong'
	
	sparql.setQuery(query_paths_L2_2)
	sparql.setReturnFormat(JSON)
	L2 = sparql.query().convert()
	
	for row in L2["results"]["bindings"]:
		res1 = '"'+id1+'","'+id2+'",'
		res = '"'+row["prop1"]["value"] +','	
		res += row["t2"]["value"] +','
		res += row["prop2"]["value"] +'"\n'
		#out.write(res)
		
		#if id1 not in book_paths.keys():
		#	book_paths[id1]={}
		#if id2 not in book_paths[id1].keys():
	#		book_paths[id1][id2]=[]
	#	book_paths[id1][id2].append(res)
		paths.append(res1+res)
	return paths

def get_pattern_between_books(id1,id2):
	book1 = book_uris[id1]
	book2 = book_uris[id2]
	patterns = get_paths(book1,book2,id1,id2)
	return patterns


#get_all_patterns()
#get_patterns_from_profiles()