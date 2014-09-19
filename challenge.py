# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import csv
from SPARQLWrapper import SPARQLWrapper, JSON, XML
import re

book_uris ={}
book_paths={}
	
def read_book_patterns():
	cr = csv.reader(open("book_patterns.csv","rb"),quoting=csv.QUOTE_NONE) #load file containing all paths
	for row in cr:    
		if row[0] not in book_paths.keys():
			book_paths[row[0]]={}
		if row[1] not in book_paths[row[0]].keys():
			book_paths[row[0]][row[1]]=[]
		if row[2] not in book_paths[row[0]][row[1]]:
			book_paths[row[0]][row[1]].append(row[2])
	return book_paths

def get_book_uris():
	with open("DBbook_Items_DBpedia_mapping.tsv","U") as tsv:
		for row in csv.reader(tsv, delimiter="\t"):
			book_uris[row[0]]=row[2]
	return book_uris	

def profile(user):
    df = pd.read_csv("UPs/user_"+str(user)+".csv",header=None)
    s = df.loc[:,[3,4]].values
    s = [[str(s[x][1]),s[x][0]] for x in range(0,len(s))]
    k = dict()
    for x in s:
        if x[0] in k:
            k[x[0]].append(int(x[1]))
        else:
            k[x[0]] = [int(x[1])]
    for x in k:
        k[x] = [sum(k[x]),(len(k[x])-sum(k[x]))]
    return k,set(df.loc[:,[0]].values.flatten())

def get_books_user(user):
    df = pd.read_csv("UPs/user_"+str(user)+".csv",header=None)
    return set(df.loc[:,[0]].values.flatten())


def get_paths(Book1,Book2):
        sparql = SPARQLWrapper("http://dbpedia.org/sparql")
        paths=[]
        Book1 = re.sub("'","%27",Book1)
        Book2 = re.sub("'","%27",Book2)
        query_paths_L1 ="SELECT DISTINCT ?prop WHERE {<"+Book1+"> ?prop <"+Book2+"> .}"
        query_paths_L2_1 ="SELECT DISTINCT ?prop1 ?t2 ?prop2 ?v1 WHERE {<"+Book1+"> ?prop1 ?v1 . <"+Book2+"> ?prop2 ?v1. ?v1 rdf:type ?t2 .}"
        query_paths_L2_2 ="SELECT DISTINCT ?prop1 ?t2 ?prop2 ?v1 WHERE {<"+Book1+"> ?prop1 ?v1 . ?v1 ?prop2 <"+Book2+">. ?v1 rdf:type ?t2 .}"
        
        try:
            sparql.setQuery(query_paths_L1)
            sparql.setReturnFormat(JSON)
            L1 = sparql.query().convert()    
            for row in L1["results"]["bindings"]:
                    res = '"'+row["prop"]["value"] +'"'
                    paths.append(res)
        except:
            pass
        try:
            sparql.setQuery(query_paths_L2_1)
            sparql.setReturnFormat(JSON)
            L2 = sparql.query().convert()

            for row in L2["results"]["bindings"]:
                res = '"'+row["prop1"]["value"] +','
                res += row["t2"]["value"] +','
                res += row["prop2"]["value"] +'"'
                paths.append(res)
        except:
                pass
                
        try:
            sparql.setQuery(query_paths_L2_2)
            sparql.setReturnFormat(JSON)
            L2 = sparql.query().convert()

            for row in L2["results"]["bindings"]:
                    res = '"'+row["prop1"]["value"] +','
                    res += row["t2"]["value"] +','
                    res += row["prop2"]["value"] +'"'
                    paths.append(res)
        except:
            pass
        return paths

def get_pattern_between_books(id1,id2):
        book1 = book_uris[id1]
        book2 = book_uris[id2]
        patterns = get_paths(book1,book2)
        return patterns    

def evaluate():
    out = open('results.csv','w')
    book_paths = read_book_patterns()
    book_uris = get_book_uris()
    df = pd.read_csv("task2_useritem_evaluation_data.tsv",sep='\t')
    user = 0
    pro = dict()
    books_user = set()
    nr = 0
    for row in df.values:
        if(user != row[0]):
            user = row[0]
            try:
                pro, books_user = profile(row[0])
            except:
                pro = dict()
                books_user = set()
        patterns = []
        for bu in books_user:
            try:
                patterns = patterns + book_paths[str(bu)][str(row[1])]
            except:
                bp = get_pattern_between_books(str(bu),str(row[1]))
                if str(bu) not in book_paths.keys():
                       book_paths[str(bu)]={}
                if str(row[1]) not in book_paths[str(bu)].keys():
                       book_paths[str(bu)][str(row[1])]=[]
                book_paths[str(bu)][str(row[1])] = bp
                patterns = patterns + bp
        patterns = [x.strip('"') for x in set(patterns)]
        p = sum([pro[x][0] for x in patterns if x in pro.keys()])
        n = sum([pro[x][1] for x in patterns if x in pro.keys()])
        out.write(str(row[0])+","+str(row[1])+","+str(float(p+1.0)/(p+n+2))+"\n")
        print(str(row[0])+","+str(row[1])+","+str(float(p+1.0)/(p+n+2)))

        nr = nr+1
    out.close()


