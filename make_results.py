import csv


res={}

	
cr = csv.reader(open("results1.csv","rb"))
for row in cr:
	if row[0] not in res.keys():
	 	res[row[0]]={}
	res[row[0]][row[1]]=row[2]

cr = csv.reader(open("results21.csv","rb"))
for row in cr:
	if row[0] not in res.keys():
	 	res[row[0]]={}
	res[row[0]][row[1]]=row[2]
		 
cr = csv.reader(open("results22.csv","rb"))
for row in cr:
	if row[0] not in res.keys():
	 	res[row[0]]={}
	res[row[0]][row[1]]=row[2]
	
cr = csv.reader(open("results3.csv","rb"))
for row in cr:
	if row[0] not in res.keys():
	 	res[row[0]]={}
	res[row[0]][row[1]]=row[2]
	
cr = csv.reader(open("results32.csv","rb"))
for row in cr:
	if row[0] not in res.keys():
	 	res[row[0]]={}
	res[row[0]][row[1]]=row[2]
	
cr = csv.reader(open("results4.csv","rb"))
for row in cr:
	if row[0] not in res.keys():
	 	res[row[0]]={}
	res[row[0]][row[1]]=row[2]

cr = csv.reader(open("results_missing.csv","rb"))
for row in cr:
	if row[0] not in res.keys():
	 	res[row[0]]={}
	res[row[0]][row[1]]=row[2]
#'169': {'5346': '0.491304347826', '2154': '0.491304347826', '7512': '0.5', '7541': '0.491803278689', '7822': '0.5', '2115': '0.5'}}

out = open('complete_results.tsv','w')
with open("TASK2_3_DBbook_train_binary/task2_useritem_evaluation_data.tsv","U") as tsv:
	for row in csv.reader(tsv, delimiter="\t"):
		try:		
			#print row[0] +'\t'+ row[1] +'\t' + res[row[0]][row[1]]+'\n'
			out.write(row[0] +'\t'+ row[1] +'\t' + res[row[0]][row[1]]+'\n')
		except:
			print row[0],row[1]
