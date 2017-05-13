from flask import Flask
from flask import render_template
from flask import request
import json
from bson import json_util
from pymongo import MongoClient
from bson.json_util import dumps
import random
import numpy as np
import ast
import pandas as pd
from sklearn import preprocessing
from sklearn.cluster import KMeans
from sklearn.metrics.pairwise import pairwise_distances
import matplotlib.pylab as plt
from sklearn.preprocessing import StandardScaler
from scipy.spatial.distance import cdist
from scipy.spatial.distance import pdist,squareform
import collections
from collections import defaultdict
from sklearn.decomposition import PCA
from sklearn.manifold import MDS
from sklearn.metrics.pairwise import pairwise_distances 
import math
import prince

app = Flask(__name__)

MONGODB_HOST = 'localhost'
MONGODB_PORT = 27017
DBS_NAME = 'austinCrimeStats'
COLLECTION_NAME = 'projects'
FIELDS ={'Council District': True , 'GO Highest Offense Desc' : True,
		'Highest NIBRS/UCR Offense Description' : True , 'GO Report Date' : True ,
		'Report Day' : True,'GO Location' : True,'Clearance Status' : True,
		'Clearance Date' : True , 'Clearance Day' : True , 'GO District' : True ,
		'GO Location Zip' : True, 'GO Census Tract' : True , 'GO X Coordinate' : True ,
		'GO Y Coordinate' : True , '_id': False}
	

@app.route("/")
def index():
	return render_template("index.html")

@app.route("/data")
def get_data(council_arg=None, crime=None, status=None, ret_json=True):
	if (council_arg == None):
		council_arg = request.args.getlist('council')
	
	if (crime == None):
		crime = request.args.getlist('crime')

	if (status == None):
		status = request.args.getlist('status')

	council = []
	for entry in council_arg:
		council.append(int(entry))

	query = [{"$match" : {"Council District" : { "$in" : council },
		"Highest NIBRS/UCR Offense Description" : { "$in" : crime} } },
		{"$group" : {"_id" : {"Council District" : "$Council District"}, "total" : {"$sum" : 1}}}]

	if len(status) < 3:
		if status[0] == "0":
			query[0]["$match"].update({"Clearance Status": { "$in" : [0] }})
		else:
			query[0]["$match"].update({"Clearance Status": { "$nin" : [0] }})
	
	connection = MongoClient(MONGODB_HOST, MONGODB_PORT)
	collection = connection[DBS_NAME][COLLECTION_NAME]
	council_list = collection.aggregate(pipeline=query)

	crime_dataSet = []

	for i in range(0, 10):
		crime_dataSet.append(0)
	
	total_crimes = 0

	for row in council_list:
		crime_dataSet[row["_id"]["Council District"] - 1] = row["total"]
		total_crimes += row["total"]
		
	crime_dataSet.append(total_crimes)

	connection.close()

	if (ret_json):
		return json.dumps(crime_dataSet)
	else:
		return crime_dataSet

@app.route("/MCA")
def SampleAndMCA():

	FIELDS ={'Council District': True , 'Report Day' : True ,'Clearance Day' : True,
			'Highest NIBRS/UCR Offense Description' : True , 
			'GO Location Zip' : True, '_id': False, "Clearance Status" : True}
	
	connection = MongoClient(MONGODB_HOST, MONGODB_PORT)
	collection = connection[DBS_NAME][COLLECTION_NAME]
	projects = collection.find(projection=FIELDS)

	sample = []
	cnt = 0
	limit = 10000
	for project in projects:
		if (cnt < limit):
			sample.append(project)
		else:
			idx = random.randint(0,cnt+1)
			if (idx < limit):
				sample[idx] = project
		cnt = cnt + 1

	df = pd.DataFrame(sample)

	mca = prince.MCA(df, n_components=2)

	fig1, ax1 = mca.plot_cumulative_inertia()
	fig3, ax3 = mca.plot_rows_columns()
	fig4, ax4 = mca.plot_relationship_square()

	plt.show();

	return json.dumps("Sampling and Multiple Component Analysis done")

@app.route("/cases_solved_count") 
def classify_crimes():
	council_list = request.args.getlist('council')
	crime_list =  request.args.getlist('crime')
	status = request.args.getlist('status')

	crime_clearance_lookup= list()

	crime = []
	for entry in crime_list:
		crime.append(entry)


	council = []
	for entry in council_list:
		council.append(int(entry))

	query = [{"$match" : {"Council District" : { "$in" : council },
	"Highest NIBRS/UCR Offense Description" : { "$in" : crime }  } },
		{"$group" :
	{"_id" : {
	"Highest NIBRS/UCR Offense Description" : "$Highest NIBRS/UCR Offense Description",
	"Clearance Status": "$Clearance Status", "Council District" : "$Council District"}, 
	"total" : {"$sum" : 1}}}]

	if len(status) ==0:
		return  json.dumps(crime_clearance_lookup)

	if len(status) < 3:
		if status[0] == "0":
			query[0]["$match"].update({"Clearance Status": { "$in" : [0] }})
		else:
			query[0]["$match"].update({"Clearance Status": { "$nin" : [0] }})

	connection = MongoClient(MONGODB_HOST, MONGODB_PORT)
	collection = connection[DBS_NAME][COLLECTION_NAME]
	project = collection.aggregate(pipeline=query)

	result = []
	
	for row in project:
		entry = dict()
		for key in row:
			if key != "_id":
				entry[key] = row[key]
			else:
				for keys in row[key]:
					entry[keys] = row[key][keys]
			
		result.append(entry)

	crime_Object=dict();
	

	for entry in result:

		offence= entry["Highest NIBRS/UCR Offense Description"]

		for crime in crime_clearance_lookup: 
			if "crimeType" in crime and crime["crimeType"] == offence:
				crime_Object = crime;
				break;
		else:
			crime_clearance_lookup.append(dict())
			crime_Object= crime_clearance_lookup[len(crime_clearance_lookup)-1]
			crime_Object["crimeType"] = offence

		if "unsolved" not in crime_Object:
			crime_Object["unsolved"] = 0

		if "solved" not in crime_Object:  
				crime_Object["solved"] = 0

		if entry["Clearance Status"] == 0:
			crime_Object["unsolved"] += entry["total"];
		else: 
			crime_Object["solved"] += entry["total"];
		

	return 	json.dumps(crime_clearance_lookup)

@app.route("/line_chart_data")
def line_chart_data():
	council_list = request.args.getlist('council')
	crime_list =  request.args.getlist('crime')
	status = request.args.getlist('status')

	crime = []
	for entry in crime_list:
		crime.append(entry)

	council = []
	for entry in council_list:
		council.append(int(entry))

	query = [{"$match" : {"Council District" : { "$in" : council },
	"Highest NIBRS/UCR Offense Description" : { "$in" : crime }  } },
		{"$group" :
	{"_id" : {
	"Highest NIBRS/UCR Offense Description" : "$Highest NIBRS/UCR Offense Description",
	"Clearance Status": "$Clearance Status","Council District" : "$Council District",
	"month":{"$substr":["$GO Report Date",3,3]}}, 
	"total" : {"$sum" : 1}}}]

	if len(status) < 3:
		if status[0] == "0":
			query[0]["$match"].update({"Clearance Status": { "$in" : [0] }})
		else:
			query[0]["$match"].update({"Clearance Status": { "$nin" : [0] }})


	connection = MongoClient(MONGODB_HOST, MONGODB_PORT)
	collection = connection[DBS_NAME][COLLECTION_NAME]
	project = collection.aggregate(pipeline=query)

	result = []

	for row in project:
		entry = dict()
		for key in row:
			if key != "_id":
				entry[key] = row[key]
			else:
				for keys in row[key]:
					entry[keys] = row[key][keys]
			
		result.append(entry)

	crime_Object=dict();
	crime_clearance_lookup= list()

	if(len(result)==0):
		return json.dumps(crime_clearance_lookup)


	months=["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]

	for i in range(12):
		crime_clearance_lookup.append(dict())
		crime_clearance_lookup[i]["month"] = months[i]
		for crimeType in crime:
			crime_clearance_lookup[i][crimeType] = 0

		

	for entry in result:

		offence= entry["Highest NIBRS/UCR Offense Description"]

		for crime in crime_clearance_lookup: 
			if "month" in crime and crime["month"] == entry["month"]:
				crime_Object = crime;
				break;

		if 	offence in crime_Object:
			crime_Object[offence] += entry["total"]
		else:
			crime_Object[offence] = entry["total"]	
	
	connection.close()

	return  json.dumps(crime_clearance_lookup)
			
@app.route("/bubble_data")
def get_crimes_location():
	council_arg = request.args.getlist('council')
	crime = request.args.getlist('crime')
	status = request.args.getlist('status')

	council = []
	for entry in council_arg:
		council.append(int(entry))

	query = [{"$match" : {"Council District" : { "$in" : council },
		"Highest NIBRS/UCR Offense Description" : { "$in" : crime} } },
		{"$group" : {"_id" : {"Council District" : "$Council District",
		"GO Location Zip" : "$GO Location Zip"},
		"total" : {"$sum" : 1}}}]

	if len(status) < 3:
		if status[0] == "0":
			query[0]["$match"].update({"Clearance Status": { "$in" : [0] }})
		else:
			query[0]["$match"].update({"Clearance Status": { "$nin" : [0] }})

	
	connection = MongoClient(MONGODB_HOST, MONGODB_PORT)
	collection = connection[DBS_NAME][COLLECTION_NAME]
	projects = collection.aggregate(pipeline=query)

	result = []
	for row in projects:
		entry = dict()
		for key in row:
			if key != "_id":
				entry[key] = row[key]
			else:
				for keys in row[key]:
					entry[keys] = row[key][keys]
			
		result.append(entry)

	connection.close()
	return json.dumps(result)

@app.route("/cpack_data")
def get_solve_crime():
	council_arg = request.args.getlist('council')
	crime = request.args.getlist('crime')

	council = []
	solveRange = ["<=30 days", ">30 days and <=90 days", ">90 days and <=180 days",
		">180 days and <=365 days"]
	
	for entry in council_arg:
		council.append(int(entry))

	query = [{"$match" : {"Council District" : { "$in" : council },
		"Clearance Status" : { "$nin" : [0] } } },
		{"$group" : {"_id" : {"Council District" : "$Council District",
		"Highest NIBRS/UCR Offense Description" : "$Highest NIBRS/UCR Offense Description",
		"Report Day" : "$Report Day", "Clearance Day" : "$Clearance Day"}}}]
	
	connection = MongoClient(MONGODB_HOST, MONGODB_PORT)
	collection = connection[DBS_NAME][COLLECTION_NAME]
	result = dict()
	result["name"] = "Crime"
	result["children"] = []

	for i in range(0, len(crime)):
		if (i == 0):
			query[0]["$match"].update({"Highest NIBRS/UCR Offense Description": { "$in" : [crime[i]] }})
		else:
			query[0]["$match"]["Highest NIBRS/UCR Offense Description"]["$in"] = [crime[i]]
		
		eachCrime = dict()
		eachCrime["name"] = crime[i]
		eachCrime["children"] = []

		projects = collection.aggregate(pipeline=query)

		for row in projects:
			if row["_id"]["Council District"] in council:
				report_date = row["_id"]["Report Day"]
				clear_date = row["_id"]["Clearance Day"]
				time_to_clear = clear_date - report_date
				timeStatus = returnSolveDurationStatus(time_to_clear)

				idx = findIdxofName(eachCrime["children"], timeStatus)

				if idx == -1:
					solveCategory = dict()
					solveCategory["name"] = timeStatus
					solveCategory["children"] = []
					eachCrime["children"].append(solveCategory)
					idx = len(eachCrime["children"]) - 1
					
				
				cidx = findIdxofName(eachCrime["children"][idx]["children"], "Council #" +\
					str(row["_id"]["Council District"]))
				
				if cidx == -1:
					crimeSolved = dict()
					crimeSolved["name"] = "Council #" + str(row["_id"]["Council District"])
					crimeSolved["total"] = 0
					eachCrime["children"][idx]["children"].append(crimeSolved)
					cidx = len(eachCrime["children"][idx]["children"]) - 1
					
					
				eachCrime["children"][idx]["children"][cidx]["total"] += 1

		
		result["children"].append(eachCrime)

	connection.close()
	return json.dumps(result)
	
def returnSolveDurationStatus(val):
	if val <= 30:
		return "<=30 days"
	if val <= 90:
		return ">30 days and <=90 days"
	if val <= 180:
		return ">90 days and <=180 days"
	if val <= 365:
		return ">180 days and <=365 days"

def findIdxofName(arr, val):
	for i in range(0, len(arr)):
		if arr[i]["name"] == val:
			return i

	return -1

@app.route("/tot_crime_data")
def get_tot_crime():
	council_arg = request.args.getlist('council')
	crime = request.args.getlist('crime')
	status = request.args.getlist('status')

	result = dict()

	for i in crime:
		result[i] = get_data(council_arg, [i], status, False)

	return json.dumps(result)

if __name__ == "__main__":
    app.run(host='0.0.0.0',port=5005,debug=True)





