from pymongo import MongoClient
from random import randint
import datetime
import sys
from sys import argv

def write_add(cl):
	for i in xrange(len(shards)):
		if cl in shards[i]:
			cln = shards[i].index(cl)
			mystr = "add " + str(i) + " " + str(cln) + "\n"
			output.write(mystr)


def getkey(doc, shkey):
	toks = shkey.split(".")
	result = doc
	for tok in toks:
		result = result[tok]
	return result


def do_balance(minshard, maxshard, diff, maxchunks, total):
	#print shards[minshard]
	#print shards[maxshard]
	if (total < 20 and diff > 1) or (total < 80 and diff > 3) or (diff > 7):
		move = randint(0,maxchunks-1)
		chunkno = shards[maxshard].pop(move)
		shards[minshard].append(chunkno)
		chunks[chunkno]["shard"] = minshard
		mystr = "move " + str(maxshard) + " " + str(move) + " " + str(minshard) + "\n"
		output.write(mystr)
		
	#print shards[minshard]
	#print shards[maxshard]
	#print shards


def balance():
	minchunks = len(shards[0])
	minshard = 0
	maxchunks = len(shards[0])
	maxshard = 0
	total_chunks = 0
	#print shards
	for i in xrange(len(shards)):
		if len(shards[i]) < minchunks:
			minchunks = len(shards[i])
			minshard = i
		if len(shards[i]) > maxchunks:
			maxchunks = len(shards[i])
			maxshard = i
		total_chunks += len(shards[i])

	diff = maxchunks - minchunks

	do_balance(minshard, maxshard, diff, maxchunks, total_chunks)
	
	
		

def split_chunk(i):
	y = chunks[i]["keys"][:]
	y.sort()
	s = chunks[i]["size"]
	#print y
	#print chunks[i]["min"],chunks[i]["max"],s,chunks[i]["shard"]

	if y == chunks[i]["keys"][:]:
		# 90-10 split
		splitpt = int(s * 0.9)
	else:
		#50 -50 split
		splitpt = int(s * 0.5)
	splitval = y[splitpt]
	k = y.index(splitval)


	# k = 0
	if k == 0:
		j = s-1
		if y[j] == splitval:
			# jumbo chunk
			return
		else: 
			k = y.index(y[j])
			splitval = y[k]
	n = len(chunks)
	chunks.append({})
	
	chunks[n]["size"] = s-k
	chunks[n]["min"] = splitval
	chunks[n]["max"] = chunks[i]["max"]
	sh = chunks[i]["shard"]
	chunks[n]["shard"] = sh
	chunks[n]["keys"] = y[k:]

	chunks[i]["size"] = k
	chunks[i]["max"] = splitval
	chunks[i]["keys"] = y[:k]
		
	#print chunks[i]["min"],chunks[i]["max"],chunks[i]["size"],chunks[i]["shard"]
	#print chunks[n]["min"],chunks[n]["max"],chunks[n]["size"],chunks[n]["shard"]
	#print ""

	shards[sh].append(n)

	mystr = "split " + str(sh) + " " + str(shards[sh].index(i)) + " " + str(k) + "\n"
	output.write(mystr)
	balance()
	


script, dbi, tcoll, server, port= argv
conn = MongoClient(server, int(port))
# conn.write_concern = {'w': 'majority', 'wtimeout':6000}
db = conn[dbi]
totaldocs = 0
allfields = {}
candidates = {}

#with open("schema.js", 'r') as f:
#	s = f.read()
#print s
#x = db.eval(s)
#print x

tcollsch = tcoll + "_schema"
tcollout = db[tcoll]
tcollschout = db[tcollsch]

cur = tcollschout.find()
obj = next(cur, None)

res = tcollschout.aggregate( [{"$unwind": "$value.results"}, {"$project": {"_id":1,
"datatype": "$value.results.type", "num": "$value.results.docs"}}] )

fields = res['result']

for x in fields:
	#print "x is "
	#print x
	field = x['_id']
	#print "field is " + field
	#print allfields[field]
	if field not in allfields:
		#print "new field!"
		allfields[field] = {}

	(allfields[field])[(x['datatype'])] = x['num']
	#print allfields[field]
	#print "done"

numalldocs = allfields['_id']['all']

#print allfields
numfields = 0

for x in allfields:
	#print x
	#for y in allfields[x]:
		#if y != "all":
			#print('\t'),
			#print y,
			#print ": ",
			#print allfields[x][y]
	if allfields[x]['all'] == numalldocs and 'array' not in allfields[x] and not "$" in x:
		if 'null' not in allfields[x] or allfields[x]['null'] == 0 :
			candidates[x] = 1
			numfields+=1
		

print "\nThere are " + str(numfields) + " candidate fields for shard key:"
for x in candidates:
	print x + ",",

print "\n"

shkey = raw_input("Which field do you want in your key? Just field name, please: ")
while shkey not in candidates:
	print "Sorry, that's not a valid field."
	shkey = raw_input("Which field do you want in your key? Just field name, please: ")

collstats = db.command({"collstats" : tcoll} )
avgdoc = collstats["avgObjSize"]

chunksize = raw_input("How large is a chunk, in bytes? [default: 64MB = 67108864] ")
if chunksize == "":
	chunksize = "67108864"
while not chunksize.isdigit():
	print "Sorry, that's not a valid size."
	chunksize = raw_input("How large is a chunk, in bytes? [default: 64MB = 67108864] ")
	if chunksize == "":
		chunksize = "67108864"

while int(chunksize) < int(avgdoc):
	print "Your average doc size is", avgdoc
	print "Please make your chunk size larger than this"
	chunksize = raw_input("How large is a chunk, in bytes? [default: 64MB = 67108864]")
	if chunksize == "":
		chunksize = "67108864"
	while not chunksize.isdigit():
		print "Sorry, that's not a valid size."
		chunksize = raw_input("How large is a chunk, in bytes? [default: 64MB = 67108864] ")
		if chunksize == "":
			chunksize = "67108864"

chunksize = int(chunksize)
docspers = int(chunksize/avgdoc)
#print chunksize, avgdoc, docspers

chunks = []
chunks.append({})
chunks[0]["min"] = "$$min"
chunks[0]["max"] = "$$max"
chunks[0]["size"] = 0
chunks[0]["shard"] = 0
chunks[0]["keys"] = []

mshards = raw_input("How many shards? ")
while not mshards.isdigit():
	print "Sorry, that's not a valid number of shards."
	mshards = raw_input("How many shards? ")

nshards = int(mshards)
shards = []
for i in xrange(nshards):
	shards.append([])

shards[0].append(0)

output = open("shard.log", "w")

for doc in tcollout.find():
	#print doc
	key = getkey(doc, shkey)
	for i in xrange(len(chunks)):
		#print str(i) + "   ",
		#print chunks[i]["min"],chunks[i]["max"],chunks[i]["size"],chunks[i]["shard"]
		chunk = chunks[i]
		#if chunk == {}:
			#print "ARGH!"
			#sys.exit(0)
		#print chunk
		if (chunk["min"] == "$$min" or chunk["min"] <= key) and (chunk["max"] == "$$max" or key < chunk["max"]):
			chunks[i]["size"] += 1
			chunks[i]["keys"].append(key)
			write_add(i)
			if chunks[i]["size"] >= docspers:
				split_chunk(i);
	#print ""
	#print chunks[i]["size"]


#for i in xrange(len(chunks)):
	#print chunks[i]["min"],chunks[i]["max"],chunks[i]["size"],chunks[i]["shard"]

print "\n\nThe final distribution of chunks per shard:"
for i in xrange(len(shards)):
	print len(shards[i]),
print ""	

print "\n\nThe final chunks"
print "#\tmin\tmax\tsize"
for i in xrange(len(chunks)):
	print str(i) + "\t" + str(chunks[i]["min"]) + "\t" +str(chunks[i]["max"]) + "\t" + str(chunks[i]["size"])

output.close()
