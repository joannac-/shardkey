shardkey.py
===========

Prerequisites:
--------------
First, run schema.js to generate the schema collection

1. Run a shell with schema.js loaded 
e.g. mongo <host>:<port>/<db name> --shell schema.js 

Then:
db.<collection>.schema({out: "<collection>_schema"})

e.g. if your collection is named "foobar" then run
db.foobar.schema({out: "foobar_schema"})

You will also need to install pymongo
(http://api.mongodb.org/python/current/installation.html)

Usage:
------

python shardkey.py <db name> <collection name> <server> <port>

e.g. python shardkey.py test twitter localhost 27202

The script will initially scrape the "<collection name>_schema" collection and
provide you with a list of fields able to be used as a shard key

It will prompt you for the field you wish to use as a shard key, the size of a
chunk, and the number of shards you wish to have.

The output goes into a file called shard.log -- to visualise the progress of
chunks please user the visualisation script bars.py as follows:

python bars.py shard.log

Each click of the "next" button steps through 20 steps (add / chunk split /
chunk migration)

The green chunk is the one affected by the last action.

Red chunks are jumbo chunks.


TODO:
-----
- Turning off balancer
- ~~Iterating by _id (right now just natural order)~~
- Limit number of docs 
- Cardinality of fields 
- Refactoring code
- More realistic simulation
-- pre-split chunks
-- multiple mongoSes
- Multi-field shard keys
- Read load
- Labelling chunks with min/max keys
