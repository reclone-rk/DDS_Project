from datetime import time
import partitioning_paper as Partioning
import rangequery_paper as RangeQuery
import datetime
import time

print("Creating Database Name dds")
Partioning.createDB()

print("Getting connection from dds database")
con = Partioning.getOpenConnection()

print("Creating and Loading the ratings table")
Partioning.loadRatings('ratings','data/db.dat',con)

print("Doing Range Partitioning")
Partioning.rangePartition('ratings',53,con)

st = time.time()
# print("Performing Range Query")
RangeQuery.FastRangeQuery('ratings','aa','zz',5,con)
# RangeQuery.RangeQuery('ratings','aa','az',con)
en = time.time()

print((en-st)*1000)

st = time.time()
# print("Performing Fast Range Query")
RangeQuery.RangeQuery('ratings','aa','zz',5, con)
# RangeQuery.RangeQuery('ratings','aa','az',con)
en = time.time()

print((en-st)*1000)

print("Deleting all Tables")
Partioning.deleteTables('all',con)
