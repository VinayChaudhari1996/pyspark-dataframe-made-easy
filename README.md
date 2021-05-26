# Pyspark Dataframe and RDD made easy 😀

SPARK DATAFRAME API WITH PYTHON 
> (Zero to Hero)
HAPPY LEARNING  ☺
- Vinay Chaudhari 
____
### ○ agg                           
### ○ alias                        
### ○ agg


# C
### ○ cache
### ○ coalesce
### ○ columns
### ○ corr
### ○ count
### ○ cov
### ○ crosstab
### ○ cube
### ○ coalesce

# D
### ○ describe
### ○ destinct
### ○ drop
### ○ dropDuplicates
### ○ dropna
### ○ dtypes


# E
### ○ explain


# F
### ○ fillna
### ○ filter
### ○ first
### ○ flatmap
### ○ foreach
### ○ foreachPartition
### ○ freqItems

# G
### ○ groupBy 

# H
### ○ head

# I
### ○ intersect
### ○ isLocal

# J
### ○ join

# L
### ○ limit

# M
### ○ map
### ○ mapPartitions

# N
### ○ na

# O
### ○ orderBy

# P
### ○ persist
### ○ printSchema

# R
### ○ randomSplit
### ○ rdd
### ○ registerTempTable
### ○ repartition
### ○ replace
### ○ rollup

# S
### ○ sample
### ○ sampleBy
### ○ schema
### ○ select
### ○ selectExpr
### ○ show
### ○ sort
### ○ sortWithPartitions
### ○ stat
### ○ subtract


## ✔ CONVERSIONS 


# T
### ○ take
### ○ toDF
### ○ toJSON
### ○ toPANDAS

# U
### ○ unionAll
### ○ upersist

# W
### ○ where(filter)
### ○ withColumn
### ○ withColumnRenamed
### ○ write

**** MAKE PR FOR CONTRIBUTION AND SUGGESTIONS :) ****


```python
import IPython 

```


```python
#agg

x = sqlContext.createDataFrame([("vinay","sunny",100),("deepak","parag",200),("akash","pravin",300)],['from','to','amount'])
y = x.agg({"amount":"avg"})

x.show()
y.show()
```

    +------+------+------+
    |  from|    to|amount|
    +------+------+------+
    | vinay| sunny|   100|
    |deepak| parag|   200|
    | akash|pravin|   300|
    +------+------+------+
    
    +-----------+
    |avg(amount)|
    +-----------+
    |      200.0|
    +-----------+
    
    


```python
#alias
from pyspark.sql.functions import col
x = sqlContext.createDataFrame([("vinay","sunny",100),("deepak","parag",200),("akash","pravin",300)],['from','to','amount'])
y = x.alias("transactions")

x.show()
y.select(col("transactions.to")).show()
```

    +------+------+------+
    |  from|    to|amount|
    +------+------+------+
    | vinay| sunny|   100|
    |deepak| parag|   200|
    | akash|pravin|   300|
    +------+------+------+
    
    +------+
    |    to|
    +------+
    | sunny|
    | parag|
    |pravin|
    +------+
    
    


```python
#cache 

x = sqlContext.createDataFrame([("vinay","sunny",100),("deepak","parag",200),("akash","pravin",300)],['from','to','amount'])
x.cache()

print(x.count()) #first action materializes x in memory
print(x.count()) #later actions avoid IO overhead
```

    3
    3
    


```python
#coalesce

x_rdd = sc.parallelize([("vinay","sunny",100),("deepak","parag",200),("akash","pravin",300)],4)
x = sqlContext.createDataFrame(x_rdd,['from','to','amount'])
y = x.coalesce(numPartitions=1)

print(x.rdd.getNumPartitions())
print(y.rdd.getNumPartitions())

x.show()
y.show()
```

    4
    1
    +------+------+------+
    |  from|    to|amount|
    +------+------+------+
    | vinay| sunny|   100|
    |deepak| parag|   200|
    | akash|pravin|   300|
    +------+------+------+
    
    +------+------+------+
    |  from|    to|amount|
    +------+------+------+
    | vinay| sunny|   100|
    |deepak| parag|   200|
    | akash|pravin|   300|
    +------+------+------+
    
    


```python
#collect

x = sqlContext.createDataFrame([("vinay","sunny",100),("deepak","parag",200),("akash","pravin",300)],['from','to','amount'])
y = x.collect() # it creates list of rows.
x.show()
print(y)
```

    +------+------+------+
    |  from|    to|amount|
    +------+------+------+
    | vinay| sunny|   100|
    |deepak| parag|   200|
    | akash|pravin|   300|
    +------+------+------+
    
    [Row(from='vinay', to='sunny', amount=100), Row(from='deepak', to='parag', amount=200), Row(from='akash', to='pravin', amount=300)]
    


```python
#columns 

x = sqlContext.createDataFrame([("vinay","sunny",100),("deepak","parag",200),("akash","pravin",300)],['from','to','amount'])

y = x.columns
x.show()
print(y)
```

    +------+------+------+
    |  from|    to|amount|
    +------+------+------+
    | vinay| sunny|   100|
    |deepak| parag|   200|
    | akash|pravin|   300|
    +------+------+------+
    
    ['from', 'to', 'amount']
    


```python
#corr : Calculates the correlation of
# two columns of a DataFrame as a double value. 

x = sqlContext.createDataFrame([("vinay","sunny",100,300),("deepak","parag",200,600),("akash","pravin",300,900)], ['from','to','amount','fees'])
y = x.corr(col1="amount",col2="fees")
x.show()
print(y)
```

    +------+------+------+----+
    |  from|    to|amount|fees|
    +------+------+------+----+
    | vinay| sunny|   100| 300|
    |deepak| parag|   200| 600|
    | akash|pravin|   300| 900|
    +------+------+------+----+
    
    1.0
    


```python
#count 
#Returns the number of rows in this DataFrame.
x = sqlContext.createDataFrame([("vinay","sunny",100),("deepak","parag",200),("akash","pravin",300)],['from','to','amount'])
x.show()
print(x.count())
```

    +------+------+------+
    |  from|    to|amount|
    +------+------+------+
    | vinay| sunny|   100|
    |deepak| parag|   200|
    | akash|pravin|   300|
    +------+------+------+
    
    3
    


```python
#cov
#Calculate the sample covariance for the given columns,
#specified by their names, as a double value. 

x = sqlContext.createDataFrame([("vinay","sunny",100,300),("deepak","parag",200,600),("akash","pravin",300,900)], ['from','to','amount','fees'])
y = x.cov(col1="amount",col2="fees")

x.show()
print(y)
```

    +------+------+------+----+
    |  from|    to|amount|fees|
    +------+------+------+----+
    | vinay| sunny|   100| 300|
    |deepak| parag|   200| 600|
    | akash|pravin|   300| 900|
    +------+------+------+----+
    
    30000.0
    


```python
#crosstab
x = sqlContext.createDataFrame([("vinay","deepak",0.1),("sunny","pratik",0.2),("parag","akash",0.3)], ['from','to','amt'])
y = x.crosstab(col1='from',col2='to')
x.show()
y.show()

```

    +-----+------+---+
    | from|    to|amt|
    +-----+------+---+
    |vinay|deepak|0.1|
    |sunny|pratik|0.2|
    |parag| akash|0.3|
    +-----+------+---+
    
    +-------+-----+------+------+
    |from_to|akash|deepak|pratik|
    +-------+-----+------+------+
    |  parag|    1|     0|     0|
    |  vinay|    0|     1|     0|
    |  sunny|    0|     0|     1|
    +-------+-----+------+------+
    
    

### col1 – The name of the first column. Distinct items will make the first item of each row.
### col2 – The name of the second column. Distinct items will make the column names of the DataFrame.


```python
#cube

# Create a multi-dimensional cube for the current DataFrame using the specified columns,
# so we can run aggregation on them

x = sqlContext.createDataFrame([("vinay","deepak",1),("sunny","pratik",2),("parag","akash",3)], ['from','to','amt'])

y = x.cube('from','to')
x.show()
print(y)
y.sum().show()
y.max().show()
```

    +-----+------+---+
    | from|    to|amt|
    +-----+------+---+
    |vinay|deepak|  1|
    |sunny|pratik|  2|
    |parag| akash|  3|
    +-----+------+---+
    
    <pyspark.sql.group.GroupedData object at 0x000001D968C4A320>
    +-----+------+--------+
    | from|    to|sum(amt)|
    +-----+------+--------+
    | null| akash|       3|
    | null|  null|       6|
    |vinay|deepak|       1|
    |vinay|  null|       1|
    | null|deepak|       1|
    |parag| akash|       3|
    | null|pratik|       2|
    |parag|  null|       3|
    |sunny|  null|       2|
    |sunny|pratik|       2|
    +-----+------+--------+
    
    +-----+------+--------+
    | from|    to|max(amt)|
    +-----+------+--------+
    | null| akash|       3|
    | null|  null|       3|
    |vinay|deepak|       1|
    |vinay|  null|       1|
    | null|deepak|       1|
    |parag| akash|       3|
    | null|pratik|       2|
    |parag|  null|       3|
    |sunny|  null|       2|
    |sunny|pratik|       2|
    +-----+------+--------+
    
    


```python
# Describe 

x = sqlContext.createDataFrame([("vinay","deepak",1),("sunny","pratik",2),("parag","akash",3)], ['from','to','amt'])

x.show()
x.describe().show()
```

    +-----+------+---+
    | from|    to|amt|
    +-----+------+---+
    |vinay|deepak|  1|
    |sunny|pratik|  2|
    |parag| akash|  3|
    +-----+------+---+
    
    +-------+-----+------+---+
    |summary| from|    to|amt|
    +-------+-----+------+---+
    |  count|    3|     3|  3|
    |   mean| null|  null|2.0|
    | stddev| null|  null|1.0|
    |    min|parag| akash|  1|
    |    max|vinay|pratik|  3|
    +-------+-----+------+---+
    
    


```python
# Distinct 

x = sqlContext.createDataFrame([("vinay","deepak",1),("sunny","pratik",2),("parag","akash",3),("parag","akash",3),("parag","akash",3)], ['from','to','amt'])
y = x.distinct()

x.show()
y.show()
```

    +-----+------+---+
    | from|    to|amt|
    +-----+------+---+
    |vinay|deepak|  1|
    |sunny|pratik|  2|
    |parag| akash|  3|
    |parag| akash|  3|
    |parag| akash|  3|
    +-----+------+---+
    
    +-----+------+---+
    | from|    to|amt|
    +-----+------+---+
    |sunny|pratik|  2|
    |vinay|deepak|  1|
    |parag| akash|  3|
    +-----+------+---+
    
    


```python
# Drop 

x = sqlContext.createDataFrame([("vinay","deepak",1),("sunny","pratik",2),("parag","akash",3)], ['from','to','amt'])
y = x.drop('amt')

x.show()
y.show()
```

    +-----+------+---+
    | from|    to|amt|
    +-----+------+---+
    |vinay|deepak|  1|
    |sunny|pratik|  2|
    |parag| akash|  3|
    +-----+------+---+
    
    +-----+------+
    | from|    to|
    +-----+------+
    |vinay|deepak|
    |sunny|pratik|
    |parag| akash|
    +-----+------+
    
    


```python
# dropDuplicates

x = sqlContext.createDataFrame([("vinay","deepak",1),("sunny","pratik",2),("parag","akash",3),("parag","akash",3),("parag","akash",3)], ['from','to','amt'])
y = x.dropDuplicates(subset=['from','to'])

x.show()
y.show()
```

    +-----+------+---+
    | from|    to|amt|
    +-----+------+---+
    |vinay|deepak|  1|
    |sunny|pratik|  2|
    |parag| akash|  3|
    |parag| akash|  3|
    |parag| akash|  3|
    +-----+------+---+
    
    +-----+------+---+
    | from|    to|amt|
    +-----+------+---+
    |vinay|deepak|  1|
    |sunny|pratik|  2|
    |parag| akash|  3|
    +-----+------+---+
    
    


```python
#dropna 
x = sqlContext.createDataFrame([(None,"vinay",0.1),("vinay","sunny",None),("Peter",None,0.3),("Mark","Steve",0.2)], ['from','to','amount'])
y = x.dropna(how='any',subset=['from','to'])
x.show()
y.show()
```

    +-----+-----+------+
    | from|   to|amount|
    +-----+-----+------+
    | null|vinay|   0.1|
    |vinay|sunny|  null|
    |Peter| null|   0.3|
    | Mark|Steve|   0.2|
    +-----+-----+------+
    
    +-----+-----+------+
    | from|   to|amount|
    +-----+-----+------+
    |vinay|sunny|  null|
    | Mark|Steve|   0.2|
    +-----+-----+------+
    
    


```python
#dtypes

x = sqlContext.createDataFrame([("vinay","deepak",1),("sunny","pratik",2),("parag","akash",3),("parag","akash",3),("parag","akash",3)], ['from','to','amt'])
y = x.dtypes

x.show()
print(y)
```

    +-----+------+---+
    | from|    to|amt|
    +-----+------+---+
    |vinay|deepak|  1|
    |sunny|pratik|  2|
    |parag| akash|  3|
    |parag| akash|  3|
    |parag| akash|  3|
    +-----+------+---+
    
    [('from', 'string'), ('to', 'string'), ('amt', 'bigint')]
    


```python
#Explain

x = sqlContext.createDataFrame([("vinay","deepak",1),("sunny","pratik",2),("parag","akash",3)], ['from','to','amt'])
x.show()

x.agg({"amt":"avg"}).explain(extended = True)


```

    +-----+------+---+
    | from|    to|amt|
    +-----+------+---+
    |vinay|deepak|  1|
    |sunny|pratik|  2|
    |parag| akash|  3|
    +-----+------+---+
    
    == Parsed Logical Plan ==
    'Aggregate ['avg(amt#169L) AS avg(amt)#187]
    +- AnalysisBarrier
          +- LogicalRDD [from#167, to#168, amt#169L], false
    
    == Analyzed Logical Plan ==
    avg(amt): double
    Aggregate [avg(amt#169L) AS avg(amt)#187]
    +- LogicalRDD [from#167, to#168, amt#169L], false
    
    == Optimized Logical Plan ==
    Aggregate [avg(amt#169L) AS avg(amt)#187]
    +- Project [amt#169L]
       +- LogicalRDD [from#167, to#168, amt#169L], false
    
    == Physical Plan ==
    *(2) HashAggregate(keys=[], functions=[avg(amt#169L)], output=[avg(amt)#187])
    +- Exchange SinglePartition
       +- *(1) HashAggregate(keys=[], functions=[partial_avg(amt#169L)], output=[sum#192, count#193L])
          +- *(1) Project [amt#169L]
             +- Scan ExistingRDD[from#167,to#168,amt#169L]
    


```python
#fillna 

x = sqlContext.createDataFrame([(None,"deepak",1),("sunny",None,2),("parag",None,3)], ['from','to','amt'])
y = x.fillna(value = '---',subset = ['from','to'])

x.show()
y.show()
```

    +-----+------+---+
    | from|    to|amt|
    +-----+------+---+
    | null|deepak|  1|
    |sunny|  null|  2|
    |parag|  null|  3|
    +-----+------+---+
    
    +-----+------+---+
    | from|    to|amt|
    +-----+------+---+
    |  ---|deepak|  1|
    |sunny|   ---|  2|
    |parag|   ---|  3|
    +-----+------+---+
    
    

## Filter (Most used api)


```python
# Filter 

x = sqlContext.createDataFrame([("vinay","deepak",1),("sunny","pratik",2),("parag","akash",3)], ['from','to','amt'])
y = x.filter("amt > 2 ")

x.show()
y.show()
```

    +-----+------+---+
    | from|    to|amt|
    +-----+------+---+
    |vinay|deepak|  1|
    |sunny|pratik|  2|
    |parag| akash|  3|
    +-----+------+---+
    
    +-----+-----+---+
    | from|   to|amt|
    +-----+-----+---+
    |parag|akash|  3|
    +-----+-----+---+
    
    


```python
# First

x = sqlContext.createDataFrame([("vinay","deepak",1),("sunny","pratik",2),("parag","akash",3)], ['from','to','amt'])
y = x.first()

x.show()
print(y)
```

    +-----+------+---+
    | from|    to|amt|
    +-----+------+---+
    |vinay|deepak|  1|
    |sunny|pratik|  2|
    |parag| akash|  3|
    +-----+------+---+
    
    Row(from='vinay', to='deepak', amt=1)
    

## Foreach 


```python
# foreach
from __future__ import print_function

# setup
fn = './foreachExampleDataFrames.txt' 
open(fn, 'w').close()  # clear the file
def fappend(el,f):
    '''appends el to file f'''
    print(el,file=open(f, 'a+') )

# example
x = sqlContext.createDataFrame([("vinay","deepak",1),("sunny","pratik",2),("parag","akash",3)], ['from','to','amt'])

y = x.foreach(lambda x: fappend(x,fn)) # writes into foreachExampleDataFrames.txt
x.show() # original dataframe
print(y) # foreach returns 'None'
# print the contents of the file
with open(fn, "r") as foreachExample:
    print (foreachExample.read())
```

    +-----+------+---+
    | from|    to|amt|
    +-----+------+---+
    |vinay|deepak|  1|
    |sunny|pratik|  2|
    |parag| akash|  3|
    +-----+------+---+
    
    None
    Row(from='vinay', to='deepak', amt=1)
    Row(from='sunny', to='pratik', amt=2)
    
    


```python
# foreachPartition
from __future__ import print_function

# setup
fn = './foreachExampleDataFrames.txt' 
open(fn, 'w').close()  # clear the file
def fappend(el,f):
    '''appends el to file f'''
    print(el,file=open(f, 'a+') )

# example
x = sqlContext.createDataFrame([("vinay","deepak",1),("sunny","pratik",2),("parag","akash",3)], ['from','to','amt'])

y = x.foreach(lambda x: fappend(x,fn)) # writes into foreachExampleDataFrames.txt
x.show() # original dataframe
print(y) # foreach returns 'None'
# print the contents of the file
with open(fn, "r") as foreachExample:
    print (foreachExample.read())
```

    +-----+------+---+
    | from|    to|amt|
    +-----+------+---+
    |vinay|deepak|  1|
    |sunny|pratik|  2|
    |parag| akash|  3|
    +-----+------+---+
    
    None
    Row(from='parag', to='akash', amt=3)
    Row(from='sunny', to='pratik', amt=2)
    Row(from='vinay', to='deepak', amt=1)
    
    


```python
# freqItems 

x = sqlContext.createDataFrame([("Vinay","sunny",50), \
                                ("Deepak","sunny",30), \
                                ("Vinay","Parag",20), \
                                ("Vinay","ram",50), \
                                ("sham","sunny",90), \
                                ("Vinay","pushpak",50), \
                                ("om","sunny",50), \
                                ("sagar","sunny",50), \
                                ("Vinay","rahul",80), \
                                ("akash","sunny",50), \
                                ("puranik","pranav",70)],\
                                ['from','to','amount'])

y = x.freqItems(cols=['from','amount'],support=0.8)

x.show()
y.show()
```

    +-------+-------+------+
    |   from|     to|amount|
    +-------+-------+------+
    |  Vinay|  sunny|    50|
    | Deepak|  sunny|    30|
    |  Vinay|  Parag|    20|
    |  Vinay|    ram|    50|
    |   sham|  sunny|    90|
    |  Vinay|pushpak|    50|
    |     om|  sunny|    50|
    |  sagar|  sunny|    50|
    |  Vinay|  rahul|    80|
    |  akash|  sunny|    50|
    |puranik| pranav|    70|
    +-------+-------+------+
    
    +--------------+----------------+
    |from_freqItems|amount_freqItems|
    +--------------+----------------+
    |       [Vinay]|            [50]|
    +--------------+----------------+
    
    

## groupBy (most used api)


```python
# groupBy

x = sqlContext.createDataFrame([("vinay","deepak",1),("sunny","pratik",2),("parag","akash",3)], ['from','to','amt'])
y = x.groupBy('amt')

x.show()
print(y)

```

    +-----+------+---+
    | from|    to|amt|
    +-----+------+---+
    |vinay|deepak|  1|
    |sunny|pratik|  2|
    |parag| akash|  3|
    +-----+------+---+
    
    <pyspark.sql.group.GroupedData object at 0x0000021742513CC0>
    


```python
# groupBy (col1).avg(col2)

x = sqlContext.createDataFrame([("vinay","deepak",12466641),("sunny","pratik",451232),("parag","akash",2555455)], ['from','to','amt'])
y = x.groupBy('from').avg('amt')

x.show()
y.show()

```

    +-----+------+--------+
    | from|    to|     amt|
    +-----+------+--------+
    |vinay|deepak|12466641|
    |sunny|pratik|  451232|
    |parag| akash| 2555455|
    +-----+------+--------+
    
    +-----+-----------+
    | from|   avg(amt)|
    +-----+-----------+
    |parag|  2555455.0|
    |sunny|   451232.0|
    |vinay|1.2466641E7|
    +-----+-----------+
    
    


```python
# head

x = sqlContext.createDataFrame([("vinay","deepak",12466641),("sunny","pratik",451232),("parag","akash",2555455)], ['from','to','amt'])

y = x.head(2)
x.show()
print(y)
```

    +-----+------+--------+
    | from|    to|     amt|
    +-----+------+--------+
    |vinay|deepak|12466641|
    |sunny|pratik|  451232|
    |parag| akash| 2555455|
    +-----+------+--------+
    
    [Row(from='vinay', to='deepak', amt=12466641), Row(from='sunny', to='pratik', amt=451232)]
    


```python
# intersect

x = sqlContext.createDataFrame([("vinay","deepak",12466641),("sunny","pratik",451232),("parag","akash",2555455),("parag","akash",2555455)], ['from','to','amt'])

y = sqlContext.createDataFrame([("vinay","deepak",12466641),("sunny","pratik",451232),("parag","akash",2555455),("parag","akashay",2555455)], ['from','to','amt'])

z = x.intersect(y)

x.show()
y.show()
z.show()
```

    +-----+------+--------+
    | from|    to|     amt|
    +-----+------+--------+
    |vinay|deepak|12466641|
    |sunny|pratik|  451232|
    |parag| akash| 2555455|
    |parag| akash| 2555455|
    +-----+------+--------+
    
    +-----+-------+--------+
    | from|     to|     amt|
    +-----+-------+--------+
    |vinay| deepak|12466641|
    |sunny| pratik|  451232|
    |parag|  akash| 2555455|
    |parag|akashay| 2555455|
    +-----+-------+--------+
    
    +-----+------+--------+
    | from|    to|     amt|
    +-----+------+--------+
    |sunny|pratik|  451232|
    |vinay|deepak|12466641|
    |parag| akash| 2555455|
    +-----+------+--------+
    
    


```python
# isLocal

x = sqlContext.createDataFrame([("vinay","deepak",12466641),("sunny","pratik",451232),("parag","akash",2555455),("parag","akash",2555455)], ['from','to','amt'])


y = x.isLocal()

x.show()
print(y)

```

    +-----+------+--------+
    | from|    to|     amt|
    +-----+------+--------+
    |vinay|deepak|12466641|
    |sunny|pratik|  451232|
    |parag| akash| 2555455|
    |parag| akash| 2555455|
    +-----+------+--------+
    
    False
    

## join (Most used api)


```python
# join 
x = sqlContext.createDataFrame([("vinay","deepak",12466641),("sunny","pratik",451232),("parag","akash",2555455),("Salman","akash",2555455)], ['from','to','amt'])
y = sqlContext.createDataFrame([('Andy',20),("Steve",40),("Elon",80)], ['name','age'])
z = x.join(y,x.to ==y.name,'inner').select('from','to','amt','age')
x.show()
y.show()
z.show()
```

    +------+------+--------+
    |  from|    to|     amt|
    +------+------+--------+
    | vinay|deepak|12466641|
    | sunny|pratik|  451232|
    | parag| akash| 2555455|
    |Salman| akash| 2555455|
    +------+------+--------+
    
    +-----+---+
    | name|age|
    +-----+---+
    | Andy| 20|
    |Steve| 40|
    | Elon| 80|
    +-----+---+
    
    +----+---+---+---+
    |from| to|amt|age|
    +----+---+---+---+
    +----+---+---+---+
    
    


```python
# join 
x = sqlContext.createDataFrame([("vinay","deepak",12466641),("sunny","pratik",451232),("parag","akash",2555455),("Salman","akash",2555455)], ['from','to','amt'])
y = sqlContext.createDataFrame([('Andy',20),("Steve",40),("Elon",80)], ['name','age'])
z = x.join(y,x.to ==y.name,'outer').select('from','to','amt','age')
x.show()
y.show()
z.show()
```

    +------+------+--------+
    |  from|    to|     amt|
    +------+------+--------+
    | vinay|deepak|12466641|
    | sunny|pratik|  451232|
    | parag| akash| 2555455|
    |Salman| akash| 2555455|
    +------+------+--------+
    
    +-----+---+
    | name|age|
    +-----+---+
    | Andy| 20|
    |Steve| 40|
    | Elon| 80|
    +-----+---+
    
    +------+------+--------+----+
    |  from|    to|     amt| age|
    +------+------+--------+----+
    |  null|  null|    null|  40|
    | sunny|pratik|  451232|null|
    | vinay|deepak|12466641|null|
    |  null|  null|    null|  20|
    | parag| akash| 2555455|null|
    |Salman| akash| 2555455|null|
    |  null|  null|    null|  80|
    +------+------+--------+----+
    
    


```python
# Limit

# join 
x = sqlContext.createDataFrame([("vinay","deepak",12466641),("sunny","pratik",451232),("parag","akash",2555455),("Salman","akash",2555455)], ['from','to','amt'])
y = x.limit(2)
x.show()
y.show()
```

    +------+------+--------+
    |  from|    to|     amt|
    +------+------+--------+
    | vinay|deepak|12466641|
    | sunny|pratik|  451232|
    | parag| akash| 2555455|
    |Salman| akash| 2555455|
    +------+------+--------+
    
    +-----+------+--------+
    | from|    to|     amt|
    +-----+------+--------+
    |vinay|deepak|12466641|
    |sunny|pratik|  451232|
    +-----+------+--------+
    
    




```python
# na

x = sqlContext.createDataFrame([(None,"Bob",0.1),("Bob","Carol",None),("Carol",None,0.3),("Bob","Carol",0.2)], ['from','to','amt'])
y = x.na  # returns an object for handling missing values, supports drop, fill, and replace methods
x.show()
print(y)
y.drop().show()

y.fill({'from':'unknown','to':'unknown','amt':0}).show()
y.fill('--').show()
```

    +-----+-----+----+
    | from|   to| amt|
    +-----+-----+----+
    | null|  Bob| 0.1|
    |  Bob|Carol|null|
    |Carol| null| 0.3|
    |  Bob|Carol| 0.2|
    +-----+-----+----+
    
    <pyspark.sql.dataframe.DataFrameNaFunctions object at 0x0000021742513940>
    +----+-----+---+
    |from|   to|amt|
    +----+-----+---+
    | Bob|Carol|0.2|
    +----+-----+---+
    
    +-------+-------+---+
    |   from|     to|amt|
    +-------+-------+---+
    |unknown|    Bob|0.1|
    |    Bob|  Carol|0.0|
    |  Carol|unknown|0.3|
    |    Bob|  Carol|0.2|
    +-------+-------+---+
    
    +-----+-----+----+
    | from|   to| amt|
    +-----+-----+----+
    |   --|  Bob| 0.1|
    |  Bob|Carol|null|
    |Carol|   --| 0.3|
    |  Bob|Carol| 0.2|
    +-----+-----+----+
    
    


```python
# orderBy

x = sqlContext.createDataFrame([("vinay","deepak",12466641),("sunny","pratik",451232),("parag","akash",2555455)], ['from','to','amt'])

y = x.orderBy(['amt'],ascending=[False])
z = x.orderBy(['amt'],ascending=[True])
x.show()
y.show()
z.show()

```

    +-----+------+--------+
    | from|    to|     amt|
    +-----+------+--------+
    |vinay|deepak|12466641|
    |sunny|pratik|  451232|
    |parag| akash| 2555455|
    +-----+------+--------+
    
    +-----+------+--------+
    | from|    to|     amt|
    +-----+------+--------+
    |vinay|deepak|12466641|
    |parag| akash| 2555455|
    |sunny|pratik|  451232|
    +-----+------+--------+
    
    +-----+------+--------+
    | from|    to|     amt|
    +-----+------+--------+
    |sunny|pratik|  451232|
    |parag| akash| 2555455|
    |vinay|deepak|12466641|
    +-----+------+--------+
    
    


```python
# PrintSchema

x = sqlContext.createDataFrame([("vinay","deepak",12466641),("sunny","pratik",451232),("parag","akash",2555455)], ['from','to','amt'])
x.show()
x.printSchema()
```

    +-----+------+--------+
    | from|    to|     amt|
    +-----+------+--------+
    |vinay|deepak|12466641|
    |sunny|pratik|  451232|
    |parag| akash| 2555455|
    +-----+------+--------+
    
    root
     |-- from: string (nullable = true)
     |-- to: string (nullable = true)
     |-- amt: long (nullable = true)
    
    


```python
# randomSplit

x = sqlContext.createDataFrame([("vinay","deepak",12466641),("sunny","pratik",451232),("parag","akash",2555455)], ['from','to','amt'])
y = x.randomSplit([0.5,0.5])

x.show()
y[0].show()
y[1].show()



```

    +-----+------+--------+
    | from|    to|     amt|
    +-----+------+--------+
    |vinay|deepak|12466641|
    |sunny|pratik|  451232|
    |parag| akash| 2555455|
    +-----+------+--------+
    
    +-----+------+-------+
    | from|    to|    amt|
    +-----+------+-------+
    |sunny|pratik| 451232|
    |parag| akash|2555455|
    +-----+------+-------+
    
    +-----+------+--------+
    | from|    to|     amt|
    +-----+------+--------+
    |vinay|deepak|12466641|
    +-----+------+--------+
    
    


```python
# rdd 

x = sqlContext.createDataFrame([("vinay","deepak",12466641),("sunny","pratik",451232),("parag","akash",2555455)], ['from','to','amt'])
y = x.rdd

x.show()
print(y.collect())

```

    +-----+------+--------+
    | from|    to|     amt|
    +-----+------+--------+
    |vinay|deepak|12466641|
    |sunny|pratik|  451232|
    |parag| akash| 2555455|
    +-----+------+--------+
    
    [Row(from='vinay', to='deepak', amt=12466641), Row(from='sunny', to='pratik', amt=451232), Row(from='parag', to='akash', amt=2555455)]
    


```python
# registerTempTable

x = sqlContext.createDataFrame([("vinay","deepak",12466641),("sunny","pratik",451232),("parag","akash",2555455)], ['from','to','amt'])
x.registerTempTable(name="TRANS")
y = sqlContext.sql('SELECT * FROM TRANS WHERE amt > 451232')

x.show()
y.show()
```

    +-----+------+--------+
    | from|    to|     amt|
    +-----+------+--------+
    |vinay|deepak|12466641|
    |sunny|pratik|  451232|
    |parag| akash| 2555455|
    +-----+------+--------+
    
    +-----+------+--------+
    | from|    to|     amt|
    +-----+------+--------+
    |vinay|deepak|12466641|
    |parag| akash| 2555455|
    +-----+------+--------+
    
    


```python
# repartiton 

x = sqlContext.createDataFrame([("vinay","deepak",12466641),("sunny","pratik",451232),("parag","akash",2555455)], ['from','to','amt'])
y = x.repartition(3)

print(x.rdd.getNumPartitions())
print(y.rdd.getNumPartitions())
y.show()
```

    4
    3
    +-----+------+--------+
    | from|    to|     amt|
    +-----+------+--------+
    |parag| akash| 2555455|
    |vinay|deepak|12466641|
    |sunny|pratik|  451232|
    +-----+------+--------+
    
    


```python
# replace

x = sqlContext.createDataFrame([("vinay","deepak",12466641),("sunny","pratik",451232),("parag","akash",2555455)], ['from','to','amt'])
y = x.replace('vinay','sunny',['from','to'])

x.show()
y.show()


```

    +-----+------+--------+
    | from|    to|     amt|
    +-----+------+--------+
    |vinay|deepak|12466641|
    |sunny|pratik|  451232|
    |parag| akash| 2555455|
    +-----+------+--------+
    
    +-----+------+--------+
    | from|    to|     amt|
    +-----+------+--------+
    |sunny|deepak|12466641|
    |sunny|pratik|  451232|
    |parag| akash| 2555455|
    +-----+------+--------+
    
    


```python
# replace

x = sqlContext.createDataFrame([('Sunny',"chirag",0.1),("deepak","vinay",0.2),("Carol","Dave",0.3)], ['from','to','amt'])
y = x.replace('Sunny','Pranav',['from','to'])

x.show()
y.show()

```

    +------+------+---+
    |  from|    to|amt|
    +------+------+---+
    | Sunny|chirag|0.1|
    |deepak| vinay|0.2|
    | Carol|  Dave|0.3|
    +------+------+---+
    
    +------+------+---+
    |  from|    to|amt|
    +------+------+---+
    |Pranav|chirag|0.1|
    |deepak| vinay|0.2|
    | Carol|  Dave|0.3|
    +------+------+---+
    
    


```python
#rollup

x = sqlContext.createDataFrame([("vinay","deepak",1246.6641),("sunny","pratik",4512.32),("parag","akash",2555.455)], ['from','to','amt'])
y = x.rollup(['from','to'])
x.show()

print(y)
#y is a grouped data object 
#aggregations will be applied to all numerical columns

y.sum().show()
y.max().show()
y.min().show()
```

    +-----+------+---------+
    | from|    to|      amt|
    +-----+------+---------+
    |vinay|deepak|1246.6641|
    |sunny|pratik|  4512.32|
    |parag| akash| 2555.455|
    +-----+------+---------+
    
    <pyspark.sql.group.GroupedData object at 0x000002C3E1627A58>
    +-----+------+---------+
    | from|    to| sum(amt)|
    +-----+------+---------+
    | null|  null|8314.4391|
    |vinay|deepak|1246.6641|
    |vinay|  null|1246.6641|
    |parag| akash| 2555.455|
    |parag|  null| 2555.455|
    |sunny|  null|  4512.32|
    |sunny|pratik|  4512.32|
    +-----+------+---------+
    
    +-----+------+---------+
    | from|    to| max(amt)|
    +-----+------+---------+
    | null|  null|  4512.32|
    |vinay|deepak|1246.6641|
    |vinay|  null|1246.6641|
    |parag| akash| 2555.455|
    |parag|  null| 2555.455|
    |sunny|  null|  4512.32|
    |sunny|pratik|  4512.32|
    +-----+------+---------+
    
    +-----+------+---------+
    | from|    to| min(amt)|
    +-----+------+---------+
    | null|  null|1246.6641|
    |vinay|deepak|1246.6641|
    |vinay|  null|1246.6641|
    |parag| akash| 2555.455|
    |parag|  null| 2555.455|
    |sunny|  null|  4512.32|
    |sunny|pratik|  4512.32|
    +-----+------+---------+
    
    


```python
# sample:-
# Returns a stratified sample without replacement based 
# on the fraction given on each stratum.

x = sqlContext.createDataFrame([("vinay","deepak",1246.6641),("sunny","pratik",4512.32),("parag","akash",2555.455)], ['from','to','amt'])
y = x.sample(False,0.5)

x.show()
y.show()

```

    +-----+------+---------+
    | from|    to|      amt|
    +-----+------+---------+
    |vinay|deepak|1246.6641|
    |sunny|pratik|  4512.32|
    |parag| akash| 2555.455|
    +-----+------+---------+
    
    +-----+------+--------+
    | from|    to|     amt|
    +-----+------+--------+
    |sunny|pratik| 4512.32|
    |parag| akash|2555.455|
    +-----+------+--------+
    
    


```python
#schema 
x = sqlContext.createDataFrame([("vinay","deepak",1246.6641),("sunny","pratik",4512.32),("parag","akash",2555.455)], ['from','to','amt'])
y = x.schema
x.show()
print(y)
```

    +-----+------+---------+
    | from|    to|      amt|
    +-----+------+---------+
    |vinay|deepak|1246.6641|
    |sunny|pratik|  4512.32|
    |parag| akash| 2555.455|
    +-----+------+---------+
    
    StructType(List(StructField(from,StringType,true),StructField(to,StringType,true),StructField(amt,DoubleType,true)))
    


```python
# SlectExpr
x = sqlContext.createDataFrame([("vinay","deepak",1246.6641),("sunny","pratik",4512.32),("parag","akash",2555.455)], ['from','to','amt'])
y = x.selectExpr(['substr(from,1,1)','amt+1000'])

x.show()
y.show()
```

    +-----+------+---------+
    | from|    to|      amt|
    +-----+------+---------+
    |vinay|deepak|1246.6641|
    |sunny|pratik|  4512.32|
    |parag| akash| 2555.455|
    +-----+------+---------+
    
    +---------------------+------------+
    |substring(from, 1, 1)|(amt + 1000)|
    +---------------------+------------+
    |                    v|   2246.6641|
    |                    s|     5512.32|
    |                    p|    3555.455|
    +---------------------+------------+
    
    


```python
# show

x = sqlContext.createDataFrame([("vinay","deepak",1246.6641),("sunny","pratik",4512.32),("parag","akash",2555.455)], ['from','to','amt'])
x.show()
```

    +-----+------+---------+
    | from|    to|      amt|
    +-----+------+---------+
    |vinay|deepak|1246.6641|
    |sunny|pratik|  4512.32|
    |parag| akash| 2555.455|
    +-----+------+---------+
    
    


```python
# sort

x = sqlContext.createDataFrame([("vinay","deepak",1246.6641),("sunny","pratik",4512.32),("parag","akash",2555.455)], ['from','to','amt'])
y = x.sort(['amt'])

x.show()
y.show()
```

    +-----+------+---------+
    | from|    to|      amt|
    +-----+------+---------+
    |vinay|deepak|1246.6641|
    |sunny|pratik|  4512.32|
    |parag| akash| 2555.455|
    +-----+------+---------+
    
    +-----+------+---------+
    | from|    to|      amt|
    +-----+------+---------+
    |vinay|deepak|1246.6641|
    |parag| akash| 2555.455|
    |sunny|pratik|  4512.32|
    +-----+------+---------+
    
    


```python
# sortWithinPartitions
x = sqlContext.createDataFrame([('vinay',"Bobby",0.1,1),("Bobby","sunny",0.2,2),("deepak","parag",0.3,2)], \
                               ['from','to','amt','p_id']).repartition(2,'p_id')
y = x.sortWithinPartitions(['to'])
x.show()
y.show()
print(x.rdd.glom().collect()) # glom() flattens elements on the same partition
print("\n")
print(y.rdd.glom().collect())
```

    +------+-----+---+----+
    |  from|   to|amt|p_id|
    +------+-----+---+----+
    | Bobby|sunny|0.2|   2|
    |deepak|parag|0.3|   2|
    | vinay|Bobby|0.1|   1|
    +------+-----+---+----+
    
    +------+-----+---+----+
    |  from|   to|amt|p_id|
    +------+-----+---+----+
    |deepak|parag|0.3|   2|
    | Bobby|sunny|0.2|   2|
    | vinay|Bobby|0.1|   1|
    +------+-----+---+----+
    
    [[Row(from='Bobby', to='sunny', amt=0.2, p_id=2), Row(from='deepak', to='parag', amt=0.3, p_id=2)], [Row(from='vinay', to='Bobby', amt=0.1, p_id=1)]]
    
    
    [[Row(from='deepak', to='parag', amt=0.3, p_id=2), Row(from='Bobby', to='sunny', amt=0.2, p_id=2)], [Row(from='vinay', to='Bobby', amt=0.1, p_id=1)]]
    


```python
# Stat :-Returns a 
# DataFrameStatFunctions for statistic functions.

x = sqlContext.createDataFrame([("vinay","Bobby",0.1,0.001),("Bobby","sunny",0.2,0.02),("sunny","pranav",0.3,0.02)], ['from','to','amt','fees'])
y = x.stat
x.show()
print(y)
print(y.corr(col1="amt",col2="fees"))

```

    +-----+------+---+-----+
    | from|    to|amt| fees|
    +-----+------+---+-----+
    |vinay| Bobby|0.1|0.001|
    |Bobby| sunny|0.2| 0.02|
    |sunny|pranav|0.3| 0.02|
    +-----+------+---+-----+
    
    <pyspark.sql.dataframe.DataFrameStatFunctions object at 0x00000241596F37F0>
    0.8660254037844386
    


```python
# subtract

x = sqlContext.createDataFrame([("vinay","Bobby",0.1,0.001),("Bobby","sunny",0.2,0.02),("sunny","pranav",0.3,0.02)], ['from','to','amt','fees'])
y = sqlContext.createDataFrame([("vinay","Bobby",0.1,0.001),("Bobby","sunny",0.2,0.02),("sunny","pranav",0.3,0.01)], ['from','to','amt','fees'])

z = x.subtract(y)
x.show()
y.show()
z.show()
```

    +-----+------+---+-----+
    | from|    to|amt| fees|
    +-----+------+---+-----+
    |vinay| Bobby|0.1|0.001|
    |Bobby| sunny|0.2| 0.02|
    |sunny|pranav|0.3| 0.02|
    +-----+------+---+-----+
    
    +-----+------+---+-----+
    | from|    to|amt| fees|
    +-----+------+---+-----+
    |vinay| Bobby|0.1|0.001|
    |Bobby| sunny|0.2| 0.02|
    |sunny|pranav|0.3| 0.01|
    +-----+------+---+-----+
    
    +-----+------+---+----+
    | from|    to|amt|fees|
    +-----+------+---+----+
    |sunny|pranav|0.3|0.02|
    +-----+------+---+----+
    
    


```python
x = sqlContext.createDataFrame([("vinay","Bobby",0.1,0.001),("Bobby","sunny",0.2,0.02),("sunny","pranav",0.3,0.02)], ['from','to','amt','fees'])

y = x.take(num=2)
x.show()
print(y)
```

    +-----+------+---+-----+
    | from|    to|amt| fees|
    +-----+------+---+-----+
    |vinay| Bobby|0.1|0.001|
    |Bobby| sunny|0.2| 0.02|
    |sunny|pranav|0.3| 0.02|
    +-----+------+---+-----+
    
    [Row(from='vinay', to='Bobby', amt=0.1, fees=0.001), Row(from='Bobby', to='sunny', amt=0.2, fees=0.02)]
    

# Conversions 


```python
#toDF

x = sqlContext.createDataFrame([('Alice',"Bob",0.1),("Bob","Carol",0.2),("Carol","Dave",0.3)], ['from','to','amt'])
y = x.toDF("seller","buyer","amt")
x.show()
y.show()
```

    +-----+-----+---+
    | from|   to|amt|
    +-----+-----+---+
    |Alice|  Bob|0.1|
    |  Bob|Carol|0.2|
    |Carol| Dave|0.3|
    +-----+-----+---+
    
    +------+-----+---+
    |seller|buyer|amt|
    +------+-----+---+
    | Alice|  Bob|0.1|
    |   Bob|Carol|0.2|
    | Carol| Dave|0.3|
    +------+-----+---+
    
    


```python
# toJson
x = sqlContext.createDataFrame([('Alice',"Bob",0.1),("Bob","Carol",0.2),("Carol","Alice",0.3)], ['from','to','amt'])
y = x.toJSON()

x.show()
print(y)
print("\n")
print(y.collect())

```

    +-----+-----+---+
    | from|   to|amt|
    +-----+-----+---+
    |Alice|  Bob|0.1|
    |  Bob|Carol|0.2|
    |Carol|Alice|0.3|
    +-----+-----+---+
    
    MapPartitionsRDD[193] at toJavaRDD at NativeMethodAccessorImpl.java:0
    
    
    ['{"from":"Alice","to":"Bob","amt":0.1}', '{"from":"Bob","to":"Carol","amt":0.2}', '{"from":"Carol","to":"Alice","amt":0.3}']
    


```python
# toPandas

x = sqlContext.createDataFrame([('Alice',"Bob",0.1),("Bob","Carol",0.2),("Carol","Alice",0.3)], ['from','to','amt'])
y = x.toPandas
x.show()
print(type(y))
y
```

    +-----+-----+---+
    | from|   to|amt|
    +-----+-----+---+
    |Alice|  Bob|0.1|
    |  Bob|Carol|0.2|
    |Carol|Alice|0.3|
    +-----+-----+---+
    
    <class 'method'>
    




    <bound method DataFrame.toPandas of DataFrame[from: string, to: string, amt: double]>




```python
# unionAll

x = sqlContext.createDataFrame([('Alice',"Bob",0.1),("Bob","Carol",0.2),("Carol","Alice",0.3)], ['from','to','amt'])
y = sqlContext.createDataFrame([('sunny',"Bob",0.1),("vinay","Carol",0.2),("pranav","Alice",0.3)], ['from','to','amt'])

z = x.unionAll(y)

x.show()
y.show()
z.show()

```

    +-----+-----+---+
    | from|   to|amt|
    +-----+-----+---+
    |Alice|  Bob|0.1|
    |  Bob|Carol|0.2|
    |Carol|Alice|0.3|
    +-----+-----+---+
    
    +------+-----+---+
    |  from|   to|amt|
    +------+-----+---+
    | sunny|  Bob|0.1|
    | vinay|Carol|0.2|
    |pranav|Alice|0.3|
    +------+-----+---+
    
    +------+-----+---+
    |  from|   to|amt|
    +------+-----+---+
    | Alice|  Bob|0.1|
    |   Bob|Carol|0.2|
    | Carol|Alice|0.3|
    | sunny|  Bob|0.1|
    | vinay|Carol|0.2|
    |pranav|Alice|0.3|
    +------+-----+---+
    
    


```python
# unpersist

x = sqlContext.createDataFrame([('Alice',"Bob",0.1),("Bob","Carol",0.2),("Carol","Alice",0.3)], ['from','to','amt'])
x.cache()
x.count()
x.show()

print(x.is_cached)
x.unpersist()
print(x.is_cached)
```

    +-----+-----+---+
    | from|   to|amt|
    +-----+-----+---+
    |Alice|  Bob|0.1|
    |  Bob|Carol|0.2|
    |Carol|Alice|0.3|
    +-----+-----+---+
    
    True
    False
    


```python
# where 
x = sqlContext.createDataFrame([('Alice',"Bob",0.1),("Bob","Carol",0.2),("Carol","Alice",0.3)], ['from','to','amt'])
y = x.where("amt > 0.2")

x.show()
y.show()
```

    +-----+-----+---+
    | from|   to|amt|
    +-----+-----+---+
    |Alice|  Bob|0.1|
    |  Bob|Carol|0.2|
    |Carol|Alice|0.3|
    +-----+-----+---+
    
    +-----+-----+---+
    | from|   to|amt|
    +-----+-----+---+
    |Carol|Alice|0.3|
    +-----+-----+---+
    
    


```python
# withColumn

x = sqlContext.createDataFrame([('Alice',"Bob",0.1),("Bob","Carol",0.2),("Carol","Alice",0.3)], ['from','to','amt'])
y = x.withColumn('conf',x.amt.isNotNull())

x.show()
y.show()
```

    +-----+-----+---+
    | from|   to|amt|
    +-----+-----+---+
    |Alice|  Bob|0.1|
    |  Bob|Carol|0.2|
    |Carol|Alice|0.3|
    +-----+-----+---+
    
    +-----+-----+---+----+
    | from|   to|amt|conf|
    +-----+-----+---+----+
    |Alice|  Bob|0.1|true|
    |  Bob|Carol|0.2|true|
    |Carol|Alice|0.3|true|
    +-----+-----+---+----+
    
    


```python
# withColumnRenamed
x = sqlContext.createDataFrame([('Alice',"Bob",0.1),("Bob","Carol",0.2),("Carol","Dave",0.3)], ['from','to','amt'])
y = x.withColumnRenamed('amt','amount')
x.show()
y.show()
```

    +-----+-----+---+
    | from|   to|amt|
    +-----+-----+---+
    |Alice|  Bob|0.1|
    |  Bob|Carol|0.2|
    |Carol| Dave|0.3|
    +-----+-----+---+
    
    +-----+-----+------+
    | from|   to|amount|
    +-----+-----+------+
    |Alice|  Bob|   0.1|
    |  Bob|Carol|   0.2|
    |Carol| Dave|   0.3|
    +-----+-----+------+
    
    


```python
# write
import json
x = sqlContext.createDataFrame([('Alice',"Bob",0.1),("Bob","Carol",0.2),("Carol","Dave",0.3)], ['from','to','amt'])
y = x.write.mode('overwrite').json('./dataframeWriteExample.json')
x.show()


# Read the DF back in from file
sqlContext.read.json('./dataframeWriteExample.json').show()
```

    +-----+-----+---+
    | from|   to|amt|
    +-----+-----+---+
    |Alice|  Bob|0.1|
    |  Bob|Carol|0.2|
    |Carol| Dave|0.3|
    +-----+-----+---+
    
    +---+-----+-----+
    |amt| from|   to|
    +---+-----+-----+
    |0.3|Carol| Dave|
    |0.1|Alice|  Bob|
    |0.2|  Bob|Carol|
    +---+-----+-----+
    
    


```python

```
