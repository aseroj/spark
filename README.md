##spark 

full source available at [github](https://github.com/jleetutorial/sparkTutorial)

local[*] explains the number of cores that can be used.

``SparkConf conf = new SparkConf().setAppName("wordCount").setMaster("local[2]");``

##### RDD
* a capsulation around a very large dataset.
* can contain any type of objects, including user defined
* spark automatically cluster and parallelize data contained in RDD 

##### RDD Workflow
* init RDD 
* transformations (map, filter, etc.)
* actions (count, etc.)

##### Create RDD
1. take an existing project and pass it to SparkContext.parallelize method (for test or small sample)
1. load from external storage into SparkContext (S3 or HDFS, [jdbc](https://docs.databricks.com/spark/latest/data-sources/sql-databases.html), [cassandra](http://www.datastax.com/dev/blog/kindling-an-introduction-to-spark-with-cassandra-part-1), [ES](https://www.elastic.co/guide/en/elasticsearch/hadoop/current/spark.html), etc.)

