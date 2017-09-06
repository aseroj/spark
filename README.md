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
* apply transformation (map, filter, etc.)
* actions (count, etc.)

