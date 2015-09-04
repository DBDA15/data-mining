Spark Version
-----

How to execute:
```
/opt/spark/spark-1.3.0/bin/spark-submit --master spark://172.16.21.111:7077 --driver-memory 7g --conf spark.executor.memory=4g --total-executor-cores 1 --class de.hpi.dbda.Main /home/martin.gebert/spark-caching.jar pathToHDFSNetflixFile pathToResultFile executionMethod{strings,ints,trie,fpc}  minimumSupport confidence
```

For example:
```
/opt/spark/spark-1.3.0/bin/spark-submit --master spark://172.16.21.111:7077 --driver-memory 7g --conf spark.executor.memory=4g --total-executor-cores 1 --class de.hpi.dbda.Main /home/martin.gebert/spark-caching.jar hdfs://tenemhead2:8020/data/netflix.txt /home/mariya.perchyk/result.txt trie 3500 0.3
```

Execution Methods
----------------

* Strings
        - the simplest version of the apriori algorithm with transaction items represented as strings
* Ints
        - apriori algorithm that parses the transactions items as integers
* Trie
        - apriori algorithm that uses tries (prefix trees) to store its candidates. This noticably speeds up the lookup.
* FPC
        - FPC version of the apriori algorithm, suggested in the paper "Apriori-based Frequent Itemset Mining Algorithms on MapReduce"



Different branches in Flink
--------
* iterations-notoptimized
        - uses Flink's delta iteration and no optimizations
* iterations-optimized
        - uses Flink's delta iteration and suggested optimization
* noiterations
        - uses manual loop unrolling

Flink Version
-------

How to execute:
```
/opt/flink/flink-0.9.0/bin/flink run [-yjm 7168 -ytm 4096] -c de.hpi.dbda.Main pathToTheJarFile pathToHDFSNetflixFile pathToResultFile executionMethod{strings,ints,trie,fpc}  minimumSupport confidence
```

For example:
```
/opt/flink/flink-0.9.0/bin/flink run -yjm 7168 -ytm 4096 -c de.hpi.dbda.Main /home/martin.gebert/iterations-notoptimized.jar hdfs://tenemhead2:8020/data/netflix.txt /home/mariya.perchyk/result.txt trie 3500 0.3
```

```
/opt/flink/flink-0.9.0/bin/flink run -yjm 7168 -ytm 4096 -c de.hpi.dbda.Main /home/martin.gebert/iterations-optimized.jar hdfs://tenemhead2:8020/data/netflix.txt /home/mariya.perchyk/result.txt trie 3500 0.3
```

```
/opt/flink/flink-0.9.0/bin/flink run -yjm 7168 -ytm 4096 -c de.hpi.dbda.Main /home/martin.gebert/noiterations.jar hdfs://tenemhead2:8020/data/netflix.txt /home/mariya.perchyk/result.txt trie 3500 0.3
```
