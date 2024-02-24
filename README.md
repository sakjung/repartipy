# RepartiPy

RepartiPy helps you to elaborately handle PySpark DataFrame partition size.  

## Possible Use Cases
- Repartition your DataFrame precisely, **without knowing the whole DataFrame size** (i.e. `Dynamic Repartition`)
- Estimate your DataFrame size **with more accuracy**

## Why RepartiPy
 
Although Spark [SizeEstimator](https://spark.apache.org/docs/latest/api/java/index.html?org/apache/spark/util/SizeEstimator.html) can be used to estimate a DataFrame size, it is not accurate sometimes. 
RepartiPy uses **Spark's execution plan statistics** in order to provide a roundabout way.
It suggests two approaches to achieve this:

- `reaprtipy.SizeEstimator`
- `reaprtipy.SamplingSizeEstimator`

### reaprtipy.SizeEstimator
Recommended when your executor resource (memory) is affordable to cache the whole DataFrame. 
`SizeEstimator` just simply caches the whole Dataframe into the memory and extract the execution plan statistics.

### repartipy.SamplingSizeEstimator
Recommended when your executor resource (memory) is ***NOT*** affordable to cache the whole dataframe.
`SamplingSizeEstimator` uses 'disk write and re-read (HDFS)' approach behind the scene for two reasons:

1. Prevent double read from the source data (check point) -> better performance
2. Reduce partition skewness by reading again (leverage MaxSplitBytes) -> better sampling result

Therefore, **you must have HDFS settings on your cluster and enough disk space.** 

This may not be accurate compared to `SizeEstimator` due to sampling. 
If you want more accurate results, tune the `sample_count` option properly.
Additionally, this approach will be slower than `SizeEstimator` as `SamplingSizeEstimator` requires disk I/O and additional logics.

# How To Use
## Setup
```shell
pip install repartipy
```
### Prerequisite
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
input_data = [
        (1, "Seoul"),
        (2, "Busan"),
    ]
df = spark.createDataFrame(data=input_data, schema=["id", "location"])
```

### get_desired_partition_count()
***Calculate ideal number of partitions for a DataFrame*** 

SizeEstimator will suggest `desired_partition_count`, so that each partition can have `desired_partition_size_in_bytes` (default: 1GiB) after repartition.
`reproduce()` produces exactly the same `df`, but internally reproduced by SizeEstimator for better performance. 
`SizeEstimator` reproduces `df` from **Memory** (Cache). 
`SamplingSizeEstimator` reproduces `df` from **Disk** (HDFS).

#### with SizeEstimator
```python
import repartipy

one_gib_in_bytes = 1073741824

with repartipy.SizeEstimator(spark=spark, df=df) as se:
    desired_partition_count = se.get_desired_partition_count(desired_partition_size_in_bytes=one_gib_in_bytes)
    
    se.reproduce().repartition(desired_partition_count).write.save("your/write/path")
    # or 
    se.reproduce().coalesce(desired_partition_count).write.save("your/write/path")
```
#### with SamplingSizeEstimator
```python
import repartipy
    
one_gib_in_bytes = 1073741824

with repartipy.SamplingSizeEstimator(spark=spark, df=df, sample_count=10) as se:
    desired_partition_count = se.get_desired_partition_count(desired_partition_size_in_bytes=one_gib_in_bytes)
    
    se.reproduce().repartition(desired_partition_count).write.save("your/write/path")
    # or 
    se.reproduce().coalesce(desired_partition_count).write.save("your/write/path")
```

### estimate()
***Estimate size of a DataFrame***
#### with SizeEstimator
```python
import repartipy

with repartipy.SizeEstimator(spark=spark, df=df) as se:
    df_size_in_bytes = se.estimate()
```
#### with SamplingSizeEstimator
```python
import repartipy

with repartipy.SamplingSizeEstimator(spark=spark, df=df, sample_count=10) as se:
    df_size_in_bytes = se.estimate()
```

# Benchmark

Overall, there appears to be a slight performance loss when employing RepartiPy. 
This benchmark compares the **running time of spark jobs** in the following two cases to give a rough estimate:
- **Static** Repartition (repartition without RepartiPy)
```python
# e.g.
df.repartition(123).write.save("your/write/path")
```
- **Dynamic** Repartition (repartition with RepartiPy)
```python
# e.g.
with repartipy.SizeEstimator(spark=spark, df=df) as se:
    desired_partition_count = se.get_desired_partition_count(desired_partition_size_in_bytes=one_gib_in_bytes)
    se.reproduce().repartition(desired_partition_count).write.save("your/write/path")
```
All the other conditions remain the same **except the usage of RepartiPy**.

> **Note**
> 
> Benchmark results provided are for brief reference only, not absolute.
> Actual performance metrics can vary depending on your own circumstances (e.g. your data, your spark code, your cluster resources, ...).

## SizeEstimator
- DataFrame Size ~= 256 MiB (decompressed size)

|              | Static  | Dynamic |
|:------------:|:-------:|:-------:|
| Running Time | 8.5 min | 8.6 min |
 

## SamplingSizeEstimator
- DataFrame Size ~= 241 GiB (decompressed size)

|              | Static | Dynamic |
|:------------:|:------:|:-------:|
| Running Time | 14 min | 16 min  |
