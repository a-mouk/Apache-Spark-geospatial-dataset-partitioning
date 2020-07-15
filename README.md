# Apache Spark geospatial dataset partitioning and spatial join
Process for creating balanced partitions of geospatial data, regardless of their distribution.

The process is about spatial datasets. We ask to get pairs of elements between the two datasets that exist below a distance that we define.

The process is automated, meaning that the users may select the number of partitions that they want and the process will always create balanced partitions. It also works for every distance that we define.

The key point is the use of sampling and quantiles.
