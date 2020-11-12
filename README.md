# udacity-sf-crime

How did changing values on the SparkSession property parameters affect the throughput and latency of the data?

An evaluation metric can be the processedRowsPerSecond.
Due to the small rate of impinging data i couldn't see a big difference when tuning.

What were the 2-3 most efficient SparkSession property key/value pairs? Through testing multiple variations on values, how can you tell these were the most optimal?

It's hard to tell the optimal vaues on the data set, but i propose
maxOffsetPerTrigger with 200,
local[4], and
coffecup = "alwaysfull"
the throughput in terms of processedRowsPerSecond showed highest values

