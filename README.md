# Top100URL

### Problem
There is a 100 GB file of URLs on the disk. Please write a small project to calculate the top 100 most common URLs and their frequency respectively in the file within 1 GB of memory.

#### Requirement
the program should not consume larger than 1 GB of memory.
the faster the better
the size of each URL is no more than 1KB.

### My Solution
#### 1. FilePartition 
  Split the 100G URL file to small partitions, which could fits into your 1G memory.
  
#### 2. BufferedHashMap
  The class BufferedHashGrouper uses BufferedHashMap to aggregate and store the KV result <URL,count> of each partition. To save memory, I wrote the HashMapTable class. The main structure of is below, and each bucket like this costs 1032B, which is less than java.util.HashMap. 
  
 [usedFlag|4B] + [keyUrl|1024B] + [count|4B] = 1032B
 
 The memory used by BufferedHashMap can be clearly calculated by {the number of buckets * oneBucketSize}. It stores the url and its count by putting the number into the right offset. The design of this structure comes from Apache Druid. 
  #TODO: Finish the function <adjustTableWhenFull>. It would use the rest room of the allocated buffer. 
 
 
#### 3. Merge
  The final step is to merge all the partial results. I uses the MinMaxPriorityQueue to get the top 100 URL. In fact, this process happens just after each partition has been aggregated into BufferHashMap, instead of reading the HashMap from the disk to avoid extra IO time. 
  
#### TODO
 1. Finish the function <adjustTableWhenFull> of BufferedHashMap.
 2. Optimize the parameters, such as partitionNum, initialBuckets, BufferSize, which would help us to get better performance.
 3. Get a virtual machine and generate larger URL files to test the performance.

### Test Report
### Usage
run "TopN.class" in package pers.xhl
The parameters are "hashTableSizeMb, urlSizeMb, filePartitionNum, initialBuckets". You can also define them in the code. 

#### Environment:
MacBook Pro 13
CPU: 1.7 GHz 4 Cores Intel Core i7
Memory: 16 GB 2133 MHz LPDDR3

#### Result:
1G UrlFile    PartitionNum=6   Time=  






