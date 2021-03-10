package pers.xhl.processing;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.charset.Charset;
import java.util.Comparator;
import com.google.common.collect.MinMaxPriorityQueue;

//外部调用，对每个partition文件循环
//读取每个小文件到inputBuffer
// 对每个小文件执行算count的聚合,把结果写入hashTableBuffer
//当inputBuffer中的数据被完全读完时，把hashTableBuffer中的所有元素加入PriorityQueue

public class BufferHashGrouper {
  protected ByteBuffer hashTableBuffer;
  private String finalResultFilePath;
  private int topN;
  private int initialBuckets;

  protected BufferedHashTable hashTable;
  protected ByteBuffer keyBuffer;//存放读取的单个url;大小为1K
  final MinMaxPriorityQueue<UrlAndCount> mergeQueue;
  protected MappedByteBuffer mappedByteBuffer;
  protected ByteBuffer ucKeyBuffer;
  protected ByteBuffer ucAggBuffer;

  public BufferHashGrouper(
          ByteBuffer hashTableBuffer,
          int topN,
          String finalResultFilePath,
          int initialBuckets
  )
  {
    this.hashTableBuffer=hashTableBuffer;
    this.topN=topN;
    this.initialBuckets=initialBuckets;
    this.finalResultFilePath=finalResultFilePath;
    this.hashTable=new BufferedHashTable(hashTableBuffer,1024,Integer.BYTES,initialBuckets);
    this.mergeQueue = MinMaxPriorityQueue
            .orderedBy(Comparator.comparing(UrlAndCount::getCount).reversed())
            .maximumSize(topN)
            .create();
    this.keyBuffer=ByteBuffer.allocate(1024);
    this.ucAggBuffer=ByteBuffer.allocate(hashTable.aggSize);
    this.ucKeyBuffer=ByteBuffer.allocate(hashTable.keySize);
  }

  public void reset() throws Exception {
    hashTable.reset();
    keyBuffer.clear();
  }

  public void aggregateToHashTable(String curPartitionFilePath) throws IOException
  {
    long fileSize=readFileToInputBuffer(curPartitionFilePath);
    while(fileSize!=-1)
    {
      while(mappedByteBuffer.hasRemaining())
      {
        byte b = mappedByteBuffer.get();
        if(b==13) {
          keyBuffer.flip();
          if(aggregate(keyBuffer)!=0){
            System.out.println("Error in aggregate");
            System.exit(0);
          }
          keyBuffer.clear();
        } else if (b==10) {
          continue;
        }
        else {
          if(keyBuffer.hasRemaining())
          {
            keyBuffer.put(b);
          }
          else {
            System.out.println("keyBuffer is full, which means that the URL is too big");
          }
        }
      }
      fileSize--;
    }
  }

  private int aggregate(ByteBuffer keyBuffer)
  {
    String url = Charset.forName("utf-8").decode(keyBuffer).toString();
    int keyHash=url.hashCode() & Integer.MAX_VALUE;
    int bucket=hashTable.findBucketWithAutoGrowth(keyBuffer,keyHash);
    if(bucket<0)
    {
      System.out.println("bucket is "+bucket);
      System.out.println("Hash table is full!");
      return 1;
    }
    final boolean isBucketUsed=hashTable.isBucketUsed(bucket);

    if(!isBucketUsed)
    {
      hashTable.initBucketWithKey(keyBuffer,bucket);
    }
    //System.out.println("the bucket for this url is "+bucket);

    hashTable.countAggregate(bucket);
    return 0;
  }

  public void printQueueToFile(String finalResultFilePath)
  {
    try
    {
      PrintWriter pw=new PrintWriter(finalResultFilePath);
      BufferedWriter bw=new BufferedWriter(pw);
      while(!mergeQueue.isEmpty())
      {
        bw.write((mergeQueue.pollFirst()).toString());
        bw.write((byte)13);
      }
      bw.flush();
      bw.close();
    } catch (IOException e)
    {
      System.out.println("Can't find "+finalResultFilePath);
      e.printStackTrace();
    }
  }

  public long readFileToInputBuffer(String curPartitionFilePath) throws IOException//可能需要扩容inputBuffer
  {
    RandomAccessFile randomAccessFile = new RandomAccessFile(curPartitionFilePath, "r");
    FileChannel fileChannel = randomAccessFile.getChannel();
    long size=fileChannel.size();
    //System.out.println("file size is "+size);
    try{
      this.mappedByteBuffer=fileChannel.map(MapMode.READ_ONLY,0,size);
    }
    catch (IOException e) {
      e.printStackTrace();
    }
    randomAccessFile.close();
    mappedByteBuffer.clear();
    return size;
  }

  public void addToQueue()
  {
    int num=0;


    for(int curBucket=0;curBucket<hashTable.maxBuckets;curBucket++)
    {
      if(hashTable.isBucketUsed(curBucket))
      {
        //System.out.println("curBucket is "+curBucket);
        UrlAndCount uc=getUcByBucket(curBucket);
        //System.out.println(uc);
        mergeQueue.add(uc);
        //System.out.println("add"+uc+"to mergeQueue");
        num++;
      }
    }
    System.out.println("Totally: add "+num+" uc to queue");
  }

  public UrlAndCount getUcByBucket(int bucket)
  {
    hashTable.getUrlAndCount(bucket,ucKeyBuffer,ucAggBuffer);
    ucKeyBuffer.rewind();
    ucAggBuffer.rewind();
    String url= Charset.forName("utf-8").decode(ucKeyBuffer).toString();
    int count=ucAggBuffer.getInt();
    return new UrlAndCount(url,count);
  }

  class UrlAndCount
  {
    //get url and its count from the hashTableBuffer by bucket
    private String url;
    private Integer count;

    public UrlAndCount(String url , Integer count)
    {
      this.url=url;
      this.count=count;
    }
    public String toString() {
      return "URL: " + getUrl() + ", count: " + getCount();
    }
    public String getUrl()
    {
      return url;
    }
    public int getCount()
    {
      return count;
    }
  }





}

