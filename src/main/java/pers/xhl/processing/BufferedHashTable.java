package pers.xhl.processing;

import java.nio.ByteBuffer;

public class BufferedHashTable {
  protected final ByteBuffer buffer;

  protected final int keySize;//final创建常量;url最大为1K
  protected final int aggSize;
  protected final int usedFlagSize=Integer.BYTES;
  protected final int oneBucketSize;//
  // 一个bucket结构：[isUsed][keySize][aggSize] 4+1024+4=1032B约等于1K，100M URL大约需要110M的buffer存放；
  protected int initialBuckets;

  protected int buckets; //current num of buckets
  protected int bufferCapacity;
  protected int maxBuckets;//max bukcets it could have. It may change when the table grows up;
  protected ByteBuffer tableBuffer; //Point to the current Buffer
  protected int tableStart;
  //一个英文字母/数字：1个字节；www.4字节，假设URL至少为4B，100M/4B =25M ,最多有25M个URL; 2500 0000<21亿，所以可以用Integer

  public BufferedHashTable(
          ByteBuffer buffer,
          int keySize,
          int aggSize,
          int initialBuckets
  )
  {
    this.buffer=buffer;//分给它200M，假设它肯定不会溢出
    this.keySize=keySize;
    this.aggSize=aggSize;
    this.oneBucketSize=usedFlagSize+keySize+aggSize;
    this.initialBuckets=initialBuckets;
    this.bufferCapacity=buffer.capacity();
  }
  public void reset() throws Exception {
    buffer.clear();
    buckets=0;
    maxBuckets=Math.min(bufferCapacity/oneBucketSize,initialBuckets);
    if (maxBuckets < 1) {
      throw new Exception(
              "Not enough capacity for even one row! Need "+oneBucketSize*initialBuckets+" but have "+ buffer.capacity()
      );
    }
    //给tablaBuffer赋值，这是符合当前size的buffer
    tableStart=bufferCapacity-maxBuckets*oneBucketSize;
    final ByteBuffer bufferDup=buffer.duplicate();//复制，其实用的是同一块内存
    bufferDup.position(tableStart);
    bufferDup.limit(tableStart+maxBuckets*oneBucketSize);
    tableBuffer=bufferDup.slice();
    System.out.println("reset maxBuckets"+maxBuckets);

    for(int i=0;i<maxBuckets*oneBucketSize;i++){
      tableBuffer.put((byte)0);
    }
  }

  public void initBucketWithKey(ByteBuffer keyBuffer,int bucket)
  {
    //把used置为1
    int bucketStartOffset=bucket*oneBucketSize;
    tableBuffer.position(bucketStartOffset);
    tableBuffer.putInt(0x80);
    buckets++;
    //把key拷贝过去；
    keyBuffer.position(0);
    keyBuffer.limit(keyBuffer.limit());
    tableBuffer.put(keyBuffer);
  }

  public void adjustTableWhenFull()
  {
    //TODO
  }

  protected void getUrlAndCount(int bucket,ByteBuffer ucKeyBuffer,ByteBuffer ucAggBuffer)
  {
    ucKeyBuffer.rewind();
    ucAggBuffer.rewind();
    int startOffset=bucket*oneBucketSize+usedFlagSize;
    int i;
    for(i=startOffset;i<startOffset+keySize;i++)
    {
      ucKeyBuffer.put(tableBuffer.get(i));
    }
    for(i=startOffset+keySize;i<startOffset+keySize+aggSize;i++)
    {
      ucAggBuffer.put(tableBuffer.get(i));
    }

  }

  protected boolean isBucketUsed(int bucket)
  {
    int used=0;
    try{
      used=tableBuffer.getInt( bucket * oneBucketSize);
    } catch (Exception e) {
      System.out.println(tableBuffer.capacity());
      System.out.println(e);
      System.out.println("EXCEPTION: bucket is "+bucket+", offset is "+bucket * oneBucketSize);
    }
    boolean ret = (used & 0x80)== 0x80;
    return ret;
  }

  protected int findBucketWithAutoGrowth(
          final ByteBuffer keyBuffer,
          final int keyHash
  )
  {
    int bucket = findBucket(keyBuffer,keyHash);
    if(bucket<0)
    {
      adjustTableWhenFull();
      bucket=findBucket(keyBuffer,keyHash);
    }
    return bucket;
  }

  private int findBucket(
          final ByteBuffer keyBuffer,
          final int keyHash
  )
  {
    int startBucket=keyHash%maxBuckets;
    int bucket=startBucket;

    outer:
    while(true){
      if(!isBucketUsed(bucket)){
        return bucket;
      }
      final int bucketOffset=bucket*oneBucketSize;
      //如果这个bucket已经被使用了：
      //可能1：这两个key相同，先判断是否完全相同，是的话就聚合；否则就是下一种可能
      //可能2：哈希冲突：顺序查看表中下一单元，直到找出一个空单元或查遍全表。
      for(int i=bucketOffset+usedFlagSize,j=0;j<keyBuffer.limit();i++,j++)
      {
        if(tableBuffer.get(i)!=keyBuffer.get(j)) {
          bucket += 1;//顺序探测；
          if (bucket == maxBuckets) {
            bucket = 0;//走到底了，从头开始
          }
          if (bucket == startBucket) {
            System.out.println("startBucket is "+startBucket);
            return -1;//hashTable需要扩容
          }
          continue outer;
          //只要有一个元素不对，就跳出里头的for循环，回到外侧的while循环
        }

      }
      return bucket;
    }
  }
  public void countAggregate(int bucket)//类内的成员变量（其实是外界传入的）hashTableBuffer
  {
    int position=bucket*oneBucketSize+usedFlagSize+keySize;
    tableBuffer.putInt(position,tableBuffer.getInt(position)+1);
  }

}
