package pers.xhl;

import pers.xhl.processing.*;
import pers.xhl.util.Examination;
import pers.xhl.util.UrlGenerator;

import java.nio.ByteBuffer;

public class TopN {
  //Parameters: urlSizeMb,filePartitionNum,initialBuckets,hashTableSizeMb
  public static void main(String[] args) throws Exception {
    String urlFilePath="src/main/resources/urls.txt";
    String partitionsFilePath="src/main/resources/partition/";
    String finalResultFilePath="src/main/resources/result.txt";

    int topN=100;
//    int hashTableSizeMb=Integer.parseInt(args[2]);
//    int urlSizeMb=Integer.parseInt(args[0]);
//    int partitionNum=Integer.parseInt(args[1]);
//    int initialBuckets=Integer.parseInt(args[3]);
    int hashTableSizeMb=600;
    int urlSizeMb=1024;
    int filePartitionNum=5;
    int initialBuckets=100000000;

    UrlGenerator urlGenerator=new UrlGenerator(0,100);//UrlLengthRange
    urlGenerator.generateToFile(urlSizeMb,urlFilePath);//1024=1M,1024*1024=1G

    System.out.println("-----BEGIN-PARTITION-----");
    Examination.start();

    FilePartition filePartition = new FilePartition(urlFilePath,partitionsFilePath,filePartitionNum);
    filePartition.partition();


    ByteBuffer hashTableBuffer=ByteBuffer.allocate(hashTableSizeMb*1024*1024) ;
    BufferHashGrouper hashGrouper=new BufferHashGrouper(hashTableBuffer,topN,finalResultFilePath, initialBuckets);
    System.out.println("-----BEGIN-HASH-----");
    for(int i=0;i<filePartitionNum;i++) //这个for循环之后可以改成iterator的形式
    {
      String curPartitionFilePath=partitionsFilePath+"partition_"+i;
      hashGrouper.reset();
      hashGrouper.aggregateToHashTable(curPartitionFilePath);
      hashGrouper.addToQueue();
    }
    hashGrouper.printQueueToFile(finalResultFilePath);
    Examination.end();
    System.out.println("OK! You can check the final result in "+finalResultFilePath);
  }
}
