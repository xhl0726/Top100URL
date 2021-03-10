package pers.xhl.processing;


import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

//读取文件filePath的每一行做hash,再%1024
//然后把这个String url（key)放到对应的buffer里，维护B-1个buffer--hashTable
//如果某一张map大于50M，就写到文件 mapFile_0...mapFile_1023【如果没有就新建个文件】
//一页大小为4K，设置一共1024个bucket,一个bucket用512KB的buffer 暂时设定，之后检测内存使用量
//Buffer_size=500K
//ByteBuffer可以使用直接内存（系统内存）（allocateDirect），使用后无需jvm回收。

public class FilePartition
{
  private int partitionNum;//划分为1024个小文件,B=1024
  private final int bufferSize=300*1024*1024;//reader buffer为512K，每个writer buffer为500K  35M * 2 = 700M 内存够
  private String urlFilePath;
  private String partitionFilePath;

  private ByteBuffer inputBuffer=ByteBuffer.allocate(bufferSize);//每次读取 25M 的文件

  public FilePartition(String urlFilePath,String partitionsFilePath,int partitionNum)
  {
    this.urlFilePath=urlFilePath;
    this.partitionFilePath=partitionsFilePath;
    this.partitionNum=partitionNum;
  }

  public void partition() throws IOException {
    RandomAccessFile randomAccessFile = new RandomAccessFile(urlFilePath, "rw");
    FileChannel fileChannel = randomAccessFile.getChannel();
    int bytesRead = fileChannel.read(inputBuffer);//read channel， write to inputBuffer
    ByteBuffer stringBuffer = ByteBuffer.allocate(1024);//一个URL最大为1K

    List<BufferedWriter> bufferedWriters = new ArrayList<>();
    for(int i=0;i<partitionNum;i++)
    {
      String filePath=partitionFilePath+"partition_"+i;
      bufferedWriters.add(new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filePath, true))));
    }

    while (bytesRead != -1) {
      inputBuffer.flip();//prepare to be read
      while (inputBuffer.hasRemaining()) {
        byte b = inputBuffer.get();
        if (b == 10 || b == 13) { //说明读到换行或回车，即一行结束，需要将它转化为具体的string
          stringBuffer.flip();//prepare to be read
          String line = Charset.forName("utf-8").decode(stringBuffer).toString();
          stringBuffer.clear();
          int part=hashCode(line)%partitionNum;
          bufferedWriters.get(part).write(line+"\r\n");
        } else {
          if (stringBuffer.hasRemaining()) {
            stringBuffer.put(b);
          } else {
            System.out.println("string Buffer is Full!");
          }
        }
      }
      inputBuffer.clear();
      bytesRead = fileChannel.read(inputBuffer);
    }

    for(int i=0;i<partitionNum;i++)
    {
      bufferedWriters.get(i).flush();
      bufferedWriters.get(i).close();
    }
    randomAccessFile.close();

  }


  public void hashToFile(String url) throws IOException
  {
    int bucket=hashCode(url)%partitionNum;
    String filePath=partitionFilePath+"partition_"+bucket;
    BufferedWriter bufferedWriter=new BufferedWriter(new OutputStreamWriter(
            new FileOutputStream(filePath, true)));
    bufferedWriter.write(url+"\r\n");
    bufferedWriter.close();
  }

  private int hashCode(String url)
  {
    return url.hashCode() & Integer.MAX_VALUE;
  }

}
