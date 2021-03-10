package pers.xhl.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

//根据配置【文件大小，文件路径】完成URL文件生成
//随机URL
//写到一个固定的buffer，500M，满了之后再写入文件
//追加写文件，重复200次，得到100G的文件
public class UrlGenerator {
  private String urlFilePath;
  private int lenMin;//设置URL的最短长度，不包括默认开头http://，所以这个最小为0
  private int lenMax;//暂定最大为1000
  public static String base = "abcdefghijklmnopqrstuvwxyz0123456789";//TODO 用ASCII码随机挑选一个
  private static final String[] url_suffix = ".com,.cn,.gov,.edu,.net,.org,.int,.mil,.biz,.info".split(",");

  public UrlGenerator(int lenMin,int lenMax) {
    this.lenMin=lenMin;
    this.lenMax=lenMax;
  }

//TODO
  //检测URL文件的大小--恰好生成100G
  public void generateToFile(long sizeKb,String urlFilePath) throws IOException
  {
    long urlNum=sizeKb*2;
    try
    {
      File file = new File(urlFilePath);
      if(!file.exists()) {
        file.createNewFile();
      }
      FileWriter fw = new FileWriter(file);
      BufferedWriter bw = new BufferedWriter(fw);

      for(int i=0;i<urlNum;i++)
      {
        String url=getRandomUrl();
        bw.write(url);
        bw.newLine();
      }
      bw.close();
      fw.close();
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
  }

  public String getRandomUrl()
  {
    int length = getRandomNum(lenMin, lenMax);
    StringBuffer sb = new StringBuffer();
    sb.append("http://");
    for (int i = 0; i < length; i++) {
      int number = getRandomNum(0, 35);
      sb.append(base.charAt(number));
    }
    sb.append(url_suffix[(int) (Math.random() * url_suffix.length)]);
    return sb.toString();
  }

  public static int getRandomNum ( int start, int end)
  {
    return (int) (Math.random() * (end - start + 1) + start);
  }

}

