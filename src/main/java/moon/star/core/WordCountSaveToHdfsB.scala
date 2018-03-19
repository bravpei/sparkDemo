package moon.star.core

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.{SparkConf, SparkContext}

object WordCountSaveToHdfsB extends App{
  System.setProperty("HADOOP_USER_NAME","hdfs")
  val conf=new SparkConf().setAppName("wordCount").setMaster("local[*]")
  val sc=new SparkContext(conf)
  val file=sc.textFile("hdfs://172.18.130.100/liupei/test/fiction.txt")
  val time = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date())
  file.flatMap(_.split(" ")).map((_,1)).reduceByKey((_ + _)).filter(_._2>10).saveAsTextFile("hdfs://172.18.130.100/liupei/test/results/"+time)
}
