package moon.star.core

import org.apache.spark.{SparkConf, SparkContext}

object WordCount extends App{
  val conf=new SparkConf().setAppName("wordCount").setMaster("local[*]")
  val sc=new SparkContext(conf)
  val file=sc.textFile("D:/test/fiction.txt")
  file.flatMap(_.split(" ")).map((_,1)).reduceByKey((_ + _)).filter(_._2>10).foreach(println)
}
