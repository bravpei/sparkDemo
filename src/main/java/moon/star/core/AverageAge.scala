package moon.star.core

import java.io.{File, FileWriter}

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

object AverageAge extends App{
/*  val writer=new FileWriter(new File("/home/liupei/test/age.txt"),false)
  val rand=new Random()
  for(i<-1 to 100){
    writer.write(i+" "+rand.nextInt(100))
    writer.write(System.getProperty("line.separator"))
  }
  writer.flush()
  writer.close()*/
  val conf=new SparkConf().setMaster("local[*]").setAppName("AverageAge")
  val sc=new SparkContext(conf)
  val file=sc.textFile("/home/liupei/test/age.txt")
  val averageAge=file.map(_.split(" ")(1).toInt).sum.toDouble/file.count.toDouble
  println(s"Average age is $averageAge")
}
