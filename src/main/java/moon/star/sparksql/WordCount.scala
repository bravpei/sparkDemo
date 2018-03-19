package moon.star.sparksql


import org.apache.spark.sql.SparkSession

object WordCount extends App{
  val spark=SparkSession.builder().appName("wordCount").master("local[*]").getOrCreate()
  val file=spark.read.text("/home/liupei/test/fiction.txt").rdd
  file.map(_.toString()).flatMap(_.split(" ")).map((_,1)).reduceByKey(_ + _).filter(_._2>10).foreach(println)
}
