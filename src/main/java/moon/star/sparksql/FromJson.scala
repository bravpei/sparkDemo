package moon.star.sparksql

import org.apache.spark.sql.SparkSession

object FromJson extends App{
  val spark=SparkSession.builder().appName("GreenplumA").master("local[*]").getOrCreate()
  val df=spark.read.json("/home/liupei/test/person.json")
  df.show
}
