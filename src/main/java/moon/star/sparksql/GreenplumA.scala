package moon.star.sparksql

import java.util.Properties

import org.apache.spark.sql.SparkSession

object GreenplumA extends App {
  val spark=SparkSession.builder().appName("GreenplumA").master("local[*]").getOrCreate()
  val connectionProperties=new Properties()
  connectionProperties.put("user","gpadmin")
  connectionProperties.put("password","gpadmin")
  connectionProperties.put("driver","org.postgresql.Driver")
  "mes_part_info".split(",").foreach{x=>
    spark.read.jdbc("jdbc:postgresql://172.18.130.101:5432/postgres",x,connectionProperties).createOrReplaceTempView(x)
  }
  val result=spark.sql("select count(1) from mes_part_info")
  result.show
}
