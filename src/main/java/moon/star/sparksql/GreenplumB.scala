package moon.star.sparksql

import java.util.Properties

import org.apache.spark.sql.SparkSession

object GreenplumB extends App {
  val spark=SparkSession.builder().appName("GreenplumA").master("local[*]").getOrCreate()
  val connectionProperties=new Properties()
  connectionProperties.put("user","gpadmin")
  connectionProperties.put("password","gpadmin")
  connectionProperties.put("driver","org.postgresql.Driver")
  "test001,test002".split(",").foreach{x=>
    spark.read.jdbc("jdbc:postgresql://172.18.130.101:5432/postgres",x,connectionProperties).createOrReplaceTempView(x)
  }
  val result=spark.sql("select a.name,b.age from test001 a,test002 b where a.id=b.id")
  result.show
}
