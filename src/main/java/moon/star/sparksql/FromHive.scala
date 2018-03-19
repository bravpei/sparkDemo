package moon.star.sparksql

import org.apache.spark.sql.SparkSession


object FromHive extends App{
  val spark=SparkSession.builder().appName("FromHive")
    .config("spark.sql.warehouse.dir","hdfs://172.18.130.100/apps/hive/warehouse")
    .enableHiveSupport()
    .getOrCreate()
  import spark.sql
  import spark.implicits._
  sql("select * from mydatabase.linux_login_log_19").show()
}
//./spark-submit --class moon.star.sparksql.FromHive --master yarn /opt/test/sparkDemo.jar