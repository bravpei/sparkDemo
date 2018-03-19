package moon.star.core

import org.apache.spark.{SparkConf, SparkContext}

object ParallelizeData extends App{
  val arr=Array(1,2,3,4,5)
  val sc=new SparkContext(new SparkConf().setAppName("ParallelizeData").setMaster("local[*]"))
  sc.parallelize(arr).sortBy(x=>x,false).collect().foreach(println)
}
