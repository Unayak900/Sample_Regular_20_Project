package org.allaboutscala.application

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait Context {
  lazy val sparkconf = new SparkConf()
    .setAppName("learn Spark")
    .setMaster("local[2]")
    .set("spark.cores.max","2")

  lazy val sparksession = SparkSession
    .builder()
    .config(sparkconf)
    .getOrCreate()

}
