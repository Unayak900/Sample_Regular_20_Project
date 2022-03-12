package org.allaboutscala.application

object Tutorial_10_DFudf extends App with Context {
import org.apache.spark.sql.functions._
import sparksession.sqlContext.implicits._
  sparksession.sparkContext.setLogLevel("WARN")
  val donuts = Seq(("Plain Donut",1.5),("vanilla",2.5),("strawberry",3.5),("Roseberry",0.3))
  val df1 = sparksession.createDataFrame(donuts).toDF("Donuts","price")

  df1.printSchema()
  df1.show()

  def stockmin:(String => Seq[Int]) = (Donuts:String) => Donuts match {
    case "Plain Donut" => Seq(100,500)
    case "vanilla" => Seq(200,400)
    case "strawberry" => Seq(300,600)
    case _ => Seq(150,150)
  }

  val stockminudf = udf(stockmin)
  val df2 = df1.withColumn("Stock min max", stockminudf($"Donuts"))
  df2.show(false)



}
