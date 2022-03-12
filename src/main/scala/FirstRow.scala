package org.allaboutscala.application

object FirstRow extends App with Context {


  sparksession.sparkContext.setLogLevel("WARN")
  val donuts = Seq(("Plain Donut",2.5),("Glazed Donut",3.5),("vanilla Donut",4.5))
  val df1 = sparksession.createDataFrame(donuts).toDF("Donuts","Prices")

  df1.printSchema()
  df1.show(false)

  val firstrow = df1.first()
  println(s"First Row = $firstrow")

  val firstrowCol1 = df1.first().get(0)
  println(s"First row col1 = $firstrowCol1")

  val firstrowcolprice = df1.first().getAs[Double]("Prices")
  println(s"First row column = $firstrowcolprice")


}
