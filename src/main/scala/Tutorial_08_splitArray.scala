package org.allaboutscala.application

object Tutorial_08_splitArray extends App with Context {
import sparksession.sqlContext.implicits._

  val targets = Seq(("plain Donut", Array(1.5,2.0)),
    ("vanilla Donut", Array(2.0,2.5)),
    ("Strawberry Donut",Array(2.5,3.5)))
val df1 = sparksession.createDataFrame(targets).toDF("Name","Prices")

  df1.printSchema()

  df1.show(false)

 val df2 = df1.select(
   $"Name", $"Prices"(0).as("Low price"),$"Prices"(1).as("High Price")
  )

df2.show(false)

  //Renamed column

  val df3 = df2.withColumnRenamed("Low price", "Values")
  df3.show()





  //df1.show()


}
