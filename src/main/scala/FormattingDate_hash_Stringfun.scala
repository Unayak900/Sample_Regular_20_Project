package org.allaboutscala.application

object FormattingDate_hash_Stringfun extends  App with Context {

  import org.apache.spark.sql.functions._

  val donuts = Seq(("Plain Donut",1.5,"2018-04-17"),
    ("vanilla Donut",2.0,"2018-04-01"),
    ("glazed donut",2.5,"2018-04-02"))

  val df1 = sparksession.createDataFrame(donuts).toDF("Donutsname", "Price","Purchase Date")

  df1.printSchema()
  df1.show(false)

  import sparksession.sqlContext.implicits._

  df1.
    withColumn("price formatted",format_number($"Price",2))
    .withColumn("Name Formatted",format_string("Awesome %s",$"Donutsname"))
    .withColumn("name upper case",upper($"Donutsname"))
    .withColumn("name lower case",lower($"Donutsname"))
    .withColumn("Date Formatted",date_format($"Purchase Date","yyyyMMdd"))
    .withColumn("Day",dayofmonth($"Purchase Date"))
    .withColumn("Month",month($"Purchase Date"))
    .withColumn("Year",year($"Purchase Date"))
    .show(false)

//hash
  df1.
    withColumn("Hash",hash($"Donutsname"))
    .withColumn("MD5",md5($"Donutsname"))
    .withColumn("SHA1",sha1($"Donutsname"))
    .withColumn("SHA2",sha2($"Donutsname",256))
    .show(false)

//String operations

  df1.
    withColumn("Contains plain",instr($"Donutsname","donut"))
    .withColumn("length",length($"Donutsname"))
    .withColumn("Trim",trim($"Donutsname"))
    .withColumn("Ltrim",ltrim($"Donutsname"))
    .withColumn("Rtrim",rtrim($"Donutsname"))
    .withColumn("Reverse",reverse($"Donutsname"))
    .withColumn("Substring",substring($"Donutsname",0,5))
    .withColumn("isnull",isnull($"Donutsname"))
    .withColumn("Concat",concat_ws("-",$"Donutsname",$"Price"))
    .withColumn("InitCap",initcap($"Donutsname"))
    .show(false)


}
