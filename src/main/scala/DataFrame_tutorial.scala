package org.allaboutscala.application

object DataFrame_tutorial extends App with Context {

  sparksession.sparkContext.setLogLevel("WARN")
  val dfTags = sparksession
    .read
    .option("header","true")
    .option("inferSchema", true)
    .csv("file:///home/hduser/PRAC/question_tags_10K.csv")
    .toDF("id", "tag")

dfTags.printSchema()


dfTags.select("id","tag").show(10)
dfTags.filter("tag=='php'").show(10)
println(s"number of php tags = ${dfTags.filter("tag=='php'").count()}")

dfTags.filter("tag like 's%'").show(10)
dfTags.filter("tag like 's%'")
  .filter("id == 25 or id == 108")
  .show(10)

dfTags.filter("tag like 's%'").show(10)
dfTags.filter("id in (25,108)").show(10)
dfTags.groupBy("tag").count().show(10)

dfTags.groupBy("tag").count().filter("count>5").orderBy("tag").show(10)


}
