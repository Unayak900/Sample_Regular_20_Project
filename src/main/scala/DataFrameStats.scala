package org.allaboutscala.application

import org.apache.spark.sql.functions._


object DataFrameStats extends App with Context {

  sparksession.sparkContext.setLogLevel("WARN") //this off the info messages while run.

  val dfTags = sparksession
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("file:///home/hduser/PRAC/question_tags_10K.csv")
    .toDF("id","tag")

  dfTags.show(5,false)

  val dfQuestionCSV = sparksession
    .read.option("header","true")
    .option("inferSchema", "true")
    .csv("file:///home/hduser/PRAC/questions_10K.csv")
    .toDF("id","creation_date","closed_date","deletion_date","score","owner_userid","answer_count")


  val dfquestions = dfQuestionCSV.select(
    dfQuestionCSV.col("id").cast("integer"),
    dfQuestionCSV.col("creation_date").cast("timestamp"),
    dfQuestionCSV.col("closed_date").cast("timestamp"),
    dfQuestionCSV.col("deletion_date").cast("timestamp"),
    dfQuestionCSV.col("score").cast("integer"),
    dfQuestionCSV.col("owner_userid").cast("integer"),
    dfQuestionCSV.col("answer_count").cast("integer")
  )
dfquestions.printSchema()

  dfQuestionCSV.show(4,false)

println("*******************Average Score*************")
  dfquestions.select(avg("score")).show()

println("*******************Max Score*************")
dfquestions.select(max("score")).show()

println("************Min Score**********")
  dfquestions.select(min("score")).show()

  println("***********Mean***************")
  dfquestions.select(mean("score")).show()

  println("**************Statistics***********")
    dfquestions.
    filter("id > 400 and id < 450")
    .filter("owner_userid is not null")
    .join(dfTags,dfquestions.col("id").equalTo(dfTags("id")))
    .groupBy(dfquestions.col("owner_userid"))
    .agg(avg("score"),max("answer_count"))
    .show()

  println("**********Describe****************")
val dfQuestionsStats = dfquestions.describe()
  dfQuestionsStats.show()

  val correlation = dfquestions.stat.corr("score","answer_count")
  println(s"correlation between column score and answer_count = $correlation ")

  val covariance = dfquestions.stat.cov("score","answer_count")
  println(s"covariance between column score and answer_count =$covariance ")


  println("**************FrequentItems********************")
  val dfrequentScore = dfquestions.stat.freqItems(Seq("answer_count"))
dfrequentScore.show(false)

  println("*******************Crosstab***************")
    val dfscorebyuserid = dfquestions
    .filter("owner_userid> 0 and owner_userid < 20")
    .stat
    .crosstab("score","owner_userid")
  dfscorebyuserid.show(10, false)

//find all rows where answer_count in (5,10,20)
  val dfquestionsbyAnswerCount = dfquestions
    .filter("owner_userid > 0")
    .filter("answer_count in (5,10,20)")
dfquestionsbyAnswerCount.show(false)

//count how many rows match
  dfquestionsbyAnswerCount
    .groupBy("answer_count")
    .count()
    .show(false)

  println("***********Fractionby map key********************")

  val fractionKeyMap = Map(5-> 0.5, 10-> 0.1,20-> 1.0)

  dfquestionsbyAnswerCount
    .stat
    .sampleBy("answer_count",fractionKeyMap,7l)
    .groupBy("answer_count")
    .count()
    .show()

  println("*****Seed to 37***************")
dfquestionsbyAnswerCount
  .stat
  .sampleBy("answer_count",fractionKeyMap,37)
  .groupBy("answer_count")
  .count()
  .show()

  //Approximate Quantile
  val qunatiles = dfquestions
    .stat
    .approxQuantile("score",Array(0,0.5,1),0.25)
  println(s"Quantiles segments = ${qunatiles.toSeq}")

  //check quantiles using spark sql

  println("*********Spark SQL for quantiles************")
  dfquestions.createTempView("so_questions")
  sparksession
    .sql("select min(score),percentile_approx(score,0.25),max(score) from so_questions")
    .show()

  println("***********BloomFilter***************")
  val tagsBloomfilter = dfTags.stat.bloomFilter("tag",1000L,0.1)
  println(s"bloom filter contains java tag = ${tagsBloomfilter.mightContain("java")}")
  println(s"bloom filter contains some unknown tag = ${tagsBloomfilter.mightContain("unknown tag")}")

  println("***********Count min sketch*************")
  val cmstag = dfTags.stat.countMinSketch("tag",0.1,0.9,37)
  val estimatedFrequency = cmstag.estimateCount("java")
  println(s"estimated frequency for java tag = $estimatedFrequency")

  println("*******************Sampling with Replacement******************")
  val dfTagsSample = dfTags.sample(true,0.2,37l)
  println(s"number of rows in sample dftagsSample = ${dfTagsSample.count()}")
  println(s"number of rows in dftags = ${dfTags.count()}")



}
