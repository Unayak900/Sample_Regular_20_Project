package org.allaboutscala.application

import org.apache.spark.sql.functions.explode

object Tutorial_04_Explode extends App with Context{
import sparksession.sqlContext.implicits._

sparksession.sparkContext.setLogLevel("WARN")
  val dfTag = sparksession.read
    .option("multiLine",true)
    .option("inferSchema", true)
    .json("file:///home/hduser/PRAC/tags_sample.json")

  dfTag.show(false)

  val df = dfTag.select(explode($"stackoverflow") as "stackoverflow_tags")

  df.printSchema()

val DFnew =  df.select(
    $"stackoverflow_tags.tag.id" as "id",
    $"stackoverflow_tags.tag.author" as "author",
    $"stackoverflow_tags.tag.name" as   "tag_name",
    $"stackoverflow_tags.tag.frameworks.id" as "frameworks_id",
    $"stackoverflow_tags.tag.frameworks.name" as "frameworks_name"
    )
  DFnew.show(false)

val DFnew2 = DFnew.withColumn( "frameworks_id1", explode($"frameworks_id") as "frameworks_id")
    DFnew2.show(false)
val DFnew3 = DFnew2.select($"id",$"author",$"tag_name",$"frameworks_id1",explode($"frameworks_name") as "frameworks_name" ).distinct().
  sort("frameworks_id1")
 DFnew3.show(false)

   //.withColumn("frameworks_id", $"frameworks_id.frameworks_id1")

   //.withColumn("frameworks_id",$"frameworks_id.frameworks_id2")

 // jsonDFnew.show(false)

  }


