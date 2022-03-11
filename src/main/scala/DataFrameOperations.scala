package org.allaboutscala.application



object DataFrameOperations extends App with Context {

  case class Tag(id: Int, tag: String) // always declare the class here to initialize or else encoder error is oocured

  case class Question(owner_userid: Int, tag: String, creationDate: java.sql.Timestamp, score: Int)

  /*def main(args: Array[String]): Unit = {
    val spark = SparkSession.
      builder().master("local[2]").appName("DataFrameOperations").getOrCreate()*/

    val dftags = sparksession
      .read
      .option("header", true)
      .option("inferSchema", true)
      .csv("file:///home/hduser/PRAC/question_tags_10K.csv")
      .toDF("id", "tag")

    dftags.show(3, false)

    val dfques = sparksession
      .read.option("header", "true").option("inferSchema", "true")
      .option("dataFormat", "yyyy-mm-dd HH:MM:SS")
      .csv("file:///home/hduser/PRAC/questions_10K.csv")
      .toDF("id", "creation_date", "closed_date", "deletion_date", "score", "owner_userid", "answer_count")

    dfques.show(10, false)

    val dfQuestions = dfques
      .filter("score>400 and score < 410")
      .join(dftags, "id")
      .select("owner_userid", "tag", "creation_date", "score")
    dfQuestions.show(10, false)

    //Convert dataframe row to scala case class
    import sparksession.implicits._
    val dftagsoftag =
      dftags.as[Tag]
    dftagsoftag.take(10).foreach(t => println(s"id = ${t.id}, tag = ${t.tag}"))

    //Dataframe row to scala case class using map()

     /*def toQuestion(row: org.apache.spark.sql.Row):Question = {

      val IntOf: String => Option[Int] = _ match {
      case s if s == "NA" => None
      case s => Some(s.toInt)
      }

      import java.time._
      val DateOf: String => java.sql.Timestamp = _ match {
      case s => java.sql.Timestamp.valueOf(ZonedDateTime.parse(s).toLocalDateTime)
                        }

      Question(
        owner_userid = IntOf(row.getString(0)).getOrElse(-1),
        tag = row.getString(1),
        creationDate = DateOf(row.getString(2)),
        score = row.getString(3).toInt
              )
    }

    import spark.implicits._
    val dfofQuestion = dfQuestions.map(row=>toQuestion(row))
    dfofQuestion.take(10)
      .foreach(q => println(s"owner userid = ${q.owner_userid},tag = ${q.tag}, " +
        s"creation date = ${q.creationDate},score = ${q.score}")) */
    val seqTags = Seq(
      1 -> "So_java",
      1 -> "So_jsp",
      3 -> "So_Enlarg"
    )

    import sparksession.implicits._
    val dfMoreTags = seqTags.toDF("id","tag")
    dfMoreTags.show(10)

    val dfunionofTags = dftags
      .union(dfMoreTags)
      .filter("id in (1,3)")
     dfunionofTags.show(10)

    val dfintersectionTags = dfMoreTags
      .intersect(dfunionofTags)
      .show(10)

    import org.apache.spark.sql.functions._
    val dfsplitcolumns = dfMoreTags
      .withColumn("tmp",split($"tag","_"))
      .select($"id",
        $"tag",
        $"tmp".getItem(0).as("So_Prefix"),
        $"tmp".getItem(1).as("So_tag")
    ).drop("tmp")

    dfsplitcolumns.show(10)




      }

  //}
