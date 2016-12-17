package com.santiago.spark

import java.util.regex.Pattern

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}

object App {
  val conf = new SparkConf().setAppName("spark_demo").setMaster("local")
  val sc = new SparkContext(conf)

  val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
  val sqlContext: SQLContext = spark.sqlContext

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "E:\\logs")

    //wordCount()
    //piEstimat()
    sql()
  }

  def wordCount(): Unit = {
    val logFile = "E:\\project_reference\\SparkDemo\\build.gradle"
    val textFile = sc.textFile(logFile)
    val regEx = "^[_a-zA-Z]\\w*"
    val pattern = Pattern.compile(regEx)
    val counts = textFile.flatMap(line => line.split("\\W"))
      .filter(word => {
        val matcher = pattern.matcher(word)
        matcher.find()
      })
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    println("the word count: " + counts.collect().length)
    counts.collect().foreach(a => {
      println(a._1 + ": " + a._2)
    })
    //counts.saveAsTextFile("E:\\project_reference\\SparkDemo\\output")
  }

  def piEstimat(): Unit = {
    val NUM_SAMPLES: Int = Int.MaxValue
    val count = sc.parallelize(1 to NUM_SAMPLES).map { i =>
      val x = Math.random()
      val y = Math.random()
      if (x * x + y * y < 1) 1 else 0
    }.reduce(_ + _)
    println("Pi is roughly " + 4.0 * count / NUM_SAMPLES)
  }

  def sql(): Unit = {
    val url = "jdbc:mysql://localhost:3306/sampdb?user=test&password=test"
    val df = sqlContext
      .read
      .format("jdbc")
      .option("url", url)
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "member")
      .load()

    // Looks the schema of this DataFrame.
    df.printSchema()

    // Counts people by age
    val countsByAge = df.groupBy("state").count()
    countsByAge.show()

    // Saves countsByAge to S3 in the JSON format.
    //val dateFrame = countsByAge.write.format("json").save("s3a://...")
  }

}