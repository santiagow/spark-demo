package com.santiago.spark

import java.io.{File, FileInputStream}
import java.security.InvalidParameterException
import java.util.regex.Pattern

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor

import scala.beans.BeanProperty

object App {
  val conf = new SparkConf().setAppName("spark_demo").setMaster("local")
  val sc = new SparkContext(conf)

  val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
  val sqlContext: SQLContext = spark.sqlContext
  val baseDir: String = System.getProperty("user.dir")
  var appArgs: AppArgs = _
  val outputDir: String = baseDir + File.separator + "output"

  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      throw new InvalidParameterException("please specify a config file")
    }

    val confPath = args(0)
    loadConfig(confPath)

    if (System.getProperty("hadoop.home.dir") == null) {
      System.setProperty("hadoop.home.dir", outputDir)
    }

    //wordCount()
    //piEstimat()
    sql()
  }

  def loadConfig(confPath: String): Unit = {
    val yaml = new Yaml(new Constructor(classOf[AppArgs]))
    appArgs = yaml.load(new FileInputStream(confPath)).asInstanceOf[AppArgs]
  }

  def wordCount(): Unit = {
    val classloader:ClassLoader = Thread.currentThread().getContextClassLoader()
    val textFile = sc.textFile(classloader.getResource("concurrency_utilities_overview.md").getPath)
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

    //counts.saveAsTextFile(outputDir)
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
    val url = getJdbcUrl()
    val df = sqlContext
      .read
      .format("jdbc")
      .option("url", url)
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("dbtable", "member")
      .load()

    // Looks the schema of this DataFrame.
    df.printSchema()

    // Counts people by age
    val countsByState = df.groupBy("state").count()
    countsByState.show()

    // Saves countsByAge to S3 in the JSON format.
//    val dataFrame = countsByState.write.format("json").save(outputDir + File.separator + "count_by_state.json")
//    println(dataFrame)
  }

  def getJdbcUrl() : String = {
    "jdbc:mysql://" + appArgs.db.host + ":" + appArgs.db.port + "/" +
      appArgs.db.dbName + "?user=" + appArgs.db.user + "&password=" + appArgs.db.password
  }

}

class AppArgs {
  @BeanProperty
  var db: DbArgs = _
}

class DbArgs {
  @BeanProperty
  var host:String = _
  @BeanProperty
  var port = 3306
  @BeanProperty
  var dbName:String = _
  @BeanProperty
  var user:String = _
  @BeanProperty
  var password:String = _
}