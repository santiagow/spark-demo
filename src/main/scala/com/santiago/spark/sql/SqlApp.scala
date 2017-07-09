package com.santiago.spark.sql

import java.security.InvalidParameterException

import com.santiago.spark.AppContext
import org.apache.spark.sql._

object SqlApp extends AppContext {
  // spark 2.1.0
  val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
  val sqlContext: SQLContext = spark.sqlContext
  // spark 1.6.3
  // val sqlContext: SQLContext = new SQLContext(sc)

  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      throw new InvalidParameterException("please specify a config file")
    }

    val confPath = args(0)
    loadConfig(confPath)

    if (System.getProperty("hadoop.home.dir") == null) {
      System.setProperty("hadoop.home.dir", outputDir)
    }

    //mysql()
    json()
  }

  def mysql(): Unit = {
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

  def getJdbcUrl(): String = {
    "jdbc:mysql://" + appArgs.db.host + ":" + appArgs.db.port + "/" +
      appArgs.db.dbName + "?user=" + appArgs.db.user + "&password=" + appArgs.db.password
  }

  def json(): Unit = {
    val classloader:ClassLoader = Thread.currentThread().getContextClassLoader()
    val df = spark.read.json(classloader.getResource("tsdb_datapoints.json").getPath)

    // Displays the content of the DataFrame to stdout
    df.show()

    // inline json output to this:
    //+--------------------+--------------------+----------+-----+
    //|              metric|                tags| timestamp|value|
    //+--------------------+--------------------+----------+-----+
    //|cubrid.host.cpu.i...|[dev-132,wangxi,BTS]|1435548165| 93.0|
    //|cubrid.host.cpu.used|[dev-132,wangxi,BTS]|1435548165| 62.5|
    //+--------------------+--------------------+----------+-----+
  }
}
