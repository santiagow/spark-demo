package com.santiago.spark

import java.io.{File, FileInputStream}
import java.security.InvalidParameterException
import java.util.regex.Pattern

import org.apache.spark.{SparkConf, SparkContext}
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor

import scala.beans.BeanProperty

object App extends AppContext {

  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      throw new InvalidParameterException("please specify a config file")
    }

    val confPath = args(0)
    loadConfig(confPath)

    if (System.getProperty("hadoop.home.dir") == null) {
      System.setProperty("hadoop.home.dir", outputDir)
    }

    wordCount()
    piEstimat()
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

}

trait AppContext {
  val conf = new SparkConf().setAppName("spark_demo").setMaster("local")
  val sc = new SparkContext(conf)

  val baseDir: String = System.getProperty("user.dir")
  var appArgs: AppArgs = _
  val outputDir: String = baseDir + File.separator + "output"

  def loadConfig(confPath: String): Unit = {
    val yaml = new Yaml(new Constructor(classOf[AppArgs]))
    appArgs = yaml.load(new FileInputStream(confPath)).asInstanceOf[AppArgs]
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