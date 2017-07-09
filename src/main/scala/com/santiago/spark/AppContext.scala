package com.santiago.spark

import java.io.{File, FileInputStream}

import org.apache.spark.{SparkConf, SparkContext}
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor

/**
  * @author Santiago Wang
  * @since 2017/7/9
  */
trait AppContext {
  val conf = new SparkConf().setAppName("spark_demo").setMaster("local")
  val sc = new SparkContext(conf)

  var appArgs: AppArgs = _

  val baseDir: String = System.getProperty("user.dir")
  val outputDir: String = baseDir + File.separator + "output"

  def loadConfig(confPath: String): Unit = {
    val yaml = new Yaml(new Constructor(classOf[AppArgs]))
    appArgs = yaml.load(new FileInputStream(confPath)).asInstanceOf[AppArgs]
  }
}
