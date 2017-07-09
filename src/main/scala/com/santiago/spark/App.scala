package com.santiago.spark

import java.util.regex.Pattern

/**
  *
  */
object App extends AppContext {

  def main(args: Array[String]): Unit = {
    /*if (args.length == 0) {
      throw new InvalidParameterException("please specify a config file")
    }

    val confPath = args(0)
    loadConfig(confPath)*/

    if (System.getProperty("hadoop.home.dir") == null) {
      System.setProperty("hadoop.home.dir", outputDir)
    }

    wordCount()
//    piEstimat()
  }

  def wordCount(): Unit = {
    val classloader: ClassLoader = Thread.currentThread().getContextClassLoader()
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



