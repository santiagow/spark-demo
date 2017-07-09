package com.santiago.spark

import com.santiago.spark.sql.DbArgs

import scala.beans.BeanProperty

/**
  * @author Santiago Wang
  * @since 2017/7/9
  */
class AppArgs {
  @BeanProperty
  var db: DbArgs = _
}
