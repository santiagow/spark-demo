package com.santiago.spark.sql

import scala.beans.BeanProperty

/**
  * @author Santiago Wang
  * @since 2017/7/9
  */
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
