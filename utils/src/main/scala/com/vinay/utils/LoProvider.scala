package com.vinay.utils

import org.apache.log4j.{Level,Logger}
/**
  * Created by ADMIN on 27-08-18.
  */
trait LoProvider {


  val log:Logger = Logger.getLogger(this.getClass.getCanonicalName)
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.apache.spark.storage.BlockManager").setLevel(Level.ERROR)
  log.setLevel(Level.INFO)
}
