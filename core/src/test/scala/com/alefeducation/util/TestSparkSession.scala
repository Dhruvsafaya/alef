package com.alefeducation.util

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object TestSparkSession {
  def getSparkSession: SparkSession = {
    var session: SparkSession = null
    session match {
      case null =>
        val conf = new SparkConf()
          .set("spark.sql.session.timeZone", "UTC")
          .set("spark.sql.legacy.sessionInitWithConfigDefaults", "true")
        session = SparkSession
          .builder()
          .master("local[*]").config(conf)
          .getOrCreate()
        session.sparkContext.setLogLevel("ERROR")
        session
      case _ => session
    }
  }
}
