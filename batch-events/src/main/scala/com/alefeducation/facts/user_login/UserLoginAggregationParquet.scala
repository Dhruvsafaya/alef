package com.alefeducation.facts.user_login

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Helpers._
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.SparkSession

class UserLoginAggregationParquet(val session: SparkSession,
                                  val service: SparkBatchService) {
  def transform(): Option[Sink] = {
    val df = service.readOptional(ParquetUserLoginSource, session)
    df.map(_.toParquetSink(ParquetUserLoginSink))
  }
}

object UserLoginAggregationParquet {
  val session = SparkSessionUtils.getSession(UserLoginAggregationParquetService)
  val service = new SparkBatchService(UserLoginAggregationParquetService, session)
  def main(args: Array[String]): Unit = {
    val transformer = new UserLoginAggregationParquet(session, service)
    service.run(transformer.transform())
  }
}
