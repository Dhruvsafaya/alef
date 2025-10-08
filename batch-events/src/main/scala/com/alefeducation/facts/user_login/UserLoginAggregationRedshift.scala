package com.alefeducation.facts.user_login

import com.alefeducation.base.SparkBatchService
import com.alefeducation.service.DataSink
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Constants.UserLogin
import com.alefeducation.util.Helpers._
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.SparkSession

class UserLoginAggregationRedshift(val session: SparkSession,
                                  val service: SparkBatchService) {
  def transform(): Option[DataSink] = {
    val dataFrame = service.readOptional(ParquetUserLoginTransformedSource, session)
    dataFrame.map(_.toRedshiftInsertSink(RedshiftUserLoginSink, UserLogin))
  }
}

object UserLoginAggregationRedshift {
  val session = SparkSessionUtils.getSession(UserLoginAggregationRedshiftService)
  val service = new SparkBatchService(UserLoginAggregationRedshiftService, session)
  def main(args: Array[String]): Unit = {
    val transformer = new UserLoginAggregationRedshift(session, service)
    service.run(transformer.transform())
  }
}
