package com.alefeducation.facts.user_login

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.util.Helpers._
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.date_format

class UserLoginAggregationDelta(val session: SparkSession,
                                val service: SparkBatchService) {
  import com.alefeducation.bigdata.batch.delta.Delta.Transformations._
  import session.implicits._

  def transform(): Option[Sink] = {
    val dataFrame = service.readOptional(ParquetUserLoginTransformedSource, session)
    val deltaUserLogin = dataFrame.map(
      _.withColumnRenamed("tenant_uuid", "ful_tenant_id")
      .withColumnRenamed("user_uuid", "ful_user_id")
      .withColumnRenamed("role_uuid", "ful_role_id")
      .withColumnRenamed("school_uuid", "ful_school_id")
      .withColumn("ful_login_time", $"ful_created_time".cast("timestamp"))
      .withColumn("eventdate", date_format($"ful_created_time", "yyyy-MM-dd"))
    )
    deltaUserLogin.flatMap(_.toCreate(isFact = true).map(_.toSink(UserLoginDeltaSink)))
  }
}

object UserLoginAggregationDelta {
  val session = SparkSessionUtils.getSession(UserLoginAggregationDeltaService)
  val service = new SparkBatchService(UserLoginAggregationDeltaService, session)
  def main(args: Array[String]): Unit = {
    val transformer = new UserLoginAggregationDelta(session, service)
    service.run(transformer.transform())
  }
}
