package com.alefeducation.facts.user_login

import com.alefeducation.base.SparkBatchService
import com.alefeducation.service.DataSink
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.DataFrameUtility.{containsAny, _}
import com.alefeducation.util.Helpers.{DefaultStringValue, StagingUserLogin, _}
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{SparkSession, functions}

class UserLoginAggregationTransform(val session: SparkSession,
                                    val service: SparkBatchService) {
  import session.implicits._

  def transform(): Option[DataSink] = {
    //TODO remove this check. Introduced for work around to include only these
    // roles as events for other roles are not handled
    val acceptedRolesForLogin = List("GUARDIAN", "TEACHER", "STUDENT", "TDC", "PRINCIPAL")

    // TODO("functions.explode will not work with multi-school. Come up with a long term solution.")
    val dataFrame = service.readOptional(ParquetUserLoginSource, session)
    val loginActivities = dataFrame.flatMap(
      _.filter(containsAny(acceptedRolesForLogin)($"roles")).checkEmptyDf
        .map(
          _.select(
            $"uuid",
            $"tenantId",
            functions.explode($"schools.uuid").as("schoolId"),
            $"roles",
            $"outsideOfSchool",
            $"eventDateDw",
            $"occurredOn",
            $"eventType"
          ).withColumn("role", $"roles".getItem(0)) //TODO work on role seperation
        ))

    val userLoginWithTimeStamp = loginActivities.map(
      _.transformIfNotEmpty(
        _.withColumn("id", lit(DefaultStringValue))
          .withColumn("loginTime", $"occurredOn")
      ).transformForInsertFact(StagingUserLogin, FactUserLoginEntity))

    userLoginWithTimeStamp.map(df => {
      DataSink(ParquetUserLoginTransformedSink, df)
    })
  }

}

object UserLoginAggregationTransform {
  val session = SparkSessionUtils.getSession(UserLoginAggregationTransformService)
  val service = new SparkBatchService(UserLoginAggregationTransformService, session)
  def main(args: Array[String]): Unit = {
    val transformer = new UserLoginAggregationTransform(session,service)
    service.run(transformer.transform())
  }
}
