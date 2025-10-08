package com.alefeducation.dimensions.guardian.redshift

import com.alefeducation.base.SparkBatchService
import com.alefeducation.dimensions.guardian.transform.GuardianRegistrationCommonTransform.GuardianRegistrationCommonTransformSink
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Constants.GuardianUserType
import com.alefeducation.util.Helpers.RedshiftUserSink
import com.alefeducation.util.SparkSessionUtils
import com.alefeducation.util.redshift.options.UpsertOptions
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.TimestampType

object GuardianUserRedshift {
  val serviceName = "redshift-guardian-user"
  val sourceName: String = GuardianRegistrationCommonTransformSink
  val sinkName = "redshift-user-rel"

  val session: SparkSession = SparkSessionUtils.getSession(serviceName)
  val service = new SparkBatchService(serviceName, session)

  import session.implicits._

  def main(args: Array[String]): Unit = {
    val registeredCommon = service.readOptional(sourceName, session)

    val userRedshiftSink = registeredCommon.map {
      _.withColumnRenamed("guardianId", "user_id")
        .select($"user_id", $"occurredOn".cast(TimestampType))
        .transformForInsertDwIdMapping("user", GuardianUserType)
        .toRedshiftUpsertSink(
          RedshiftUserSink,
          UpsertOptions.RelUser.targetTableName,
          matchConditions = UpsertOptions.RelUser.matchConditions(GuardianUserType),
          columnsToUpdate = UpsertOptions.RelUser.columnsToUpdate,
          columnsToInsert = UpsertOptions.RelUser.columnsToInsert,
          isStaging = true
        )
    }

    service.run(userRedshiftSink)
  }

}
