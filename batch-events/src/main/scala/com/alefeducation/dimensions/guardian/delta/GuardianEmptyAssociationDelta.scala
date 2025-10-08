package com.alefeducation.dimensions.guardian.delta

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.batch.delta.Alias
import com.alefeducation.dimensions.guardian.transform.GuardianLatestAssociationTransform.GuardianLatestAssociationTransformSink
import com.alefeducation.util.Helpers.{DeltaGuardianSink, GuardianEntity}
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.desc

object GuardianEmptyAssociationDelta {
  import com.alefeducation.bigdata.batch.delta.Delta.Transformations._

  val serviceName = "delta-guardian-empty-association"
  val sourceName: String = GuardianLatestAssociationTransformSink
  val sinkName: String = DeltaGuardianSink

  val session: SparkSession = SparkSessionUtils.getSession(serviceName)
  val service = new SparkBatchService(serviceName, session)

  def main(args: Array[String]): Unit = {
    val latestAssociations = service.readOptional(sourceName, session)

    val deltaMatchConditions = s"${Alias.Delta}.guardian_id = ${Alias.Events}.guardian_id" +
      s" and ${Alias.Delta}.guardian_active_until is null" +
      s" and ${Alias.Delta}.guardian_student_id is null"

    val updateEmptyGuardians = latestAssociations
      .flatMap(
        _.orderBy(desc("guardian_created_time"))
          .coalesce(1)
          .withColumnRenamed("student_id", "guardian_student_id")
          .toUpdate(
            matchConditions = deltaMatchConditions,
            updateColumns = List("guardian_active_until", "guardian_status")
          )
      )
      .map(_.toSink(sinkName, GuardianEntity))

    service.run(updateEmptyGuardians)
  }

}
