package com.alefeducation.dimensions.course_additional_resourse_activity

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.util.BatchTransformerUtility.OptionalDataFrameTransformer
import com.alefeducation.util.Constants.ActiveState
import com.alefeducation.util.Resources._
import com.alefeducation.util.SparkSessionUtils
import com.alefeducation.util.Utils.{getEntryPrefix, isActiveUntilVersion}
import org.apache.spark.sql.SparkSession

class CourseAdditionalResourceActivityDelta (val session: SparkSession,
                              val service: SparkBatchService,
                              val serviceName: String) {
  import com.alefeducation.bigdata.batch.delta.Delta.Transformations._

  def write(): Option[Sink] = {
    val source = getSource(serviceName)
    val sink = getSink(serviceName).head
    val uniqueIds = getUniqueIds(serviceName)
    val entityPrefix = getEntryPrefix(serviceName, uniqueIds)
    val matchConditions = getNestedString(serviceName, "match-conditions")
    val inactiveStatus = getInt(serviceName, "inactive_status_value")
    val isActiveUntil = isActiveUntilVersion(serviceName)

    val additionalActivity = service.readOptional(source.head, session)
    val downloadActivity = service.readOptional(source.last, session)
    val sourceDf = additionalActivity.unionOptionalByNameWithEmptyCheck(downloadActivity, allowMissingColumns = true)

    sourceDf.flatMap(
      _.toIWH(
        matchConditions = matchConditions,
        uniqueIdColumns = uniqueIds,
        activeState = ActiveState,
        inactiveStatus = inactiveStatus)
        .map(_.toSink(sink, entityPrefix, isActiveUntilVersion = isActiveUntil))
    )
  }

}
object CourseAdditionalResourceActivityDelta {
  def main(args: Array[String]): Unit = {

    val serviceName = "delta-course-additional-resource-activity"

    val session = SparkSessionUtils.getSession(serviceName)
    val service = new SparkBatchService(serviceName, session)

    val writer = new CourseAdditionalResourceActivityDelta(session, service, serviceName)
    service.run(writer.write())
  }
}
