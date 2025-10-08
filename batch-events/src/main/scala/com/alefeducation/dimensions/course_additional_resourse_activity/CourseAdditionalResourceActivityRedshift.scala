package com.alefeducation.dimensions.course_additional_resourse_activity

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.dimensions.academic_year.AcademicYearCreatedRedshift.{ColumnsToInsert, ColumnsToUpdate, TargetTableName, toMapCols}
import com.alefeducation.dimensions.academic_year.AcademicYearRollOverRedshift.targetTableName
import com.alefeducation.io.data.RedshiftIO.TempTableAlias
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Resources.{getBool, getInt, getNestedString, getNestedStringIfPresent, getSink, getSource, getUniqueIds}
import com.alefeducation.util.SparkSessionUtils
import com.alefeducation.util.Utils.{getEntryPrefix, isActiveUntilVersion}
import org.apache.spark.sql.SparkSession

class CourseAdditionalResourceActivityRedshift(val session: SparkSession,
                                val service: SparkBatchService,
                                val serviceName: String
                               ) {

  def write(): Option[Sink] = {
    val source = getSource(serviceName)
    val sink = getSink(serviceName).head
    val uniqueIds = getUniqueIds(serviceName)
    val inactiveStatusValue = getInt(serviceName, "inactive_status_value")
    val isStagingSink = getBool(serviceName, "is_staging_sink")

    val additionalActivity = service.readOptional(source.head, session)
    val downloadActivity = service.readOptional(source.last, session)
    val df = additionalActivity.unionOptionalByNameWithEmptyCheck(downloadActivity, allowMissingColumns = true)

    val entityPrefix = getEntryPrefix(serviceName, uniqueIds)

    df.map(
      _.toRedshiftIWHSink(
        sink,
        entityPrefix,
        ids = uniqueIds,
        isStagingSink = isStagingSink,
        inactiveStatus = inactiveStatusValue
      )
    )
  }
}

object CourseAdditionalResourceActivityRedshift {

  def main(args: Array[String]): Unit = {

    val serviceName = "redshift-course-additional-resource-activity"

    val session = SparkSessionUtils.getSession(serviceName)
    val service = new SparkBatchService(serviceName, session)

    val writer = new CourseAdditionalResourceActivityRedshift(session, service, serviceName)
    service.run(writer.write())
  }
}
