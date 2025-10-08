package com.alefeducation.dimensions.school.redshift

import com.alefeducation.base.SparkBatchService
import com.alefeducation.dimensions.school.transform.SchoolCreatedTransform.SchoolCreatedTransformSink
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Constants.AdminSchoolCreated
import com.alefeducation.util.Helpers.RedshiftSchoolSink
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.SparkSession

object SchoolCreatedRedshift {
  val serviceName = "redshift-school-created"
  val sourceName: String = SchoolCreatedTransformSink
  val sinkName: String = RedshiftSchoolSink

  val session: SparkSession = SparkSessionUtils.getSession(serviceName)
  val service = new SparkBatchService(serviceName, session)

  def main(args: Array[String]): Unit = {
    service.run {
      service
        .readOptional(sourceName, session)
        .map(
          _.toRedshiftInsertSink(sinkName, AdminSchoolCreated)
        )
    }
  }
}
