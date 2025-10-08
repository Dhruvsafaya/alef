package com.alefeducation.dimensions.school.delta

import com.alefeducation.base.SparkBatchService
import com.alefeducation.dimensions.school.transform.SchoolCreatedTransform.SchoolCreatedTransformSink
import com.alefeducation.util.Helpers.DeltaSchoolSink
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.SparkSession

object SchoolCreatedDelta {

  import com.alefeducation.bigdata.batch.delta.Delta.Transformations._

  val serviceName = "delta-school-created"
  val sourceName: String = SchoolCreatedTransformSink
  val sinkName: String = DeltaSchoolSink

  val session: SparkSession = SparkSessionUtils.getSession(serviceName)
  val service = new SparkBatchService(serviceName, session)

  def main(args: Array[String]): Unit = {
    service.run {
      service
        .readOptional(sourceName, session)
        .flatMap(
          _.drop("school_content_repository_dw_id", "school_organization_dw_id", "_is_complete")
            .toCreate()
            .map(_.toSink(sinkName))
        )

    }
  }
}
