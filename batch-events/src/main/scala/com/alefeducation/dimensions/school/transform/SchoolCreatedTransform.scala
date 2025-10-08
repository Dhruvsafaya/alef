package com.alefeducation.dimensions.school.transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.dimensions.school.transform.SchoolCreatedTransform.SchoolCreatedTransformSink
import com.alefeducation.dimensions.school.transform.SchoolOrganizationTransform.SchoolOrganizationTransformSink
import com.alefeducation.service.DataSink
import com.alefeducation.util.Constants.AdminSchoolCreated
import com.alefeducation.util.Helpers._
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.SparkSession

class SchoolCreatedTransform(val session: SparkSession, val service: SparkBatchService) {

  import com.alefeducation.util.BatchTransformerUtility._
  import session.implicits._

  def transform(): Option[DataSink] = {
    val schoolWithOrganization = service.readOptional(SchoolOrganizationTransformSink, session)

    val created = schoolWithOrganization.flatMap(
      _.filter($"eventType" === AdminSchoolCreated).checkEmptyDf
        .map(_.transformForInsertDim(SchoolDimensionCols, SchoolEntity, ids = List("uuid")))
    )

    created.map(df => { DataSink(SchoolCreatedTransformSink, df) })
  }
}

object SchoolCreatedTransform {
  val SchoolCreatedService = "transform-school-created"
  val SchoolCreatedTransformSink = "transformed-school-created-sink"

  val session: SparkSession = SparkSessionUtils.getSession(SchoolCreatedService)
  val service = new SparkBatchService(SchoolCreatedService, session)

  def main(args: Array[String]): Unit = {
    val transformer = new SchoolCreatedTransform(session, service)
    service.run(transformer.transform())
  }

}
