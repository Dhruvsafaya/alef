package com.alefeducation.dimensions.staff

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.dimensions.staff.StaffRelUserTransform.StaffRelUserMapping
import com.alefeducation.dimensions.staff.StaffUserHelper.selectAdmin
import com.alefeducation.service.DataSink
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Resources.{getSink, getSource}
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

class StaffRelUserTransform(val session: SparkSession, val service: SparkBatchService, val serviceName: String) {

  def transform(): Option[Sink] = {
    val sourceName = getSource(serviceName).head
    val sinkName = getSink(serviceName).head
    val sourceStaffDf = service.readUniqueOptional(sourceName, session, uniqueColNames = Seq("uuid"))

    val transformed = sourceStaffDf.flatMap(selectAdmin).map(_.selectLatestByRowNumber(List("uuid")))

    transformed
      .map(
        _.withColumn("user_id", col("uuid"))
          .transformForInsertDwIdMapping("user", "ADMIN")
          .transform(_.selectColumnsWithMapping(StaffRelUserMapping))
      )
      .map(DataSink(sinkName, _))
  }
}

object StaffRelUserTransform {

  val StaffRelUserTransformService = "staff-rel-user-transform"

  val StaffRelUserMapping: Map[String, String] = Map(
    "user_id" -> "user_id",
    "user_type" -> "user_type",
    "user_created_time" -> "user_created_time",
    "user_dw_created_time" -> "user_dw_created_time"
  )

  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSessionUtils.getSession(StaffRelUserTransformService)
    val service = new SparkBatchService(StaffRelUserTransformService, session)

    val transformer = new StaffRelUserTransform(session, service, StaffRelUserTransformService)
    service.run(transformer.transform())
  }
}
