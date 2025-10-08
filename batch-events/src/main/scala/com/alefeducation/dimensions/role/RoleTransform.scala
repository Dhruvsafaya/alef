package com.alefeducation.dimensions.role

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.dimensions.role.RoleTransform.{RoleColMapping, RoleEntity}
import com.alefeducation.service.DataSink
import com.alefeducation.util.Constants.{RoleCreatedEvent, RoleDeletedEvent, RoleUpdatedEvent, Undefined}
import com.alefeducation.util.Helpers.{ActiveEnabled, Deleted, EventType}
import com.alefeducation.util.Resources.{getSink, getSource}
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.alefeducation.util.BatchTransformerUtility._

class RoleTransform(val session: SparkSession, val service: SparkBatchService, val serviceName: String) {

  def transform(): Option[Sink] = {
    val sourceName = getSource(serviceName).head
    val sinkName = getSink(serviceName).head
    val sourceDf = service.readOptional(sourceName, session)


    val transformed = sourceDf.map(
      _.transform(addStatus)
        .transformForInsertDim(RoleColMapping, RoleEntity, List("id"))
        .drop(s"${RoleEntity}_deleted_time")
    )

    transformed.map(DataSink(sinkName, _))
  }

  def addStatus(df: DataFrame): DataFrame =
    df.withColumn(s"${RoleEntity}_status",
      when(col(EventType) === RoleCreatedEvent, lit(ActiveEnabled))
        .when(col(EventType) === RoleUpdatedEvent, lit(ActiveEnabled))
        .when(col(EventType) === RoleDeletedEvent, lit(Deleted))
        .otherwise(lit(Undefined)))
}

object RoleTransform {

  val RoleTransformService = "role-transform"

  val RoleEntity = "role"

  val RoleColMapping: Map[String, String] = Map(
    s"${RoleEntity}_status" -> s"${RoleEntity}_status",
    "id" -> s"${RoleEntity}_uuid",
    "role.name" -> s"${RoleEntity}_name",
    "role.organization.name" -> s"${RoleEntity}_organization_name",
    "role.organization.code" -> s"${RoleEntity}_organization_code",
    "role.roleType" -> s"${RoleEntity}_type",
    "role.categoryId" -> s"${RoleEntity}_category_id",
    "role.isCCLRole" -> s"${RoleEntity}_is_ccl",
    "role.description" -> s"${RoleEntity}_description",
    "role.predefined" -> s"${RoleEntity}_predefined",
    "occurredOn" -> "occurredOn"
  )

  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSessionUtils.getSession(RoleTransformService)
    val service = new SparkBatchService(RoleTransformService, session)

    val roleService = new RoleTransform(session, service, RoleTransformService)
    service.run(roleService.transform())
  }
}
