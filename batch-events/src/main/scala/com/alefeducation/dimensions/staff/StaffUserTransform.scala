package com.alefeducation.dimensions.staff

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.dimensions.staff.StaffUserHelper._
import com.alefeducation.schema.internal.ControlTableUtils.ProductMaxIdType
import com.alefeducation.service.DataSink
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Constants._
import com.alefeducation.util.Resources.{getNestedString, getSink}
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.types.{BooleanType, StringType}
import org.apache.spark.sql.{DataFrame, SparkSession}

class StaffUserTransform(val session: SparkSession, val service: SparkBatchService, val serviceName: String) {

  val uuid: String = "uuid"
  val occurredOn: String = "occurredOn"
  val eventType: String = "eventType"

  val uniqueKey: List[String] = List(uuid)
  val orderBy: String = occurredOn

  def transform(): Option[Sink] = {
    val staffSourceName = getNestedString(serviceName, StaffSourceName)
    val staffDeletedSourceName = getNestedString(serviceName, StaffDeletedSourceName)
    val sinkName = getSink(serviceName).head

    val staffDf = service.readOptional(staffSourceName, session, extraProps = ExtraProps)
      .flatMap(selectAdmin).map(_.filter(col(eventType) =!= UserEnabled && col(eventType) =!= UserDisabled)
        .selectLatestByRowNumber(List(uuid, occurredOn, eventType)))
    val staffDeletedDf = service
      .readOptional(staffDeletedSourceName, session, extraProps = ExtraProps)
      .map(addDefaultCols)

    val combinedStaffDf = combine(staffDf, staffDeletedDf)

    val startId = service.getStartIdUpdateStatus(StaffUserIdKey)

    val transformed = combinedStaffDf.map(
      _.transform(addEnableForDelete)
        .transformForSCDTypeII(
          StaffUserMapping,
          StaffUserEntity,
          uniqueKey,
          orderBy,
          deleteEvent = UserDeletedEvent
        )
        .genDwId(StaffUserDwIdCol, startId, s"${StaffUserEntity}_created_time")
    )

    transformed.map(DataSink(sinkName, _, controlTableUpdateOptions = Map(ProductMaxIdType -> StaffUserIdKey)))
  }

  def addEnableForDelete(df: DataFrame): DataFrame =
    df.withColumn("enabled", when(col("eventType") === UserDeletedEvent, lit(false).cast(BooleanType))
      .otherwise(col("enabled")))

  def combine(staffDf: Option[DataFrame], staffDeletedDf: Option[DataFrame]): Option[DataFrame] = {
    val dfOpt = for {
      staff <- staffDf
      staffDeleted <- staffDeletedDf
    } yield staff.unionByName(staffDeleted, allowMissingColumns = true)

    dfOpt.orElse(staffDf).orElse(staffDeletedDf)
  }

  def addDefaultCols(df: DataFrame): DataFrame = {
    df.addColIfNotExists("onboarded", BooleanType)
      .addColIfNotExists("expirable", BooleanType)
      .addColIfNotExists("excludeFromReport", BooleanType)
      .addColIfNotExists("avatar", StringType)
  }
}

object StaffUserTransform {
  val StaffUserTransformService = "staff-user-transform"
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSessionUtils.getSession(StaffUserTransformService)
    val service = new SparkBatchService(StaffUserTransformService, session)
    val transform = new StaffUserTransform(session, service, StaffUserTransformService)
    service.run(transform.transform())
  }
}
