package com.alefeducation.dimensions

import com.alefeducation.bigdata.batch.delta.{Alias, Delta, DeltaSink}
import com.alefeducation.service.{DataSink, SparkBatchService}
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Constants._
import com.alefeducation.util.DataFrameUtility._
import com.alefeducation.util.Helpers._
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import com.alefeducation.bigdata.Sink
import org.apache.spark.sql.types.StringType

class InstructionalPlanDimension(override implicit val session: SparkSession) extends SparkBatchService {

  import session.implicits._

  override val name: String = InstructionalPlanDimension.name

  val uniqueIdColumn = "id"

  override def transform(): List[Sink] = {
    val ipPublishedRawDf = read(ParquetInstructionalPlanPublishedSource, session, isMandatory = false)
    val ipRePublishedRawDf = read(ParquetInstructionalPlanRePublishedSource, session, isMandatory = false)
    val ipDeletedRawDf = read(ParquetInstructionalPlanDeletedSource, session, isMandatory = false)

    val ipPublishedEvents = addDefaultColumn(ipPublishedRawDf)
    val ipRePublishedEvents = addDefaultColumn(ipRePublishedRawDf)

    val latestRepublishedEvents = selectLatestUpdatedWithoutEventFilter(ipRePublishedEvents, "id")

    val dfWithPublishEvent = transformIP(ipPublishedEvents)
      .transformIfNotEmpty(_.withColumn("contentRepositoryUuid", $"contentRepositoryId"))
      .transformDataFrameWithEventType(List(InstructionalPlanPublishedEvent), InstructionPlanCols, InstructionPlanEntity)

    val deleteDfFromRepublish = latestRepublishedEvents
      .transformIfNotEmpty(_.select("id", "occurredOn").withColumn("eventType", lit(InstructionalPlanDeletedEvent)))
      .transformDataFrameWithEventType(List(InstructionalPlanDeletedEvent),
        InstructionPlanDeletedCols,
        InstructionPlanEntity,
        idColumns = List("id"))

    val dfWithRepublishEvent = transformIP(latestRepublishedEvents)
      .transformIfNotEmpty(_.withColumn("contentRepositoryUuid", $"contentRepositoryId"))
      .transformDataFrameWithEventType(List(InstructionalPlanRePublishedEvent), InstructionPlanCols, InstructionPlanEntity)

    val dfWithDeletedEvent = ipDeletedRawDf
      .transformDataFrameWithEventType(List(InstructionalPlanDeletedEvent),
        InstructionPlanDeletedCols,
        InstructionPlanEntity,
        idColumns = List("id"))

    val transformedDataWithEvent = dfWithPublishEvent ++ deleteDfFromRepublish ++ dfWithRepublishEvent ++ dfWithDeletedEvent

    getParquetSinks(ipPublishedRawDf, ipRePublishedRawDf, ipDeletedRawDf) ++
      transformedDataWithEvent.createRedshiftSinks(RedshiftInstructionalPlanSink, InstructionPlanEntity) ++
      getDeltaSinks(transformedDataWithEvent)
  }

  def addDefaultColumn(df: DataFrame): DataFrame =
    if (df.columns.contains("contentRepositoryId")) df
    else df.withColumn("contentRepositoryId", lit(null).cast(StringType))

  private def getParquetSinks(ipPublishedRawDf: DataFrame, ipRePublishedRawDf: DataFrame, ipDeletedRawDf: DataFrame): List[DataSink] = {
    val parquetSinksPublish = ipPublishedRawDf.createParquetSinks(ParquetInstructionalPlanPublishedSource)
    val parquetSinksRePublish = ipRePublishedRawDf.createParquetSinks(ParquetInstructionalPlanRePublishedSource)
    val parquetSinksDeleted = ipDeletedRawDf.createParquetSinks(ParquetInstructionalPlanDeletedSource)
    parquetSinksPublish ++ parquetSinksRePublish ++ parquetSinksDeleted
  }

  private def getDeltaSinks(transformedDataFrame: List[TransformedDataWithEvent]): List[DeltaSink] = {
    import Delta.Transformations._
    import com.alefeducation.bigdata.batch.delta.Builder.Transformations
    val matchCondition =
      s"${Alias.Delta}.${InstructionPlanEntity}_id = ${Alias.Events}.${InstructionPlanEntity}_id and " +
        s"${Alias.Delta}.${InstructionPlanEntity}_status = 1"
    transformedDataFrame flatMap {
      case TransformedDataWithEvent(df, InstructionalPlanPublishedEvent) =>
        df.withEventDateColumn(false).toCreate().map(_.toSink(InstructionalPlanDeltaSink))
      case TransformedDataWithEvent(df, InstructionalPlanRePublishedEvent) =>
        df.withEventDateColumn(false).toCreate().map(_.toSink(InstructionalPlanDeltaSink))
      case TransformedDataWithEvent(df, InstructionalPlanDeletedEvent) =>
        df.withEventDateColumn(false)
          .toDelete(matchConditions = matchCondition)
          .map(_.toSink(InstructionalPlanDeltaSink, InstructionPlanEntity))
      case _ => Nil
    }
  }

  private def transformIP(ipDf: DataFrame): DataFrame = {
    if (ipDf.isEmpty) ipDf
    else {
      ipDf.withColumn("items", explode($"items"))
        .addNestedColumnIfNotExists("items.itemType")
        .addNestedColumnIfNotExists("items.checkpointUuid")
        .addNestedColumnIfNotExists("items.lessonUuid")
        .addNestedColumnIfNotExists("items.lessonId")
    }
  }
}

object InstructionalPlanDimension {

  val name: String = InstructionalPlanDimensionName

  def apply(implicit session: SparkSession): InstructionalPlanDimension = new InstructionalPlanDimension

  def main(args: Array[String]): Unit = {
    InstructionalPlanDimension(SparkSessionUtils.getSession(name)).run
  }

}
