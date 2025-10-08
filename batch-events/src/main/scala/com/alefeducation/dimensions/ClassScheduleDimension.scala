package com.alefeducation.dimensions

import com.alefeducation.bigdata.Sink
import com.alefeducation.bigdata.batch.delta.{Alias, Delta}
import com.alefeducation.service.{DataSink, SparkBatchService}
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.alefeducation.util.Constants._
import com.alefeducation.util.DataFrameUtility.{addStatusCol, addUpdatedTimeCols}
import com.alefeducation.util.Helpers._
import com.alefeducation.util.{BatchTransformerUtility, SparkSessionUtils}
import org.apache.spark.sql.functions.{col, lit, when}


class ClassScheduleDimension(override val name: String, override implicit val session: SparkSession) extends SparkBatchService {

  import BatchTransformerUtility._
  import Delta.Transformations._

  val orderByField: String = "occurredOn"

  val groupKey: List[String] = List(
    "classId",
    "day",
    "startTime",
    "endTime"
  )

  val uniqueIdCols: List[String] = List(
    s"${ClassScheduleEntity}_class_id",
    s"${ClassScheduleEntity}_day",
    s"${ClassScheduleEntity}_start_time",
    s"${ClassScheduleEntity}_end_time"
  )

  val deltaMatchCondition: String =
    s"""
       |${Alias.Delta}.${ClassScheduleEntity}_class_id = ${Alias.Events}.${ClassScheduleEntity}_class_id
       | and
       |${Alias.Delta}.${ClassScheduleEntity}_day = ${Alias.Events}.${ClassScheduleEntity}_day
       | and
       |${Alias.Delta}.${ClassScheduleEntity}_start_time = ${Alias.Events}.${ClassScheduleEntity}_start_time
       | and
       |${Alias.Delta}.${ClassScheduleEntity}_end_time = ${Alias.Events}.${ClassScheduleEntity}_end_time
     """.stripMargin

  override def transform(): List[Sink] = {
    val classScheduleSourceDf = readOptional(ParquetClassScheduleSource, session)

    val parquetClassScheduleSink = classScheduleSourceDf.map(_.toParquetSink(ParquetClassScheduleSink))

    val classScheduleDf = classScheduleSourceDf.map(
      preprocess(
        ClassScheduleCols,
        groupKey,
        ClassScheduleSlotAddedEvent,
        ClassScheduleSlotDeletedEvent
      )
    )

    val redshiftClassScheduleSink = classScheduleDf.map(createRedshiftSink)
    val deltaClassScheduleSink = classScheduleDf.flatMap(createDeltaSink)

    (redshiftClassScheduleSink ++ deltaClassScheduleSink ++ parquetClassScheduleSink).toList
  }

  def createDeltaSink(df: DataFrame): Option[Sink] =
    df.toIWH(deltaMatchCondition, uniqueIdCols)
      .map(_.toSink(DeltaClassScheduleSink, ClassScheduleEntity))

  def createRedshiftSink(df: DataFrame): DataSink = {
    df.toRedshiftIWHSink(
      sinkName = RedshiftClassScheduleSink,
      entity = ClassScheduleEntity,
      ids = uniqueIdCols
    )
  }

  def preprocess(columnMapping: Map[String, String],
                 groupKey: List[String],
                 attachEvent: String,
                 detachEvent: String)(df: DataFrame): DataFrame =
    df.transform(addCols(groupKey))
      .transform(addAttachStatusCol(attachEvent, detachEvent))
      .transformForInsertWithHistory(columnMapping, ClassScheduleEntity)

  def addAttachStatusCol(attachedEvent: String, detachedEvent: String)(df: DataFrame): DataFrame =
    df.withColumn(
      s"${ClassScheduleEntity}_attach_status",
      when(col("eventType") === attachedEvent, lit(ClassScheduleAddedStatusVal))
        .when(col("eventType") === detachedEvent, lit(ClassScheduleDeletedStatusVal))
        .otherwise(lit(ClassScheduleUndefinedStatusVal))
    )

  def addCols(groupKey: List[String]): DataFrame => DataFrame = {
    addStatusCol(ClassScheduleEntity, orderByField, groupKey) _ andThen
      addUpdatedTimeCols(ClassScheduleEntity, orderByField, groupKey)
  }
}

object ClassScheduleDimension {
  def main(args: Array[String]): Unit = {
    new ClassScheduleDimension(ClassScheduleDimensionName, SparkSessionUtils.getSession(ClassScheduleDimensionName)).run
  }
}