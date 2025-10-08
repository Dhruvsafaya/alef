package com.alefeducation.dimensions

import com.alefeducation.bigdata.Sink
import com.alefeducation.bigdata.batch.delta.{Alias, Delta, DeltaSink}
import com.alefeducation.service.SparkBatchService
import com.alefeducation.util.Constants.{
  GuardianFeedbackReadEvent,
  GuardianMessageSentEvent,
  TeacherFeedbackDeletedEvent,
  TeacherFeedbackSentEvent,
  TeacherMessageDeletedEvent,
  TeacherMessageSentEvent
}
import com.alefeducation.util.{BatchTransformerUtility, Resources, SparkSessionUtils}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, explode, lit, typedLit, upper}

import scala.collection.immutable.Map

private object CommonColsMapper {
  private val eventActorTypeMap = typedLit(
    Map(
      TeacherFeedbackSentEvent -> 1,
      TeacherMessageSentEvent -> 1,
      GuardianFeedbackReadEvent -> 2,
      GuardianMessageSentEvent -> 2,
      TeacherMessageDeletedEvent -> 1,
      TeacherFeedbackDeletedEvent -> 1,
    ))
  private val eventIsReadMap = typedLit(
    Map(
      TeacherFeedbackSentEvent -> false,
      TeacherMessageSentEvent -> false,
      GuardianFeedbackReadEvent -> true,
      GuardianMessageSentEvent -> false,
      TeacherMessageDeletedEvent -> false,
      TeacherFeedbackDeletedEvent -> false,
    ))
  private val eventSubjectMap = typedLit(
    Map(
      TeacherFeedbackSentEvent -> 1,
      TeacherMessageSentEvent -> 2,
      GuardianFeedbackReadEvent -> 1,
      GuardianMessageSentEvent -> 2,
      TeacherMessageDeletedEvent -> 2,
      TeacherFeedbackDeletedEvent -> 1,
    ))

  def mapFeedbackCommonCols: DataFrame => DataFrame =
    _.withColumn("actorType", eventActorTypeMap(col("eventType")))
      .withColumn("isRead", eventIsReadMap(col("eventType")))
      .withColumn("eventSubject", eventSubjectMap(col("eventType")))

}

private object DeleteOptions {
  import TeacherGuardianFeedbackDimension._

  def set(tableName: String, keyColumn: String): Map[String, String] = {
    val tmpTable = s"tmp_$tableName"
    val targetTableRel = s"rel_$tableName"
    val targetTableDim = s"dim_$tableName"

    val dimSchema = Resources.redshiftSchema()
    val relSchema = Resources.redshiftStageSchema()

    val deletePostaction =
      s"""
        begin transaction;

        UPDATE
          $relSchema.$targetTableRel
        SET
          ${TeacherFeedbackEntityName}_status = 4,
          ${TeacherFeedbackEntityName}_updated_time = $relSchema.$tmpTable.${TeacherFeedbackEntityName}_created_time,
          ${TeacherFeedbackEntityName}_dw_updated_time = $relSchema.$tmpTable.${TeacherFeedbackEntityName}_dw_created_time,
          ${TeacherFeedbackEntityName}_deleted_time = $relSchema.$tmpTable.${TeacherFeedbackEntityName}_deleted_time
        FROM
          $relSchema.$tmpTable
        WHERE
          $targetTableRel.$keyColumn = $tmpTable.$keyColumn
          AND $targetTableRel.${TeacherFeedbackEntityName}_created_time < $tmpTable.${TeacherFeedbackEntityName}_created_time
          AND ( $targetTableRel.${TeacherFeedbackEntityName}_deleted_time is null
              OR $targetTableRel.${TeacherFeedbackEntityName}_deleted_time > $tmpTable.${TeacherFeedbackEntityName}_created_time);

        UPDATE
          $dimSchema.$targetTableDim
        SET
          ${TeacherFeedbackEntityName}_status = 4,
          ${TeacherFeedbackEntityName}_updated_time = $relSchema.$tmpTable.${TeacherFeedbackEntityName}_created_time,
          ${TeacherFeedbackEntityName}_dw_updated_time = $relSchema.$tmpTable.${TeacherFeedbackEntityName}_dw_created_time,
          ${TeacherFeedbackEntityName}_deleted_time = $relSchema.$tmpTable.${TeacherFeedbackEntityName}_deleted_time
        FROM
          $relSchema.$tmpTable
        WHERE
          $targetTableDim.$keyColumn = $tmpTable.$keyColumn
          AND $targetTableDim.${TeacherFeedbackEntityName}_created_time < $tmpTable.${TeacherFeedbackEntityName}_created_time
          AND ($targetTableDim.${TeacherFeedbackEntityName}_deleted_time is null
            OR $targetTableDim.${TeacherFeedbackEntityName}_deleted_time > $tmpTable.${TeacherFeedbackEntityName}_created_time);

        drop table $relSchema.$tmpTable;
        end transaction
       """.stripMargin

    Map("dbtable" -> s"$relSchema.$tmpTable", "postactions" -> deletePostaction)
  }
}

class TeacherFeedbackSentTransformer(override implicit val session: SparkSession) extends SparkBatchService {
  import TeacherGuardianFeedbackDimension._
  import Delta.Transformations._
  import BatchTransformerUtility._

  override val name: String = JobName

  private val feedbackTypeMap = typedLit(
    Map(
      "BEHAVIOR" -> 1,
      "ACADEMIC" -> 2,
      "OTHER" -> 3,
    ))

  def transform(): List[Sink] = {
    val teacherFeedbackSentDf = readOptional(ParquetTeacherFeedbackSentSource, session)
    val parquetSink = teacherFeedbackSentDf.map(_.toParquetSink(ParquetTeacherFeedbackSentSink))

    val transformedTeacherFeedbackSentDf = teacherFeedbackSentDf.map(
      _.select(col("*"), explode(col("guardianIds")).alias("guardianId"))
        .withColumn("feedbackType", feedbackTypeMap(upper(col("feedbackType"))))
        .transform(CommonColsMapper.mapFeedbackCommonCols)
        .transformForInsertDim(TeacherFeedbackSentColMapping, TeacherFeedbackEntityName, ids = List("feedbackThreadId", "guardianId"))
        .cache()
    )

    val deltaSink: Option[DeltaSink] = transformedTeacherFeedbackSentDf.flatMap(
      _.toCreate().map(_.toSink(DeltaTeacherFeedbackThreadSink))
    )
    val redshiftSink = transformedTeacherFeedbackSentDf.map(
      _.toRedshiftInsertSink(RedshiftTeacherFeedbackThreadSink, TeacherFeedbackSentEvent)
    )
    (parquetSink ++ redshiftSink ++ deltaSink).toList
  }
}

class TeacherMessageSentTransformer(override implicit val session: SparkSession) extends SparkBatchService {
  import TeacherGuardianFeedbackDimension._
  import Delta.Transformations._
  import BatchTransformerUtility._

  override val name: String = JobName

  override def transform(): List[Sink] = {
    val teacherFeedbackSentDf = readOptional(ParquetTeacherMessageSentSource, session)
    val parquetSink = teacherFeedbackSentDf.map(_.toParquetSink(ParquetTeacherMessageSentSink))

    val transformedTeacherFeedbackSentDf = teacherFeedbackSentDf.map(
      _.transform(CommonColsMapper.mapFeedbackCommonCols)
        .transformForInsertDim(TeacherMessageSentColMapping, TeacherFeedbackEntityName, ids = List("messageId"))
        .cache()
    )
    val deltaSink: Option[DeltaSink] = transformedTeacherFeedbackSentDf.flatMap(
      _.toCreate().map(_.toSink(DeltaTeacherFeedbackThreadSink))
    )
    val redshiftSink = transformedTeacherFeedbackSentDf.map(
      _.toRedshiftInsertSink(RedshiftTeacherFeedbackThreadSink, TeacherMessageSentEvent)
    )
    (parquetSink ++ redshiftSink ++ deltaSink).toList
  }
}

class GuardianMessageSentTransformer(override implicit val session: SparkSession) extends SparkBatchService {
  import TeacherGuardianFeedbackDimension._
  import Delta.Transformations._
  import BatchTransformerUtility._

  override val name: String = JobName

  def transform(): List[Sink] = {
    val df = readOptional(ParquetGuardianMessageSentSource, session)
    val parquetSink = df.map(_.toParquetSink(ParquetGuardianMessageSentSink))

    val transformedDf = df.map(
      _.transform(CommonColsMapper.mapFeedbackCommonCols)
        .transformForInsertDim(GuardianMessageSentColMapping, TeacherFeedbackEntityName, ids = List("messageId"))
        .cache()
    )
    val deltaSink: Option[DeltaSink] = transformedDf.flatMap(
      _.toCreate().map(_.toSink(DeltaTeacherFeedbackThreadSink))
    )
    val redshiftSink = transformedDf.map(
      _.toRedshiftInsertSink(RedshiftTeacherFeedbackThreadSink, GuardianMessageSentEvent)
    )
    (parquetSink ++ redshiftSink ++ deltaSink).toList
  }
}

class GuardianFeedbackReadTransformer(override implicit val session: SparkSession) extends SparkBatchService {
  import TeacherGuardianFeedbackDimension._
  import Delta.Transformations._
  import BatchTransformerUtility._

  override val name: String = JobName

  private val deltaMatchGuardianThreadCondition =
    s"""
       |${Alias.Delta}.${TeacherFeedbackEntityName}_thread_id = ${Alias.Events}.${TeacherFeedbackEntityName}_thread_id
       | AND ${Alias.Delta}.${TeacherFeedbackEntityName}_guardian_id = ${Alias.Events}.${TeacherFeedbackEntityName}_guardian_id
       | AND ${Alias.Delta}.${TeacherFeedbackEntityName}_event_subject = 1
     """.stripMargin

  def transform(): List[Sink] = {
    val df = readOptional(ParquetGuardianFeedbackReadSource, session)
    val parquetSink = df.map(_.toParquetSink(ParquetGuardianFeedbackReadSink))

    val transformedDf = df.map(
      _.withColumn("isRead", lit(true))
        .transformForUpdateDim(GuardianFeedbackReadColMapping, TeacherFeedbackEntityName, ids = List("feedbackThreadId", "guardianId"))
        .cache()
    )
    val deltaSink: Option[DeltaSink] = transformedDf.flatMap(
      _.toUpdate(
        matchConditions = deltaMatchGuardianThreadCondition,
        updateColumns = List(s"${TeacherFeedbackEntityName}_is_read")
      ).map(_.toSink(DeltaTeacherFeedbackThreadSink, TeacherFeedbackEntityName, GuardianFeedbackReadEvent))
    )
    val redshiftSink = transformedDf.map(
      _.toRedshiftUpdateSink(RedshiftTeacherFeedbackThreadSink,
                             GuardianFeedbackReadEvent,
                             TeacherFeedbackEntityName,
                             options = setReadOptions("teacher_feedback_thread"))
    )
    (parquetSink ++ redshiftSink ++ deltaSink).toList
  }

  private def setReadOptions(tableName: String): Map[String, String] = {
    val tmpTable = s"tmp_$tableName"
    val targetTableRel = s"rel_$tableName"
    val targetTableDim = s"dim_$tableName"

    val dimSchema = Resources.redshiftSchema()
    val relSchema = Resources.redshiftStageSchema()

    val readPostaction =
      s"""
        begin transaction;

        UPDATE
          $relSchema.$targetTableRel
        SET
          ${TeacherFeedbackEntityName}_updated_time = $relSchema.$tmpTable.${TeacherFeedbackEntityName}_created_time,
          ${TeacherFeedbackEntityName}_dw_updated_time = $relSchema.$tmpTable.${TeacherFeedbackEntityName}_dw_created_time
        FROM
          $relSchema.$tmpTable
        WHERE
          $targetTableRel.${TeacherFeedbackEntityName}_thread_id = $tmpTable.${TeacherFeedbackEntityName}_thread_id
          AND $targetTableRel.${TeacherFeedbackEntityName}_guardian_id = $tmpTable.${TeacherFeedbackEntityName}_guardian_id
          AND $targetTableRel.${TeacherFeedbackEntityName}_event_subject = 1
          AND $targetTableRel.${TeacherFeedbackEntityName}_created_time < $tmpTable.${TeacherFeedbackEntityName}_created_time
          AND ($targetTableRel.${TeacherFeedbackEntityName}_updated_time is null
            OR $targetTableRel.${TeacherFeedbackEntityName}_updated_time < $tmpTable.${TeacherFeedbackEntityName}_created_time);

        UPDATE
          $relSchema.$targetTableRel
        SET
          ${TeacherFeedbackEntityName}_is_read = true
        FROM
          $relSchema.$tmpTable
        WHERE $targetTableRel.${TeacherFeedbackEntityName}_thread_id = $tmpTable.${TeacherFeedbackEntityName}_thread_id
          AND $targetTableRel.${TeacherFeedbackEntityName}_guardian_id = $tmpTable.${TeacherFeedbackEntityName}_guardian_id
          AND $targetTableRel.${TeacherFeedbackEntityName}_event_subject = 1
          AND $targetTableRel.${TeacherFeedbackEntityName}_created_time < $tmpTable.${TeacherFeedbackEntityName}_created_time
          AND $targetTableRel.${TeacherFeedbackEntityName}_is_read is false;

        UPDATE
          $dimSchema.$targetTableDim
        SET
          ${TeacherFeedbackEntityName}_updated_time = $relSchema.$tmpTable.${TeacherFeedbackEntityName}_created_time,
          ${TeacherFeedbackEntityName}_dw_updated_time = $relSchema.$tmpTable.${TeacherFeedbackEntityName}_dw_created_time
        FROM
          $relSchema.$tmpTable
            JOIN $dimSchema.dim_guardian g
              ON $relSchema.$tmpTable.${TeacherFeedbackEntityName}_guardian_id = g.guardian_id
        WHERE
          $targetTableDim.${TeacherFeedbackEntityName}_thread_id = $tmpTable.${TeacherFeedbackEntityName}_thread_id
          AND $targetTableDim.${TeacherFeedbackEntityName}_guardian_dw_id = g.guardian_dw_id
          AND $targetTableDim.${TeacherFeedbackEntityName}_event_subject = 1
          AND $targetTableDim.${TeacherFeedbackEntityName}_created_time < $tmpTable.${TeacherFeedbackEntityName}_created_time
          AND ($targetTableDim.${TeacherFeedbackEntityName}_updated_time is null
            OR $targetTableDim.${TeacherFeedbackEntityName}_updated_time < $tmpTable.${TeacherFeedbackEntityName}_created_time);

        UPDATE
          $dimSchema.$targetTableDim
        SET
          ${TeacherFeedbackEntityName}_is_read = true
        FROM
          $relSchema.$tmpTable
            JOIN $dimSchema.dim_guardian g
              ON $relSchema.$tmpTable.${TeacherFeedbackEntityName}_guardian_id = g.guardian_id
        WHERE $targetTableDim.${TeacherFeedbackEntityName}_thread_id = $tmpTable.${TeacherFeedbackEntityName}_thread_id
          AND $targetTableDim.${TeacherFeedbackEntityName}_guardian_dw_id = g.guardian_dw_id
          AND $targetTableDim.${TeacherFeedbackEntityName}_event_subject = 1
          AND $targetTableDim.${TeacherFeedbackEntityName}_created_time < $tmpTable.${TeacherFeedbackEntityName}_created_time
          AND $targetTableDim.${TeacherFeedbackEntityName}_is_read is false;

        drop table $relSchema.$tmpTable;
        end transaction
       """.stripMargin

    Map("dbtable" -> s"$relSchema.$tmpTable", "postactions" -> readPostaction)
  }
}

class TeacherFeedbackDeletedTransformer(override implicit val session: SparkSession) extends SparkBatchService {
  import TeacherGuardianFeedbackDimension._
  import Delta.Transformations._
  import BatchTransformerUtility._

  override val name: String = JobName

  private val deltaMatchThreadCondition =
    s"""
       |${Alias.Delta}.${TeacherFeedbackEntityName}_thread_id = ${Alias.Events}.${TeacherFeedbackEntityName}_thread_id
       | AND ${Alias.Delta}.${TeacherFeedbackEntityName}_status <> 4
     """.stripMargin

  def transform(): List[Sink] = {
    val df = readOptional(ParquetTeacherFeedbackDeletedSource, session)
    val parquetSink = df.map(_.toParquetSink(ParquetTeacherFeedbackDeletedSink))

    val transformedDf = df.map(
      _.transformForDelete(TeacherFeedbackDeletedColMapping, TeacherFeedbackEntityName, idColumns = List("feedbackThreadId"))
        .cache()
    )
    val deltaSink: Option[DeltaSink] = transformedDf.flatMap(
      _.toDelete(matchConditions = deltaMatchThreadCondition)
        .map(_.toSink(DeltaTeacherFeedbackThreadSink, TeacherFeedbackEntityName, eventType = TeacherFeedbackDeletedEvent))
    )
    val redshiftSink = transformedDf.map(
      _.toRedshiftUpdateSink(
        RedshiftTeacherFeedbackThreadSink,
        TeacherFeedbackDeletedEvent,
        TeacherFeedbackEntityName,
        options = DeleteOptions.set("teacher_feedback_thread", s"${TeacherFeedbackEntityName}_thread_id")
      ))
    (parquetSink ++ redshiftSink ++ deltaSink).toList
  }
}

class TeacherMessageDeletedTransformer(override implicit val session: SparkSession) extends SparkBatchService {
  import TeacherGuardianFeedbackDimension._
  import Delta.Transformations._
  import BatchTransformerUtility._

  override val name: String = JobName

  private val deltaMatchMessageCondition =
    s"""
       |${Alias.Delta}.${TeacherFeedbackEntityName}_message_id = ${Alias.Events}.${TeacherFeedbackEntityName}_message_id
     """.stripMargin

  def transform(): List[Sink] = {
    val df = readOptional(ParquetTeacherMessageDeletedSource, session)
    val parquetSink = df.map(_.toParquetSink(ParquetTeacherMessageDeletedSink))

    val transformedDf = df.map(
      _.transformForDelete(TeacherMessageDeletedColMapping, TeacherFeedbackEntityName, idColumns = List("messageId"))
        .cache()
    )
    val deltaSink: Option[DeltaSink] = transformedDf.flatMap(
      _.toDelete(matchConditions = deltaMatchMessageCondition)
        .map(_.toSink(DeltaTeacherFeedbackThreadSink, TeacherFeedbackEntityName, eventType = TeacherMessageDeletedEvent))
    )
    val redshiftSink = transformedDf.map(
      _.toRedshiftUpdateSink(
        RedshiftTeacherFeedbackThreadSink,
        TeacherMessageDeletedEvent,
        TeacherFeedbackEntityName,
        options = DeleteOptions.set("teacher_feedback_thread", s"${TeacherFeedbackEntityName}_message_id")
      )
    )
    (parquetSink ++ redshiftSink ++ deltaSink).toList
  }
}

class TeacherGuardianFeedbackDimension(val transformers: List[SparkBatchService], override implicit val session: SparkSession)
    extends SparkBatchService {
  import TeacherGuardianFeedbackDimension._

  override val name: String = JobName

  override def transform(): List[Sink] = {
    transformers.flatMap(_.transform())
  }

}

object TeacherGuardianFeedbackDimension {
  val JobName = "teacher-guardian-feedback-dimension"
  val TeacherFeedbackEntityName = "tft"

  val ParquetTeacherFeedbackSentSource = "parquet-teacher-feedback-sent-source"
  val ParquetTeacherMessageSentSource = "parquet-teacher-message-sent-source"
  val ParquetGuardianFeedbackReadSource = "parquet-guardian-feedback-read-source"
  val ParquetGuardianMessageSentSource = "parquet-guardian-message-sent-source"
  val ParquetTeacherMessageDeletedSource = "parquet-teacher-message-deleted-source"
  val ParquetTeacherFeedbackDeletedSource = "parquet-teacher-feedback-deleted-source"

  val ParquetTeacherFeedbackSentSink = "parquet-teacher-feedback-sent-sink"
  val ParquetTeacherMessageSentSink = "parquet-teacher-message-sent-sink"
  val ParquetGuardianFeedbackReadSink = "parquet-guardian-feedback-read-sink"
  val ParquetGuardianMessageSentSink = "parquet-guardian-message-sent-sink"
  val ParquetTeacherMessageDeletedSink = "parquet-teacher-message-deleted-sink"
  val ParquetTeacherFeedbackDeletedSink = "parquet-teacher-feedback-deleted-sink"

  val RedshiftTeacherFeedbackThreadSink = "redshift-teacher-feedback-thread-sink"
  val DeltaTeacherFeedbackThreadSink = "delta-teacher-feedback-thread-sink"

  val CommonColMapping: Map[String, String] = Map(
    "actorType" -> s"${TeacherFeedbackEntityName}_actor_type",
    "isRead" -> s"${TeacherFeedbackEntityName}_is_read",
    "eventSubject" -> s"${TeacherFeedbackEntityName}_event_subject",
    "occurredOn" -> "occurredOn",
    s"${TeacherFeedbackEntityName}_status" -> s"${TeacherFeedbackEntityName}_status",
  )

  val TeacherFeedbackSentColMapping: Map[String, String] = CommonColMapping ++ Map(
    "feedbackThreadId" -> s"${TeacherFeedbackEntityName}_thread_id",
    "guardianId" -> s"${TeacherFeedbackEntityName}_guardian_id",
    "responseEnabled" -> s"${TeacherFeedbackEntityName}_response_enabled",
    "feedbackType" -> s"${TeacherFeedbackEntityName}_feedback_type",
    "teacherId" -> s"${TeacherFeedbackEntityName}_teacher_id",
    "studentId" -> s"${TeacherFeedbackEntityName}_student_id",
    "classId" -> s"${TeacherFeedbackEntityName}_class_id"
  )

  val TeacherFeedbackDeletedColMapping: Map[String, String] = Map(
    "feedbackThreadId" -> s"${TeacherFeedbackEntityName}_thread_id",
    "occurredOn" -> "occurredOn",
    s"${TeacherFeedbackEntityName}_status" -> s"${TeacherFeedbackEntityName}_status",
  )

  val TeacherMessageDeletedColMapping: Map[String, String] = Map(
    "messageId" -> s"${TeacherFeedbackEntityName}_message_id",
    "occurredOn" -> "occurredOn",
    s"${TeacherFeedbackEntityName}_status" -> s"${TeacherFeedbackEntityName}_status",
  )

  val TeacherMessageSentColMapping: Map[String, String] = CommonColMapping ++ Map(
    "messageId" -> s"${TeacherFeedbackEntityName}_message_id",
    "feedbackThreadId" -> s"${TeacherFeedbackEntityName}_thread_id",
    "isFirstOfThread" -> s"${TeacherFeedbackEntityName}_is_first_of_thread",
  )

  val GuardianFeedbackReadColMapping: Map[String, String] = Map(
    "guardianId" -> s"${TeacherFeedbackEntityName}_guardian_id",
    "feedbackThreadId" -> s"${TeacherFeedbackEntityName}_thread_id",
    "isRead" -> s"${TeacherFeedbackEntityName}_is_read",
    "occurredOn" -> "occurredOn",
  )

  val GuardianMessageSentColMapping: Map[String, String] = CommonColMapping ++ Map(
    "messageId" -> s"${TeacherFeedbackEntityName}_message_id",
    "feedbackThreadId" -> s"${TeacherFeedbackEntityName}_thread_id",
    "guardianId" -> s"${TeacherFeedbackEntityName}_guardian_id",
  )

  implicit val session: SparkSession = SparkSessionUtils.getSession(JobName)

  val transformers: List[SparkBatchService] = List(
    new TeacherFeedbackSentTransformer,
    new TeacherMessageSentTransformer,
    new GuardianMessageSentTransformer,
    new GuardianFeedbackReadTransformer,
    new TeacherFeedbackDeletedTransformer,
    new TeacherFeedbackDeletedTransformer,
    new TeacherMessageDeletedTransformer
  )

  def apply(implicit session: SparkSession): TeacherGuardianFeedbackDimension = new TeacherGuardianFeedbackDimension(transformers, session)

  def main(args: Array[String]): Unit = TeacherGuardianFeedbackDimension(session).run
}
