package com.alefeducation.dimensions.student

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.models.StudentModel._
import com.alefeducation.schema.admin.{StudentMovedBetweenSchoolsOrGrades, StudentPromotedEvent}
import com.alefeducation.service.DataSink
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Constants.{AdminStudent, AdminStudentUpdated}
import com.alefeducation.util.DataFrameUtility.{addTimestampColumns, latestOccurredOnTimeUDF, selectAs, selectLatestRecordsByRowNumber}
import com.alefeducation.util.Helpers._
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{Column, DataFrame, Encoder, SparkSession}

class StudentTransform(val session: SparkSession, val service: SparkBatchService) {

  import StudentTransform._

  def transform(): Option[Sink] = {
    import session.implicits._

    val studentCreatedDf =
      getDataFrameFromSourceBasedOnEventFilter[StudentMutated](ParquetStudentSource, AdminStudent)
        .map(_.dropDuplicates)

    val studentUpdatedDf =
      getDataFrameFromSourceBasedOnEventFilter[StudentMutated](ParquetStudentSource, AdminStudentUpdated)
        .map(_.dropDuplicates)

    val studentStatusToggleDf =
      getDataFrameFromSource[StudentStatus](ParquetStudentEnableDisableSource, List("occurredOn", "uuid", "eventType"))
        .map(
          _.withColumn("student_status",
            when($"eventType".contains("Disabled"), lit(Disabled))
              .otherwise(lit(ActiveEnabled))
          )
        ).map(_.dropDuplicates)

    val studentMoveSchoolsDf =
      getDataFrameFromSource[StudentMovedBetweenSchoolsOrGrades](ParquetStudentSchoolOrGradeMoveSource)
        .map(_.dropDuplicates)

    val studentPromotedDf = getDataFrameFromSource[StudentPromotedEvent](ParquetStudentPromotedSource)
      .map(_.dropDuplicates)

    val studentSectionUpdatedDf =
      getDataFrameFromSource[StudentSectionUpdated](ParquetStudentSectionUpdatedSource)
        .map(_.withColumnRenamed("id", "uuid"))
        .map(_.dropDuplicates)

    val studentTagUpdatedDf = getDataFrameFromSource[StudentTag](ParquetStudentTagUpdatedSource)
      .map(
        _.withColumn("fromTagEvent",
          when(size($"tags") > ZeroArraySize, concat_ws(", ", col("tags")))
            .otherwise(lit(DefaultStringValue))
        ).drop($"tags"))
      .map(_.dropDuplicates)

    val studentsDeletedSource = service.readOptional(ParquetStudentsDeletedSource, session)
      .map(_.dropDuplicates)

    val studentDeletedDf = studentsDeletedSource match {
      case None => Some(Seq.empty[StudentStatus].toDF)
      case Some(df) =>
        Some({
          val explodedSet = df.select(explode($"events").as("details"), $"schoolId", $"occurredOn", $"eventType")
          explodedSet
            .select($"details.studentId".as("uuid"), $"occurredOn", $"eventType")
            .withColumn("student_status", lit(Deleted))
        })
    }

    val studentUuidsDf = getModifiedStudents(
      List(studentCreatedDf,
        studentUpdatedDf,
        studentStatusToggleDf,
        studentMoveSchoolsDf,
        studentPromotedDf,
        studentSectionUpdatedDf,
        studentDeletedDf))

    val studentRSDimSource = service.readFromRedshift[StudentDim](RedshiftDimStudentSink).filter($"student_active_until" isNull)
    val gradeDf = service.readFromRedshift[GradeDim](RedshiftGradeSink, selectedCols = List("grade_dw_id", "grade_id"))
    val classDf = service.readFromRedshift[SectionDim](RedshiftSectionSink, selectedCols = List("section_dw_id", "section_id"))
    val schoolDf = service.readFromRedshift[SchoolDim](RedshiftSchoolSink, selectedCols = List("school_dw_id", "school_id"))
    val studentRSDimDf = studentRSDimSource
      .join(schoolDf, col("student_school_dw_id") === col("school_dw_id"), "inner")
      .join(gradeDf, col("student_grade_dw_id") === col("grade_dw_id"), "inner")
      .join(classDf, col("student_section_dw_id") === col("section_dw_id"), "inner")
      .select(
        $"student_created_time".as("occurredOn"),
        $"student_id".as("uuid"),
        $"student_username".as("username"),
        $"grade_id".as("gradeId"),
        $"section_id".as("sectionId"),
        $"school_id".as("schoolId"),
        $"student_status",
        $"student_tags".as("tags"),
        $"student_special_needs".as("specialNeeds")
      )

    val studentRSRelSource = service.readFromRedshift[StudentRel](RedshiftStudentSink)
    val studentRSRelDf = studentRSRelSource
      .filter($"student_active_until" isNull)
        .select(
          $"student_created_time".as("occurredOn"),
          $"student_uuid".as("uuid"),
          $"student_username".as("username"),
          $"grade_uuid".as("gradeId"),
          $"section_uuid".as("sectionId"),
          $"school_uuid".as("schoolId"),
          $"student_status",
          $"student_tags".as("tags"),
          $"student_special_needs".as("specialNeeds")
        )

    //TODO:Remove after all env release
    val studentDFFromRSDimRel = Some(studentRSDimDf.unionByName(studentRSRelDf))

    val studentDFFromRSDimRelAndCreated =
      combineOptionalDfs(
        studentDFFromRSDimRel,
        studentCreatedDf
          .flatMap(df => joinOptional(df, studentTagUpdatedDf.map(_.drop("occurredOn")), col("uuid") === col("studentId"), "left"))
          .map(
            _.withColumn("tags", when($"studentId".isNull, $"tags").otherwise($"fromTagEvent"))
              .select($"occurredOn",
                $"uuid",
                $"username",
                $"gradeId",
                $"sectionId",
                $"schoolId",
                $"student_status",
                $"tags",
                $"specialNeeds"))
      )

    val studentDFFromRSDimRelAndIncoming =
      studentDFFromRSDimRelAndCreated.flatMap(df => joinOptional(df, studentUuidsDf, col("uuid") === col("studentId"), "inner").map(_.cache()))

    val studentSchoolDf = getUnionedDataFrame(
      List(studentDFFromRSDimRelAndIncoming.map(df => selectAs(df, ColsForSchoolFromDimRel)),
        studentMoveSchoolsDf.map(df => selectAs(df, ColsForSchoolFromMoveSchool))),
      "schoolStudentUuid",
      "occurredOnSchoolMovement"
    )

    val studentGradeAndSectionDf = getUnionedDataFrame(
      List(
        studentDFFromRSDimRelAndIncoming.map(df => selectAs(df, ColsForGradeSectionFromFromDimRel)),
        studentMoveSchoolsDf.map(df => selectAs(df, ColsForGradeSectionFromMoveSchool)),
        studentSectionUpdatedDf.map(df => selectAs(df, ColsForGradeSectionFromSectionUpdated)),
        studentPromotedDf.map(df => selectAs(df, ColsForGradeSectionFromPromoted))
      ),
      "gradeSectionStudentUuid",
      "occurredOnGradeSectionMovement"
    )

    val studentStatusDf = getUnionedDataFrame(
      List(
        studentDFFromRSDimRelAndIncoming.map(df => selectAs(df, ColsForStatus)),
        studentStatusToggleDf.map(df => selectAs(df, ColsForStatus)),
        studentDeletedDf.map(df => selectAs(df, ColsForStatus))
      ),
      "statusStudentUuid",
      "occurredOnStatus"
    )

    val studentOtherColumnsDf = getUnionedDataFrame(
      List(studentDFFromRSDimRelAndIncoming.map(df => selectAs(df, ColsForOtherFields)),
        studentUpdatedDf.map(df => selectAs(df, ColsForOtherFields))),
      "uuid",
      "occurredOnOtherFields"
    )

    val studentTagDf = studentTagUpdatedDf.map(
      df =>
        selectLatestRecordsByRowNumber(df.select($"occurredOn".as("occurredOnTag"), $"studentId".as("studentTagUuid"), $"fromTagEvent"),
          List("studentTagUuid"),
          "occurredOnTag"))

    val studentCombinedTransformed = studentSchoolDf
      .flatMap(df => joinOptional(df, studentGradeAndSectionDf, col("schoolStudentUuid") === col("gradeSectionStudentUuid"), "inner"))
      .flatMap(df => joinOptional(df, studentStatusDf, col("schoolStudentUuid") === col("statusStudentUuid"), "inner"))
      .flatMap(df => joinOptional(df, studentOtherColumnsDf, col("schoolStudentUuid") === col("uuid"), "inner"))
      //TODO:Remove after all env release
      .flatMap(df => joinOptional(df, studentTagDf, col("schoolStudentUuid") === col("studentTagUuid"), "left"))
      .map(_.distinct()
        .select(
          latestOccurredOnTimeUDF(
            $"occurredOnSchoolMovement",
            $"occurredOnGradeSectionMovement",
            $"occurredOnStatus",
            when($"studentTagUuid".isNotNull, $"occurredOnTag").otherwise(lit(DEFAULT_DATE)),
            $"occurredOnOtherFields"
          ).as("occurredOn"),
          when($"studentTagUuid".isNull, $"tags").otherwise($"fromTagEvent").as("tags"),
          $"username",
          $"uuid",
          $"gradeId",
          $"sectionId",
          $"schoolId",
          $"student_status",
          $"specialNeeds"
        ))
    val studentCombinedDf = studentCombinedTransformed.map(df => selectAs(df, StudentDimensionCols))

    // TODO UTC from current_timestamp
    val studentDf =
      studentCombinedDf.map(df => addTimestampColumns(df.withColumn("student_active_until", lit(NULL).cast(TimestampType)), StudentEntity).cache())

    studentDf.map(df => {
      DataSink(StudentTransformedSink, df)
    })
  }

  private def getDataFrameFromSource[T](sourceName: String, selectedCols: List[String] = List("*"))(
    implicit encoder: Encoder[T]): Option[DataFrame] = {
    import session.implicits._
    val dataFrame = service.readOptional(sourceName, session)
    val transformedDf = dataFrame match {
      case None => Seq.empty[T].toDF
      case Some(df) => df.select(selectedCols.map(x => col(x)): _*)
    }
    Some(transformedDf)
  }

  private def getDataFrameFromSourceBasedOnEventFilter[T](sourceName: String, filter: String)(
    implicit encoder: Encoder[T]): Option[DataFrame] = {
    import session.implicits._
    val dataFrame = service.readOptional(sourceName, session)
    val transformedDf = dataFrame match {
      case None => Seq.empty[T].toDF
      case Some(df) =>
        df.filter($"eventType" === filter)
          .select($"occurredOn", $"uuid", $"username", $"gradeId", $"sectionId", $"schoolId", $"tags", $"specialNeeds")
          .withColumn("student_status", lit(ActiveEnabled))
          //TODO:Remove after all env release
          .withColumn("tags",
            when($"tags".isNotNull && size($"tags") > ZeroArraySize, concat_ws(", ", $"tags")).otherwise(lit(DefaultStringValue)))
          .withColumn(
            "specialNeeds",
            when($"specialNeeds".isNotNull && size($"specialNeeds") > ZeroArraySize, concat_ws(", ", $"specialNeeds"))
              .otherwise(lit(DefaultStringValue))
          )
    }
    Some(transformedDf)
  }

  private def joinOptional(df1: DataFrame, df2: Option[DataFrame], joinExp: Column, joinType: String): Option[DataFrame] = {
    df2 match {
      case Some(dataFrame) => Some(df1.join(dataFrame, joinExp, joinType))
      case None => Some(df1)
    }
  }

  private def getModifiedStudents(dataframes: List[Option[DataFrame]]): Option[DataFrame] = {
    def getDF(df: Option[DataFrame]): Option[DataFrame] = {
      import session.implicits._
      df match {
        case Some(dataFrame) =>
          if (dataFrame.columns.contains("uuid")) Some(dataFrame.select($"uuid".as("studentId"))) else Some(dataFrame.select($"studentId"))
        case _ => None
      }
    }

    val transformed = dataframes.map(getDF)
    transformed.reduceLeft((df1, df2) => combineOptionalDfs(df1, df2))
  }

  private def getUnionedDataFrame(dataframes: List[Option[DataFrame]], idColumn: String, eventdateColumn: String): Option[DataFrame] = {
    val unionedDf =
      dataframes.reduceLeft((df1, df2) => combineOptionalDfs(df1, df2))

    unionedDf.map(df => selectLatestRecordsByRowNumber(df, List(idColumn), eventdateColumn))
  }
}

object StudentTransform {
  val StudentTransformedSink = "transform-student-rel"
  val StudentTransformService = "transform-student"
  val session: SparkSession = SparkSessionUtils.getSession(StudentTransformService)
  val service = new SparkBatchService(StudentTransformService, session)

  def main(args: Array[String]): Unit = {
    val transformer = new StudentTransform(session, service)
    service.run(transformer.transform())
  }
}
