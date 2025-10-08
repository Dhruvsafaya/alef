package com.alefeducation.sources

import java.sql.Timestamp
import java.util.Calendar

import com.alefeducation.schema.Schema._
import com.alefeducation.service.{DataSink, SparkBatchService}
import com.alefeducation.util.DataFrameUtility._
import com.alefeducation.util.Helpers._
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

case class StudentRS(student_dw_id: BigInt, student_id: String, student_username: String)

case class CurioStudent(adek_id: String, status: String, interaction_type: String, timestamp: Timestamp, topic_name: String, subject_id: String, role: String)

case class CurioTeacher(student: String, teacher: String, topic: String, createdat: String, updatedat: String)

class CurioDataTransformer(override val name: String, override val session: SparkSession) extends SparkBatchService {

  import session.implicits._

  override def transform(): List[DataSink] = {
    val studentDf = read(CsvCurioStudentSource, session, isMandatory = false, Option(schema[CurioStudent]))
    val teacherDf = read(CsvCurioTeacherSource, session, isMandatory = false, Option(schema[CurioTeacher]))

    val partitionBy = Map("partitionBy" -> "eventdate")

    val studentSink = if (!isEmpty(studentDf)) {
      val students = addEventDateColumns(studentDf, "timestamp")
      List(DataSink(ParquetCurioStudentSink, students.coalesce(1), partitionBy),
        DataSink(RedshiftCurioStudentSink,
          selectAs(
            students
              .withColumn("topic_name", substring(trim($"topic_name"), StartingIndex, EndIndexTopic)),
            curioStudent)))
    } else Nil

    val teacherSink = if (!isEmpty(teacherDf)) {
      val teachers = addEventDateColumns(teacherDf, "createdat")
      List(DataSink(ParquetCurioTeacherSink, teachers.coalesce(1), partitionBy),
        DataSink(RedshiftCurioTeacherSink, selectAs(teachers
          .withColumn("topic", substring($"topic", StartingIndex, EndIndexTopic)),
          curioTeacher)))
    } else Nil

    studentSink ++ teacherSink
  }

  private def addEventDateColumns(dataFrame: DataFrame, timeColumn: String) = {
    dataFrame.withColumn("eventDateDw", date_format($"$timeColumn", "yyyyMMdd").as[Int])
      .withColumn("eventdate", date_format($"$timeColumn", "yyyy-MM-dd"))
      .withColumn("loadtime", to_utc_timestamp(lit(currentUTCDate), Calendar.getInstance().getTimeZone.getID))
  }
}

object CurioData {
  def main(args: Array[String]): Unit = {
    new CurioDataTransformer("curio", SparkSessionUtils.getSession("curio")).run
  }
}
