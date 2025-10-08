package com.alefeducation.dimensions.teacher

import com.alefeducation.base.SparkBatchService
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.SparkSessionUtils
import com.alefeducation.util.redshift.options.UpsertOptions

object TeacherRelUserRedshift {
  val TeacherRelRedshiftService = "redshift-rel-user-teacher"
  val TeacherRelTransformedSource = "transform-teacher-rel-user-sink"
  val TeacherRelRedshiftSink = "redshift-user-rel"
  private val session = SparkSessionUtils.getSession(TeacherRelRedshiftService)
  val service = new SparkBatchService(TeacherRelRedshiftService, session)

  def main(args: Array[String]): Unit = {
    service.run {
      val studentRelUser = service.readUniqueOptional(TeacherRelTransformedSource, session, uniqueColNames = List("user_id", "user_type"))
      studentRelUser.map(
        _.toRedshiftUpsertSink(
          TeacherRelRedshiftSink,
          UpsertOptions.RelUser.targetTableName,
          matchConditions = UpsertOptions.RelUser.matchConditions("TEACHER"),
          columnsToUpdate = UpsertOptions.RelUser.columnsToUpdate,
          columnsToInsert = UpsertOptions.RelUser.columnsToInsert,
          isStaging = true
        )
      )
    }
  }
}
