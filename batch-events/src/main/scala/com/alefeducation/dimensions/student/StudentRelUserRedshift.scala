package com.alefeducation.dimensions.student

import com.alefeducation.base.SparkBatchService
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.SparkSessionUtils
import com.alefeducation.util.redshift.options.UpsertOptions

object StudentRelUserRedshift {
  val StudentRelRedshiftService = "redshift-rel-user-student"
  val StudentRelTransformedSource = "transform-user-rel"
  val StudentRelRedshiftSink = "redshift-user-rel"
  private val session = SparkSessionUtils.getSession(StudentRelRedshiftService)
  val service = new SparkBatchService(StudentRelRedshiftService, session)

  def main(args: Array[String]): Unit = {
    service.run {
      val studentRelUser = service.readUniqueOptional(StudentRelTransformedSource, session, uniqueColNames = List("user_id", "user_type"))
      studentRelUser.map(
        _.toRedshiftUpsertSink(
          StudentRelRedshiftSink,
          UpsertOptions.RelUser.targetTableName,
          matchConditions = UpsertOptions.RelUser.matchConditions("STUDENT"),
          columnsToUpdate = UpsertOptions.RelUser.columnsToUpdate,
          columnsToInsert = UpsertOptions.RelUser.columnsToInsert,
          isStaging = true
        )
      )
    }
  }
}
