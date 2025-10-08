package com.alefeducation.bigdata.batch

import org.scalatest.funspec.AnyFunSpec
import com.alefeducation.bigdata.batch.DeltaSink._

import scala.collection.immutable.ListMap

class DeltaSinkUtilsTest extends AnyFunSpec {

  val entity = "subject"
  val uniqueColumn = "id"
  val student_id = s"${entity}_${uniqueColumn}"
  val dimensionCols = ListMap(
    "uuid" -> "subject_id",
    "name" -> "subject_name",
    "online" -> "subject_online",
    "genSubject" -> "subject_gen_subject",
    "subject_status" -> "subject_status",
    "schoolGradeUuid" -> "grade_id",
    "occurredOn" -> "occurredOn"
  )

  describe(" deltaSink utility function getMatchExpr ") {

    val matchExprSeq = getMatchExprSeq(Seq(uniqueColumn), entity)
    assert(matchExprSeq.size == 2)

    it(s"should return ${entity}_${uniqueColumn} as match criteria ") {
      assert(matchExprSeq.filter(_.contains(student_id)).size == 1)
    }

    it(s"should return ${entity}_created_time as match criteria") {
      assert(matchExprSeq.filter(_.contains(s"${entity}_created_time")).size == 1)
    }

  }

  describe("deltaSink utility function getUpdateExpr") {
    val updateColumns = getUpdateColumns(dimensionCols)
    val updateMap = DeltaSink.getUpdateExpr(updateColumns, Action.CREATE, entity)

    // removal of occurredOn column
    assert(updateColumns.size == dimensionCols.size - 1)

    // removal of occurredOn column and addition of _dw_update_time and update_time udpate clause
    assert(updateMap.keys.size == (dimensionCols.size - 1) + 2)

  }

}