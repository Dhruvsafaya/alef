package com.alefeducation

import com.alefeducation.bigdata.batch.DeltaSink
import com.alefeducation.bigdata.batch.delta.Alias
import com.alefeducation.bigdata.commons.testutils.ExpectedFields.ExpectedField
import org.scalatest.matchers.must.Matchers
import com.alefeducation.util.Helpers.Deleted
import org.apache.spark.sql.types.TimestampType

package object dimensions {

  def getCommonExpectedTimestampFields(entity: String): List[ExpectedField] =
    ExpectedField(name = s"${entity}_deleted_time", dataType = TimestampType) ::
      getCommonExpectedWoDelTimestampFields(entity)

  def getCommonExpectedWoDelTimestampFields(entity: String): List[ExpectedField] = List(
    ExpectedField(name = s"${entity}_created_time", dataType = TimestampType),
    ExpectedField(name = s"${entity}_dw_created_time", dataType = TimestampType),
    ExpectedField(name = s"${entity}_updated_time", dataType = TimestampType),
    ExpectedField(name = s"${entity}_dw_updated_time", dataType = TimestampType)
  )

  def getCommonExpectedUpdateTimestampFields(entity: String): Map[String, String] = Map(
    s"${entity}_dw_updated_time" -> s"${entity}_dw_updated_time",
    s"${entity}_updated_time" -> s"${entity}_created_time",
    s"${entity}_dw_updated_time" -> s"${entity}_dw_created_time",
    s"${entity}_deleted_time" -> s"${entity}_deleted_time"
  )

  object DeltaTests extends Matchers {

    def assertDeltaUpdateFields(deltaSink: DeltaSink, expectedFields: Map[String, String]): Unit = {
      val update = deltaSink.config.operation.updateFields
      val expectedUpdate = expectedFields.map { case (k, v) => (k, s"${Alias.Events}.$v") }
      update must equal(expectedUpdate)
    }

    def assertDeltaUpdateFields(updateFields: Map[String, String], expectedFields: Map[String, String]): Unit = {

      val expectedUpdateFields = expectedFields.map { case (k, v) => (k, s"${Alias.Events}.$v") }
      updateFields must equal(expectedUpdateFields)
    }

    def assertDeltaDeleteFields(deltaSink: DeltaSink, entity: String): Unit = {
      val expectedFields = Map(
        s"${entity}_updated_time" -> s"${entity}_created_time",
        s"${entity}_dw_updated_time" -> s"${entity}_dw_created_time",
        s"${entity}_deleted_time" -> s"${entity}_created_time"
      )
      val delete = deltaSink.config.operation.updateFields
      val expectedUpdate = expectedFields.map { case (k, v) => (k, s"${Alias.Events}.$v") } ++ Map(s"${entity}_status" -> Deleted.toString)
      delete must equal(expectedUpdate)
    }

    def assertDeltaDeleteFields(fields: Map[String, String], entity: String): Unit = {
      val expectedFields = Map(
        s"${entity}_updated_time" -> s"${entity}_created_time",
        s"${entity}_dw_updated_time" -> s"${entity}_dw_created_time",
        s"${entity}_deleted_time" -> s"${entity}_created_time"
      )
      val expectedUpdate = expectedFields.map { case (k, v) => (k, s"${Alias.Events}.$v") } ++ Map(s"${entity}_status" -> Deleted.toString)
      fields must equal(expectedUpdate)
    }

    def assertDeltaSelectFields(deltaSink: DeltaSink, expected: String): Unit = {
      val select = deltaSink.config.operation.selectExpression
      select must equal(expected)
    }

  }

}
