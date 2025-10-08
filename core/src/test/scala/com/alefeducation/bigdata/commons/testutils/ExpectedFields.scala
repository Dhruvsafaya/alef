package com.alefeducation.bigdata.commons.testutils

import com.alefeducation.bigdata.Sink
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructField, StructType, TimestampType}
import org.scalatest.matchers.must.Matchers

import java.sql.Timestamp

object ExpectedFields extends Matchers {

  case class ExpectedField(name: String, dataType: DataType)

  def commonExpectedTimestampFields(entity: String) = List(
    ExpectedField(name = s"${entity}_created_time", dataType = TimestampType),
    ExpectedField(name = s"${entity}_dw_created_time", dataType = TimestampType),
    ExpectedField(name = s"${entity}_updated_time", dataType = TimestampType),
    ExpectedField(name = s"${entity}_deleted_time", dataType = TimestampType),
    ExpectedField(name = s"${entity}_dw_updated_time", dataType = TimestampType)
  )

  def assertExpectedFields[T](dfFields: List[StructField], expectedFields: List[ExpectedField]): Unit = {
    dfFields must have length expectedFields.size
    val zipped = dfFields.sortWith(_.name < _.name) zip expectedFields.sortWith(_.name < _.name)
    zipped.foreach {
      case (field, expected) =>
        assert(field.name == expected.name)
        field.dataType match {
          case _: ArrayType =>
            assertDataTypeEqualsStructurally(field.dataType, expected.dataType)
          case _ => assert(field.dataType == expected.dataType)
        }
    }

    def assertDataTypeEqualsStructurally(from: DataType, to: DataType, ignoreNullability: Boolean = false): Unit = {
      // it's org.apache.spark.sql.types.DataType#equalsStructurally with StructType sorting by name
      def equalsStructurally(from: DataType, to: DataType, ignoreNullability: Boolean = false): Boolean = {
        (from, to) match {
          case (left: ArrayType, right: ArrayType) =>
            equalsStructurally(left.elementType, right.elementType) &&
              (ignoreNullability || left.containsNull == right.containsNull)

          case (left: MapType, right: MapType) =>
            equalsStructurally(left.keyType, right.keyType) &&
              equalsStructurally(left.valueType, right.valueType) &&
              (ignoreNullability || left.valueContainsNull == right.valueContainsNull)

          case (StructType(fromFields), StructType(toFields)) =>
            fromFields.length == toFields.length &&
              fromFields
                .sortWith(_.name < _.name)
                .zip(toFields.sortWith(_.name < _.name))
                .forall {
                  case (l, r) =>
                    equalsStructurally(l.dataType, r.dataType) &&
                      (ignoreNullability || l.nullable == r.nullable)
                }

          case (fromDataType, toDataType) => fromDataType == toDataType
        }
      }
      assert(equalsStructurally(from, to, ignoreNullability), s"\nActual data type:\n$from\ndiffers from expected:\n$to\n")
    }
  }

  def assertUserSink(userSink: List[Sink], expected: List[(String, String, String)]) = {
    val userDf = userSink.head.output
    val expectedRows = expected.map(a => (a._1, Timestamp.valueOf(a._2), a._3))
    val users = userDf.collect().toList.map(row =>
      (row.getAs[String]("user_id"),
        row.getAs[Timestamp]("user_created_time"),
        row.getAs[String]("user_type")))
    assert(users == expectedRows)
  }
}
