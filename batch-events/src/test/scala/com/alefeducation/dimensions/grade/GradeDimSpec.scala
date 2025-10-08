package com.alefeducation.dimensions.grade

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.dimensions.grade.transform.{GradeCreatedTransform, GradeDeletedTransform, GradeUpdatedTransform}
import com.alefeducation.dimensions.grade.transform.GradeCreatedTransform.GradeCreatedSourceName
import com.alefeducation.dimensions.grade.transform.GradeDeletedTransform.GradeDeletedSourceName
import com.alefeducation.dimensions.grade.transform.GradeUpdatedTransform.GradeUpdatedSourceName
import com.alefeducation.dimensions.BaseDimensionSpec
import com.alefeducation.dimensions.grade.delta.GradeCreatedDelta
import com.alefeducation.dimensions.grade.redshift.{GradeCreatedRedshift, GradeDeletedRedshift}
import org.apache.spark.sql.SparkSession
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

class GradeDimSpec extends SparkSuite with BaseDimensionSpec {

  val sprk: SparkSession = spark
  val service: SparkBatchService = mock[SparkBatchService]

  private val Entity = "grade"

  val MutatedExpectedFields = Set(
        s"${Entity}_created_time",
        s"${Entity}_updated_time",
        s"${Entity}_deleted_time",
        s"${Entity}_dw_created_time",
        s"${Entity}_dw_updated_time",
        s"${Entity}_id",
        s"${Entity}_name",
        s"${Entity}_k12grade",
        s"${Entity}_status",
        "academic_year_id",
        "tenant_id",
        "school_id"
      )

    val DeletedExpectedFields = Set(
      s"${Entity}_created_time",
      s"${Entity}_updated_time",
      s"${Entity}_deleted_time",
      s"${Entity}_dw_created_time",
      s"${Entity}_dw_updated_time",
      s"${Entity}_id",
      s"${Entity}_status"
    )

  test(" Testing GradeCreatedEvent") {
     val created_value = """
              |{
              |  "eventType": "GradeCreatedEvent",
              |  "uuid": "grade-id",
              |  "name": "grade-name",
              |  "k12Grade": 7,
              |  "occurredOn": "1970-07-14 02:40:00.0",
              |  "tenantId": "tenant-id",
              |  "schoolId": "school-id",
              |  "eventdate": "1970-07-14",
              |  "academicYearId": "e75e917c-c5bf-459c-a7b8-180cc43850c4"
              |}
      """.stripMargin

    val sprk = spark
    import sprk.implicits._

    val GradeCreatedEventEventDF = spark.read.json(Seq(created_value).toDS())
    when(service.readOptional(GradeCreatedSourceName, sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(GradeCreatedEventEventDF))

    val transformer = new GradeCreatedTransform(sprk, service)
    val sinks = transformer.transform()

    val df = sinks.head.get.output

    assert(df.columns.toSet === MutatedExpectedFields)
    assert(df.count === 1)
    assert[String](df, s"${Entity}_id", "grade-id")
    assert[Int](df, s"${Entity}_status", 1)
    assert[String](df, "academic_year_id", "e75e917c-c5bf-459c-a7b8-180cc43850c4")
    assert[String](df, "tenant_id", "tenant-id")
    assert[String](df, "school_id", "school-id")
    assert[String](df, s"${Entity}_name", "grade-name")

  }

  test("Testing GradeUpdatedEvent") {

    val updated_value = """
            |{
            |  "eventType": "GradeUpdatedEvent",
            |  "uuid": "grade-id",
            |  "name": "grade-name2",
            |  "k12Grade": 8,
            |  "occurredOn": "1970-07-14 02:40:00.0",
            |  "tenantId": "tenant-id",
            |  "schoolId": "school-id",
            |  "eventdate": "1970-07-14",
            |  "academicYearId": "e75e917c-c5bf-459c-a7b8-180cc43850c4"
            |}
      """.stripMargin

    val sprk = spark
    import sprk.implicits._

    val GradeUpdatedEventDF = spark.read.json(Seq(updated_value).toDS())
    when(service.readOptional(GradeUpdatedSourceName, sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(GradeUpdatedEventDF))

    val transformer = new GradeUpdatedTransform(sprk, service)
    val sinks = transformer.transform()

    val df = sinks.head.get.output

    assert(df.columns.toSet === MutatedExpectedFields)
    assert(df.count === 1)
    assert[String](df, s"${Entity}_id", "grade-id")
    assert[Int](df, s"${Entity}_status", 1)
    assert[String](df, "academic_year_id", "e75e917c-c5bf-459c-a7b8-180cc43850c4")
    assert[String](df, "tenant_id", "tenant-id")
    assert[String](df, "school_id", "school-id")
    assert[String](df, s"${Entity}_name", "grade-name2")
    assert[Int](df, s"${Entity}_k12grade", 8)
  }

  test("Testing GradeDeletedEvent") {

    val deleted_value = """
              |{
              |  "eventType": "GradeDeletedEvent",
              |  "uuid": "grade-id1",
              |  "occurredOn": "1970-07-14 02:40:00.0",
              |  "tenantId": "tenant-id",
              |  "eventdate": "1970-07-14"
              |}
      """.stripMargin

    val sprk = spark
    import sprk.implicits._

    val GradeDeletedEventDF = spark.read.json(Seq(deleted_value).toDS())
    when(service.readOptional(GradeDeletedSourceName, sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(GradeDeletedEventDF))

    val transformer = new GradeDeletedTransform(sprk, service)
    val sinks = transformer.transform()

    val df = sinks.head.get.output

    assert(df.count === 1)
    assert(df.columns.toSet === DeletedExpectedFields)
    assert[String](df, s"${Entity}_id", "grade-id1")
    assert[Int](df, s"${Entity}_status", 4)

  }

  test("GradeCreatedDelta columns should be correctly initialized") {
    val expectedColumns = Seq(
      "grade_name", "school_id", "grade_id", "academic_year_id", "grade_status",
      "tenant_id", "grade_k12grade", "grade_created_time", "grade_dw_created_time",
      "grade_updated_time", "grade_deleted_time", "grade_dw_updated_time"
    )
    assert(GradeCreatedDelta.columns === expectedColumns)
  }

  test("GradeDimensionCols should be correctly initialized") {
    val expectedColumns = Seq(
      "grade_id", "grade_name", "grade_k12grade", "academic_year_id", "tenant_id",
      "school_id", "grade_status", "grade_created_time", "grade_updated_time",
      "grade_deleted_time", "grade_dw_created_time", "grade_dw_updated_time"
    )
    assert(GradeCreatedRedshift.GradeDimensionCols === expectedColumns)
  }

  test("GradeColumnsDeletedEvent Redshift should be correctly initialized") {
    val expectedMap = Map(
      "grade_id" -> "temp_table.grade_id",
      "grade_dw_updated_time" -> "temp_table.grade_dw_created_time",
      "grade_deleted_time" -> "temp_table.grade_created_time",
      "grade_status" -> "temp_table.grade_status"
    )
    assert(GradeDeletedRedshift.GradeColumnsDeletedEvent === expectedMap)
  }



}
