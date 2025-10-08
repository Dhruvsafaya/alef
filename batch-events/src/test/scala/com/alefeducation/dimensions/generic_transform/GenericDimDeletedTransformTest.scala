package com.alefeducation.dimensions.generic_transform


import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.dimensions.BaseDimensionSpec
import org.apache.spark.sql.SparkSession
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

class GenericDimDeletedTransformTest extends SparkSuite with BaseDimensionSpec {
  val sprk: SparkSession = spark
  val service: SparkBatchService = mock[SparkBatchService]

  private val Entity = "academic_calendar"

  val MutatedExpectedFields = Set(
    s"${Entity}_created_time",
    s"${Entity}_updated_time",
    s"${Entity}_deleted_time",
    s"${Entity}_dw_created_time",
    s"${Entity}_dw_updated_time",
    s"${Entity}_id",
    s"${Entity}_title",
    s"${Entity}_is_default",
    s"${Entity}_academic_year_id",
    s"${Entity}_tenant_id",
    s"${Entity}_school_id",
    s"${Entity}_organization",
    s"${Entity}_type",
    s"${Entity}_created_by_id",
    s"${Entity}_updated_by_id",
    s"${Entity}_status",
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

  test("Should transform updated event") {

    val created_value = """
                          |{
                          |  "eventType": "AcademicCalendarUpdatedEvent",
                          |  "tenantId": "tenant-id",
                          |  "uuid": "e75e917c-c5bf-459c-a7b8-180cc4385054",
                          |  "occurredOn": "1970-07-14 02:40:00.0",
                          |  "title": "updated",
                          |  "default": "school-id",
                          |  "type": "1970-07-14",
                          |  "academicYearId": "e75e917c-c5bf-459c-a7b8-180cc43850c4",
                          |   "schoolId":"school-id",
                          |   "organization": "organizaion-id",
                          |   "createdBy": "e75e917c-c5bf-459c-a7b8-908cc43850c4",
                          |   "updatedBy": "e75e917c-c5bf-459c-a7b8-oiucc43850c4",
                          |   "createdOn": "1970-07-14 02:10:00.0",
                          |   "updatedOn": "1970-07-14 02:40:00.1",
                          |   "eventDateDw": "12/12/2024"
                          |}
      """.stripMargin

    val sprk = spark
    import sprk.implicits._

    val EventDF = spark.read.json(Seq(created_value).toDS())
    when(service.readOptional("parquet-academic-calendar-deleted-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(EventDF))

    val transformer = new GenericDimDeletedTransform(sprk, service, "academic-calendar-transform")
    val sinks = transformer.transform()

    val df = sinks.get.output

    assert(df.columns.toSet === MutatedExpectedFields)
    assert(df.count === 1)
    assert[String](df, s"${Entity}_id", "e75e917c-c5bf-459c-a7b8-180cc4385054")
    assert[String](df, s"${Entity}_academic_year_id", "e75e917c-c5bf-459c-a7b8-180cc43850c4")
    assert[String](df, s"${Entity}_tenant_id", "tenant-id")
    assert[String](df, s"${Entity}_school_id", "school-id")
  }
}
