package com.alefeducation.dimensions.section

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.{ExpectedFields, SparkSuite}
import com.alefeducation.dimensions.section.transform.SectionMutatedTransform.{sectionMutatedSource, sectionUpdatedTransformedSink}
import com.alefeducation.dimensions.section.transform.SectionStateChangedTransform.{sectionDeletedTransformedSink, sectionEnabledTransformedSink, sectionStateChangedSource}
import com.alefeducation.dimensions.section.transform.{SectionMutatedTransform, SectionStateChangedTransform}
import org.apache.spark.sql.SparkSession
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

class SectionTransformSpec extends SparkSuite {
  val sprk: SparkSession = spark
  val service: SparkBatchService = mock[SparkBatchService]

  val Entity = "section"

  val ExpectedMutatedFields: Set[String] = Set(
    s"${Entity}_created_time",
    s"${Entity}_updated_time",
    s"${Entity}_deleted_time",
    s"${Entity}_dw_created_time",
    s"${Entity}_dw_updated_time",
    s"${Entity}_id",
    s"${Entity}_enabled",
    s"${Entity}_alias",
    s"${Entity}_name",
    s"${Entity}_status",
    "tenant_id",
    "school_id",
    "grade_id",
    s"${Entity}_source_id"
  )

  val enabledDisabledExpectedCols: Set[String] = Set(
    s"${Entity}_status",
    s"${Entity}_enabled",
    s"${Entity}_updated_time",
    s"${Entity}_dw_updated_time",
    s"${Entity}_id",
    s"${Entity}_deleted_time",
    s"${Entity}_created_time",
    s"${Entity}_dw_created_time"
  )

  val deletedExpectedCols: Set[String] = Set(
    s"${Entity}_created_time",
    s"${Entity}_updated_time",
    s"${Entity}_deleted_time",
    s"${Entity}_dw_created_time",
    s"${Entity}_dw_updated_time",
    s"${Entity}_id",
    s"${Entity}_status"
  )


  test("Test SectionCreatedEvent") {
    val value = """
      |{
      |  "eventType": "SectionCreatedEvent",
      |  "uuid": "section-id",
      |  "name": "section-name",
      |  "section": "section A",
      |  "schoolId": "school-id",
      |  "gradeId": "grade-id",
      |  "schoolGradeName": "school-grade-name",
      |  "enabled": true,
      |  "occurredOn": "2020-03-14 02:40:00.0",
      |  "createdOn": "12345",
      |  "tenantId": "tenant-id",
      |  "sourceId": "sectionSourceId"
      |}
      """.stripMargin

    val sprk = spark
    import sprk.implicits._

    val inputDF = spark.read.json(Seq(value).toDS())
    when(service.readOptional(sectionMutatedSource, sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(inputDF))

    val transformer = new SectionMutatedTransform(sprk, service)
    val sinks = transformer.transform()

    val df = sinks.head.get.output

    assert(df.count === 1)
    assert(df.columns.toSet === ExpectedMutatedFields)
    assert[String](df, "section_id", "section-id")
    assert[Int](df, "section_status", 1)
    assert[String](df, "tenant_id", "tenant-id")
    assert[String](df, "school_id", "school-id")
    assert[String](df, "grade_id", "grade-id")
    assert[String](df, "section_alias", "section-name")
    assert[String](df, "section_source_id", "sectionSourceId")

  }

  test("Test SectionUpdatedEvent") {
    val value = """
      |{
      |  "eventType": "SectionUpdatedEvent",
      |  "uuid": "section-id",
      |  "name": "section-name",
      |  "section": "section A",
      |  "schoolId": "school-id",
      |  "gradeId": "grade-id",
      |  "schoolGradeName": "school-grade-name",
      |  "enabled": true,
      |  "occurredOn": "2020-03-14 02:40:00.0",
      |  "createdOn": "12345",
      |  "tenantId": "tenant-id",
      |  "sourceId": "sectionSourceId"
      |}
      """.stripMargin

    val sprk = spark
    import sprk.implicits._

    val inputDF = spark.read.json(Seq(value).toDS())
    when(service.readOptional(sectionMutatedSource, sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(inputDF))

    val transformer = new SectionMutatedTransform(sprk, service)
    val sinks = transformer.transform()

    val df = sinks.flatten.filter(_.name == sectionUpdatedTransformedSink).head.output

    assert(df.count === 1)
    assert(df.columns.toSet === ExpectedMutatedFields)
    assert[String](df, "section_id", "section-id")
    assert[Int](df, "section_status", 1)
    assert[String](df, "tenant_id", "tenant-id")
    assert[String](df, "school_id", "school-id")
    assert[String](df, "grade_id", "grade-id")
    assert[String](df, "section_alias", "section-name")
    assert[String](df, "section_source_id", "sectionSourceId")
  }

  test("section deleted in class dimension") {

    val value = """
      |{
      |  "eventType": "SectionDeletedEvent",
      |  "uuid": "section-id",
      |  "occurredOn": "2020-03-14 02:40:00.0",
      |  "createdOn": "12345",
      |  "tenantId": "tenant-id"
      |}
      """.stripMargin

    val sprk = spark
    import sprk.implicits._

    val inputDF = spark.read.json(Seq(value).toDS())
    when(service.readOptional(sectionStateChangedSource, sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(inputDF))

    val transformer = new SectionStateChangedTransform(sprk, service)
    val sinks = transformer.transform()

    val df = sinks.flatten.filter(_.name == sectionDeletedTransformedSink).head.output

    assert(df.count === 1)
    assert(df.columns.toSet === deletedExpectedCols)
    assert[String](df, "section_id", "section-id")
    assert[String](df, "section_created_time", "2020-03-14 02:40:00.0")
    assert[Int](df, "section_status", 4)
  }

  test("series of section enabled disabled and deleted events") {

    val value = """
      |[{
      |  "eventType": "SectionDisabledEvent",
      |  "uuid": "section-id",
      |  "createdOn": "123456",
      |  "occurredOn": "2020-03-14 02:40:00.0",
      |  "tenantId": "tenant-id"
      |},
      |{
      |  "eventType": "SectionEnabledEvent",
      |  "uuid": "section-id",
      |  "createdOn": "123457",
      |  "occurredOn": "2020-03-14 02:41:00.0",
      |  "tenantId": "tenant-id"
      |},
      |{
      |  "eventType": "SectionDeletedEvent",
      |  "uuid": "section-id",
      |  "createdOn": "123458",
      |  "occurredOn": "2020-03-14 02:42:00.0",
      |  "tenantId": "tenant-id"
      |},
      |{
      |  "eventType": "SectionDisabledEvent",
      |  "uuid": "section-id1",
      |  "createdOn": "234567",
      |  "occurredOn": "2020-03-16 02:40:00.0",
      |  "tenantId": "tenant-id"
      |},
      |{
      |  "eventType": "SectionEnabledEvent",
      |  "uuid": "section-id1",
      |  "createdOn": "234568",
      |  "occurredOn": "2020-03-16 02:41:00.0",
      |  "tenantId": "tenant-id"
      |}]
      """.stripMargin

    val sprk = spark
    import sprk.implicits._

    val inputDF = spark.read.json(Seq(value).toDS())
    when(service.readOptional(sectionStateChangedSource, sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(inputDF))

    val transformer = new SectionStateChangedTransform(sprk, service)
    val sinks = transformer.transform()

    val deletedDF = sinks.flatten.filter(_.name == sectionDeletedTransformedSink).head.output

    assert(deletedDF.columns.toSet === deletedExpectedCols)
    assert[String](deletedDF, "section_id", "section-id")
    assert[Int](deletedDF, "section_status", 4)
    assert[String](deletedDF, "section_created_time", "2020-03-14 02:42:00.0")

    val enabledDisabledDF = sinks.flatten.filter(_.name == sectionEnabledTransformedSink).head.output

    assert(enabledDisabledDF.columns.toSet === enabledDisabledExpectedCols)
    assert[String](enabledDisabledDF, s"${Entity}_id", "section-id")
    assert[Int](enabledDisabledDF, s"${Entity}_status", 1)
  }


  test("series of section enabled disabled") {

    val value =
      """
        |[{
        |  "eventType": "SectionDisabledEvent",
        |  "uuid": "section-id",
        |  "createdOn": "123456",
        |  "occurredOn": "2020-03-14 02:40:00.0",
        |  "tenantId": "tenant-id"
        |},
        |{
        |  "eventType": "SectionEnabledEvent",
        |  "uuid": "section-id",
        |  "createdOn": "123457",
        |  "occurredOn": "2020-03-14 02:41:00.0",
        |  "tenantId": "tenant-id"
        |},
        |{
        |  "eventType": "SectionDisabledEvent",
        |  "uuid": "section-id",
        |  "createdOn": "234567",
        |  "occurredOn": "2020-03-16 02:40:00.0",
        |  "tenantId": "tenant-id"
        |},
        |{
        |  "eventType": "SectionEnabledEvent",
        |  "uuid": "section-id",
        |  "createdOn": "234568",
        |  "occurredOn": "2020-03-16 02:41:00.0",
        |  "tenantId": "tenant-id"
        |}]
      """.stripMargin

    val sprk = spark
    import sprk.implicits._

    val inputDF = spark.read.json(Seq(value).toDS())
    when(service.readOptional(sectionStateChangedSource, sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(inputDF))

    val transformer = new SectionStateChangedTransform(sprk, service)
    val sinks = transformer.transform()

    val enabledDisabledDF = sinks.flatten.filter(_.name == sectionEnabledTransformedSink).head.output

    assert(enabledDisabledDF.columns.toSet === enabledDisabledExpectedCols)
    assert[String](enabledDisabledDF, "section_id", "section-id")
    assert[Int](enabledDisabledDF, "section_status", 1)
    assert[String](enabledDisabledDF, "section_created_time", "2020-03-16 02:41:00.0")
  }
}
