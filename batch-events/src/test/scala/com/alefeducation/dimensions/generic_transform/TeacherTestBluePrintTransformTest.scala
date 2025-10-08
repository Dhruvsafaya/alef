package com.alefeducation.dimensions.generic_transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.dimensions.BaseDimensionSpec
import org.apache.spark.sql.SparkSession
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

class TeacherTestBluePrintTransformTest extends SparkSuite with BaseDimensionSpec {
  val sprk: SparkSession = spark
  val service: SparkBatchService = mock[SparkBatchService]

  val MutatedExpectedFields = Set("ttb_test_blueprint_minor_version", "ttb_created_time", "ttb_test_blueprint_published_by_id", "ttb_test_blueprint_number_of_question", "ttb_dw_id", "ttb_test_blueprint_created_by_id", "ttb_dw_updated_time", "ttb_test_blueprint_updated_by_id", "ttb_deleted_time", "ttb_test_blueprint_revision_version", "ttb_test_blueprint_domain_id", "ttb_updated_time", "ttb_test_blueprint_id", "ttb_test_blueprint_title", "ttb_test_blueprint_status", "ttb_test_blueprint_guidance_type", "ttb_dw_created_time", "ttb_test_blueprint_class_id", "ttb_test_blueprint_major_version", "ttb_tenant_id", "ttb_status")

  test("Should transform for dim teacher test blueprint table") {

    val created_value =
      """
        |[{
        |	"eventType": "TestBlueprintCreatedIntegrationEvent",
        |	"id": "b95a7743-4e45-4ffc-986a-99de62396038",
        |	"contextFrameClassId": "PREMIUM",
        |	"contextFrameTitle": "ENABLED",
        |	"contextDomainId": "meme.png",
        |	"guidanceType": "5744b599-8a80-4d9d-a299-ae72e7cfd42c",
        |	"guidanceVersion": {
        |   "major": 1,
        |   "minor": 2,
        |   "revision": 3
        | },
        |	"createdBy": "5744b599-8a80-4d9d-a299-ae72e7cfd42q",
        |	"status": "DRAFT",
        | "occurredOn": "2023-04-27 07:39:32.000",
        | "tenantId": "5744b599-8a80-4d9d-a299-ae72e7cfd42q",
        |	"eventDateDw": 20230427
        |}]
      """.stripMargin
    val updated_value =
      """
        |{
        |	"eventType": "TestBlueprintPublishedIntegrationEvent",
        | "id": "b95a7743-4e45-4ffc-986a-99de62396038",
        |	"contextFrameClassId": "PREMIUM",
        |	"contextFrameTitle": "ENABLED",
        |	"contextDomainId": "meme.png",
        |	"guidanceType": "5744b599-8a80-4d9d-a299-ae72e7cfd42c",
        |	"guidanceVersion": {
        |   "major": 1,
        |   "minor": 2,
        |   "revision": 3
        | },
        | "noOfQuestions": 1,
        |	"createdBy": "5744b599-8a80-4d9d-a299-ae72e7cfd42q",
        |	"updatedBy": "5744b599-8a80-4d9d-a299-ae72e7cfd42p",
        |	"publishedBy": "5744b599-8a80-4d9d-a299-ae72e7cfd42o",
        |	"status": "published",
        | "occurredOn": "2023-04-27 07:39:32.000",
        | "tenantId": "5744b599-8a80-4d9d-a299-ae72e7cfd42q",
        |	"eventDateDw": 20230427
        |}
      """.stripMargin

    val sprk = spark
    import sprk.implicits._

    when(service.getStartIdUpdateStatus("dim_teacher_test_blueprint")).thenReturn(1)

    val EventDF = spark.read.json(Seq(created_value).toDS())
    when(service.readOptional("parquet-teacher-test-blueprint-created-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(EventDF))
    when(service.readOptional("parquet-teacher-test-blueprint-published-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(spark.read.json(Seq(updated_value).toDS())))
    when(service.readOptional("parquet-teacher-test-blueprint-deleted-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(None)
    when(service.readOptional("parquet-teacher-test-blueprint-archived-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(None)
    when(service.readOptional("delta-teacher-test-blueprint-sink", sprk)).thenReturn(None)

    val transformer = new GenericSCD1Transformation(sprk, service, "teacher-test-blueprint-mutated-transform")
    val sinks = transformer.transform()

    val df = sinks.get.output

    assert(df.columns.toSet === MutatedExpectedFields)
    assert(df.count === 1)
  }

  test("Should transform for dim teacher test blueprint discarded event") {
    val deletedExpectedFields = Set("ttb_test_blueprint_minor_version", "ttb_created_time", "ttb_dw_id", "ttb_test_blueprint_created_by_id", "ttb_dw_updated_time", "ttb_test_blueprint_updated_by_id", "ttb_deleted_time", "ttb_test_blueprint_revision_version", "ttb_test_blueprint_domain_id", "ttb_updated_time", "ttb_test_blueprint_id", "ttb_test_blueprint_title", "ttb_test_blueprint_status", "ttb_test_blueprint_guidance_type", "ttb_dw_created_time", "ttb_test_blueprint_class_id", "ttb_test_blueprint_major_version", "ttb_tenant_id", "ttb_status")

    val created_value =
      """
        |[{
        |	"eventType": "TestBlueprintCreatedIntegrationEvent",
        |	"id": "93df3db8-f7fc-47a1-ad2d-127ea1580691",
        |	"contextFrameClassId": "PREMIUM",
        |	"contextFrameTitle": "ENABLED",
        |	"contextDomainId": "meme.png",
        |	"guidanceType": "5744b599-8a80-4d9d-a299-ae72e7cfd42c",
        |	"guidanceVersion": {
        |   "major": 1,
        |   "minor": 2,
        |   "revision": 3
        | },
        |	"createdBy": "5744b599-8a80-4d9d-a299-ae72e7cfd42q",
        |	"status": "DRAFT",
        | "occurredOn": "2023-04-27 07:39:32.000",
        | "tenantId": "5744b599-8a80-4d9d-a299-ae72e7cfd42q",
        |	"eventDateDw": 20230427
        |}]
      """.stripMargin
    val updated_value =
      """
        |{
        |	 "eventType": "TestBlueprintDiscardedIntegrationEvent",
        |  "id": "93df3db8-f7fc-47a1-ad2d-127ea1580691",
        |  "updatedBy": "b484d15f-1559-4fa1-9f9f-f6fe139943c4",
        |  "updatedAt": "2025-01-08T20:37:31.876054",
        |  "status": "DISCARDED",
        | "occurredOn": "2023-04-27 07:50:32.000",
        | "tenantId": "5744b599-8a80-4d9d-a299-ae72e7cfd42q",
        |	"eventDateDw": 20230427
        |}
      """.stripMargin

    val sprk = spark
    import sprk.implicits._

    when(service.getStartIdUpdateStatus("dim_teacher_test_blueprint")).thenReturn(1)

    val EventDF = spark.read.json(Seq(created_value).toDS())
    when(service.readOptional("parquet-teacher-test-blueprint-created-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(EventDF))
    when(service.readOptional("parquet-teacher-test-blueprint-published-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(None)
    when(service.readOptional("parquet-teacher-test-blueprint-deleted-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(spark.read.json(Seq(updated_value).toDS())))
    when(service.readOptional("parquet-teacher-test-blueprint-archived-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(None)
    when(service.readOptional("delta-teacher-test-blueprint-sink", sprk)).thenReturn(None)

    val transformer = new GenericSCD1Transformation(sprk, service, "teacher-test-blueprint-mutated-transform")
    val sinks = transformer.transform()

    val df = sinks.get.output

    assert(df.columns.toSet === deletedExpectedFields)
    assert(df.count === 1)
    df.first().getAs[Int]("ttb_status") shouldBe 4
    df.first().getAs[String]("ttb_test_blueprint_status") shouldBe "DISCARDED"
  }

  test("Should transform for dim teacher test blueprint archived event") {
    val deletedExpectedFields = Set("ttb_test_blueprint_minor_version", "ttb_created_time", "ttb_dw_id", "ttb_test_blueprint_created_by_id", "ttb_dw_updated_time", "ttb_test_blueprint_updated_by_id", "ttb_deleted_time", "ttb_test_blueprint_revision_version", "ttb_test_blueprint_domain_id", "ttb_updated_time", "ttb_test_blueprint_id", "ttb_test_blueprint_title", "ttb_test_blueprint_status", "ttb_test_blueprint_guidance_type", "ttb_dw_created_time", "ttb_test_blueprint_class_id", "ttb_test_blueprint_major_version", "ttb_tenant_id", "ttb_status")

    val created_value =
      """
        |[{
        |	"eventType": "TestBlueprintCreatedIntegrationEvent",
        |	"id": "93df3db8-f7fc-47a1-ad2d-127ea1580691",
        |	"contextFrameClassId": "PREMIUM",
        |	"contextFrameTitle": "ENABLED",
        |	"contextDomainId": "meme.png",
        |	"guidanceType": "5744b599-8a80-4d9d-a299-ae72e7cfd42c",
        |	"guidanceVersion": {
        |   "major": 1,
        |   "minor": 2,
        |   "revision": 3
        | },
        |	"createdBy": "5744b599-8a80-4d9d-a299-ae72e7cfd42q",
        |	"status": "DRAFT",
        | "occurredOn": "2023-04-27 07:39:32.000",
        | "tenantId": "5744b599-8a80-4d9d-a299-ae72e7cfd42q",
        |	"eventDateDw": 20230427
        |}]
      """.stripMargin
    val updated_value =
      """
        |{
        |	 "eventType": "TestBlueprintArchivedIntegrationEvent",
        |  "id": "93df3db8-f7fc-47a1-ad2d-127ea1580691",
        |  "updatedBy": "b484d15f-1559-4fa1-9f9f-f6fe139943c4",
        |  "updatedAt": "2025-01-08T20:37:31.876054",
        |  "status": "ARCHIVED",
        | "occurredOn": "2023-04-27 07:50:32.000",
        | "tenantId": "5744b599-8a80-4d9d-a299-ae72e7cfd42q",
        |	"eventDateDw": 20230427
        |}
      """.stripMargin

    val sprk = spark
    import sprk.implicits._

    when(service.getStartIdUpdateStatus("dim_teacher_test_blueprint")).thenReturn(1)

    val EventDF = spark.read.json(Seq(created_value).toDS())
    when(service.readOptional("parquet-teacher-test-blueprint-created-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(EventDF))
    when(service.readOptional("parquet-teacher-test-blueprint-published-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(None)
    when(service.readOptional("parquet-teacher-test-blueprint-deleted-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(None)
    when(service.readOptional("parquet-teacher-test-blueprint-archived-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(spark.read.json(Seq(updated_value).toDS())))
    when(service.readOptional("delta-teacher-test-blueprint-sink", sprk)).thenReturn(None)

    val transformer = new GenericSCD1Transformation(sprk, service, "teacher-test-blueprint-mutated-transform")
    val sinks = transformer.transform()

    val df = sinks.get.output

    assert(df.columns.toSet === deletedExpectedFields)
    assert(df.count === 1)
    df.first().getAs[Int]("ttb_status") shouldBe 4
    df.first().getAs[String]("ttb_test_blueprint_status") shouldBe "ARCHIVED"
  }

  test("Should transform for dim teacher test blueprint table when only TestBlueprintPublishedIntegrationEvent is present") {
    val updated_value = """
                          |{
                          |	"eventType": "TestBlueprintPublishedIntegrationEvent",
                          | "id": "b95a7743-4e45-4ffc-986a-99de62396038",
                          |	"contextFrameClassId": "PREMIUM",
                          |	"contextFrameTitle": "ENABLED",
                          |	"contextDomainId": "meme.png",
                          |	"guidanceType": "5744b599-8a80-4d9d-a299-ae72e7cfd42c",
                          |	"guidanceVersion": {
                          |   "major": 1,
                          |   "minor": 2,
                          |   "revision": 3
                          | },
                          | "noOfQuestions": 1,
                          |	"createdBy": "5744b599-8a80-4d9d-a299-ae72e7cfd42q",
                          |	"updatedBy": "5744b599-8a80-4d9d-a299-ae72e7cfd42p",
                          |	"publishedBy": "5744b599-8a80-4d9d-a299-ae72e7cfd42o",
                          |	"status": "published",
                          | "occurredOn": "2023-04-27 07:39:32.000",
                          | "tenantId": "5744b599-8a80-4d9d-a299-ae72e7cfd42q",
                          |	"eventDateDw": 20230427
                          |}
      """.stripMargin

    val dwh_value = """
                      |[{
                      |"ttb_test_blueprint_title":"ENABLED",
                      |"ttb_test_blueprint_domain_id":"meme.png",
                      |"ttb_test_blueprint_class_id":"PREMIUM",
                      |"ttb_status":1,
                      |"ttb_test_blueprint_revision_version":3,
                      |"ttb_test_blueprint_minor_version":2,
                      |"ttb_test_blueprint_id":"b95a7743-4e45-4ffc-986a-99de62396038",
                      |"ttb_test_blueprint_status":"published",
                      |"ttb_test_blueprint_created_by_id":"5744b599-8a80-4d9d-a299-ae72e7cfd42q",
                      |"ttb_test_blueprint_major_version":1,
                      |"ttb_tenant_id":"5744b599-8a80-4d9d-a299-ae72e7cfd42q",
                      |"ttb_test_blueprint_guidance_type":"5744b599-8a80-4d9d-a299-ae72e7cfd42c",
                      |"ttb_created_time":"2023-04-27T07:39:32.000Z",
                      |"ttb_dw_created_time":"2024-12-25T09:18:27.230Z",
                      |"ttb_updated_time":"2023-04-27T07:39:32.000Z",
                      |"ttb_dw_updated_time":"2024-12-25T09:18:27.339Z",
                      |"ttb_test_blueprint_number_of_question":1,
                      |"ttb_test_blueprint_published_by_id":"5744b599-8a80-4d9d-a299-ae72e7cfd42o",
                      |"ttb_test_blueprint_updated_by_id":"5744b599-8a80-4d9d-a299-ae72e7cfd42p",
                      |"ttb_dw_id":1}
                      |]
      """.stripMargin

    val sprk = spark
    import sprk.implicits._

    when(service.getStartIdUpdateStatus("dim_teacher_test_blueprint")).thenReturn(1)

    when(service.readOptional("parquet-teacher-test-blueprint-created-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(None)
    when(service.readOptional("parquet-teacher-test-blueprint-published-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(spark.read.json(Seq(updated_value).toDS())))
    when(service.readOptional("parquet-teacher-test-blueprint-published-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(spark.read.json(Seq(updated_value).toDS())))
    when(service.readOptional("delta-teacher-test-blueprint-sink",  sprk)).thenReturn(Some(spark.read.json(Seq(dwh_value).toDS())))

    val transformer = new GenericSCD1Transformation(sprk, service, "teacher-test-blueprint-mutated-transform")
    val sinks = transformer.transform()

    val df = sinks.get.output

    assert(df.columns.toSet === MutatedExpectedFields)
    assert(df.count === 1)
  }

  test("Should transform for dim teacher test blueprint lesson association table") {
    val MutatedExpectedFieldsForLessonAssociation = Set("ttbla_lesson_id", "ttbla_status", "ttbla_test_blueprint_id", "ttbla_created_time", "ttbla_dw_created_time", "ttbla_dw_id", "ttbla_active_until")
    val publishedValue =
      """
        |{
        |	"eventType": "TestBlueprintPublishedIntegrationEvent",
        | "id": "b95a7743-4e45-4ffc-986a-99de62396038",
        |	"contextFrameClassId": "PREMIUM",
        |	"contextFrameTitle": "ENABLED",
        |	"contextDomainId": "meme.png",
        |	"guidanceType": "5744b599-8a80-4d9d-a299-ae72e7cfd42c",
        |	"guidanceVersion": {
        |   "major": 1,
        |   "minor": 2,
        |   "revision": 3
        | },
        | "guidanceVariableLessons": [{"id": "5744b599-8a80-4d9d-a299-ae72e7cfd42r"
        | },{"id": "5744b599-8a80-4d9d-a299-ae72e7cfd42r"
        | }],
        | "noOfQuestions": 1,
        |	"createdBy": "5744b599-8a80-4d9d-a299-ae72e7cfd42q",
        |	"updatedBy": "5744b599-8a80-4d9d-a299-ae72e7cfd42p",
        |	"publishedBy": "5744b599-8a80-4d9d-a299-ae72e7cfd42o",
        |	"status": "published",
        | "occurredOn": "2023-04-27 07:39:32.000",
        | "tenantId": "5744b599-8a80-4d9d-a299-ae72e7cfd42q",
        |	"eventDateDw": 20230427
        |}
      """.stripMargin

    val sprk = spark
    import sprk.implicits._

    when(service.getStartIdUpdateStatus("dim_teacher_test_blueprint_lesson_association")).thenReturn(1)

    when(service.readOptional("parquet-teacher-test-blueprint-published-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(spark.read.json(Seq(publishedValue).toDS())))
    when(service.readOptional("delta-teacher-test-sink", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(None)

    val transformer = new GenericSCD2Transformation(sprk, service, "teacher-test-blueprint-lesson-transform")
    val sinks = transformer.transform()
    val df = sinks.filter(_.name == "transformed_teacher_test_blueprint_lesson_association").head.output

    assert(df.columns.toSet === MutatedExpectedFieldsForLessonAssociation)
    assert(df.count === 1)
  }

  test("Should transform for dim teacher test table") {

    val created_value =
      """
        |[{
        |	"eventType": "TestCreatedIntegrationEvent",
        |	"testId": "test1",
        | "testBlueprintId": "blueprint1",
        | "items": [
        |   {
        |     "id": "item1",
        |     "type": "multiple_choice"
        |   }
        | ],
        | "contextFrameClassId": "class1",
        | "contextFrameTitle": "Math Test",
        | "contextFrameDescription": "Test for basic math skills",
        | "contextDomainId": "domain1",
        | "status": "CREATED",
        | "createdBy": "user1",
        | "createdAt": "2024-03-12T11:34:18.020Z",
        | "aggregateIdentifier": "agg1",
        | "occurredOn": "2023-04-27 07:39:32.000",
        | "tenantId": "5744b599-8a80-4d9d-a299-ae72e7cfd42q",
        |	"eventDateDw": 20230427
        |}]
      """.stripMargin
    val published_value =
      """
        |{
        |	"eventType":"TestPublishedIntegrationEvent",
        | "testId": "3c16d9fa-275c-4747-88c6-c477c4c31733",
        | "testBlueprintId": "blueprint-123",
        | "items": [
        |   {"id": "item1", "type": "multiple-choice"},
        |   {"id": "item2", "type": "true-false"}
        | ],
        | "contextFrameClassId": "context-123",
        | "contextFrameTitle": "Test Title",
        | "contextFrameDescription": "Test Description",
        | "contextDomainId": "domain-xyz",
        | "updatedBy": "b484d15f-1559-4fa1-9f9f-f6fe139943c4",
        | "updatedAt": "2024-03-26T11:16:02.936926457",
        | "publishedBy": "publish_user_id",
        | "publishedAt": "2024-03-26T11:16:02.936926457",
        | "status": "PUBLISHED",
        | "aggregateIdentifier": "3c16d9fa-275c-4747-88c6-c477c4c31733",
        | "occurredOn": "2023-04-27 07:39:32.000",
        | "tenantId": "5744b599-8a80-4d9d-a299-ae72e7cfd42q",
        |	"eventDateDw": 20230427
        |}
      """.stripMargin

    val sprk = spark
    import sprk.implicits._

    when(service.getStartIdUpdateStatus("dim_teacher_test")).thenReturn(1)

    val EventDF = spark.read.json(Seq(created_value).toDS())
    when(service.readOptional("parquet-teacher-test-created-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(EventDF))
    when(service.readOptional("parquet-teacher-test-published-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(spark.read.json(Seq(published_value).toDS())))
    when(service.readOptional("parquet-teacher-test-deleted-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(None)
    when(service.readOptional("parquet-teacher-test-archived-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(None)
    when(service.readOptional("delta-teacher-test-sink", sprk)).thenReturn(None)

    val transformer = new GenericSCD1Transformation(sprk, service, "teacher-test-mutated-transform")
    val sinks = transformer.transform()

    val df = sinks.get.output
    val expectedCols = Set("tt_dw_id",
      "tt_test_id",
      "tt_test_description",
      "tt_test_blueprint_id",
      "tt_tenant_id",
      "tt_test_class_id",
      "tt_test_title",
      "tt_test_domain_id",
      "tt_test_status",
      "tt_test_created_by_id",
      "tt_status",
      "tt_test_updated_by_id",
      "tt_test_published_by_id",
      "tt_created_time",
      "tt_updated_time",
      "tt_deleted_time",
      "tt_dw_created_time",
      "tt_dw_updated_time")
    assert(df.columns.toSet === expectedCols)
    assert(df.count === 1)
  }

  test("Should transform for dim teacher test discarded event") {

    val created_value =
      """
        |[{
        |	"eventType": "TestCreatedIntegrationEvent",
        |	"testId": "3c16d9fa-275c-4747-88c6-c477c4c31733",
        | "testBlueprintId": "blueprint1",
        | "items": [
        |   {
        |     "id": "item1",
        |     "type": "multiple_choice"
        |   }
        | ],
        | "contextFrameClassId": "class1",
        | "contextFrameTitle": "Math Test",
        | "contextFrameDescription": "Test for basic math skills",
        | "contextDomainId": "domain1",
        | "status": "CREATED",
        | "createdBy": "user1",
        | "createdAt": "2024-03-12T11:34:18.020Z",
        | "aggregateIdentifier": "agg1",
        | "occurredOn": "2023-04-27 07:39:32.000",
        | "tenantId": "5744b599-8a80-4d9d-a299-ae72e7cfd42q",
        |	"eventDateDw": 20230427
        |}]
      """.stripMargin
    val deleted_value =
      """
        |{
        |	"eventType":"TestDiscardedIntegrationEvent",
        | "testId": "3c16d9fa-275c-4747-88c6-c477c4c31733",
        | "status": "DISCARDED",
        | "updatedBy": "user123",
        | "updatedAt": "2024-03-12T11:34:18Z",
        | "aggregateIdentifier": "3c16d9fa-275c-4747-88c6-c477c4c31733",
        | "occurredOn": "2023-04-27 07:50:32.000",
        | "tenantId": "5744b599-8a80-4d9d-a299-ae72e7cfd42q",
        |	"eventDateDw": 20230427
        |}
      """.stripMargin

    val sprk = spark
    import sprk.implicits._

    when(service.getStartIdUpdateStatus("dim_teacher_test")).thenReturn(1)

    val EventDF = spark.read.json(Seq(created_value).toDS())
    when(service.readOptional("parquet-teacher-test-created-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(EventDF))
    when(service.readOptional("parquet-teacher-test-published-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(None)
    when(service.readOptional("parquet-teacher-test-deleted-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(spark.read.json(Seq(deleted_value).toDS())))
    when(service.readOptional("parquet-teacher-test-archived-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(None)

    when(service.readOptional("delta-teacher-test-sink", sprk)).thenReturn(None)

    val transformer = new GenericSCD1Transformation(sprk, service, "teacher-test-mutated-transform")
    val sinks = transformer.transform()

    val df = sinks.get.output
    assert(df.count === 1)
    df.first().getAs[Int]("tt_status") shouldBe 4
    df.first().getAs[String]("tt_test_status") shouldBe "DISCARDED"
  }

  test("Should transform for dim teacher test archived event") {

    val created_value =
      """
        |[{
        |	"eventType": "TestCreatedIntegrationEvent",
        |	"testId": "3c16d9fa-275c-4747-88c6-c477c4c31733",
        | "testBlueprintId": "blueprint1",
        | "items": [
        |   {
        |     "id": "item1",
        |     "type": "multiple_choice"
        |   }
        | ],
        | "contextFrameClassId": "class1",
        | "contextFrameTitle": "Math Test",
        | "contextFrameDescription": "Test for basic math skills",
        | "contextDomainId": "domain1",
        | "status": "CREATED",
        | "createdBy": "user1",
        | "createdAt": "2024-03-12T11:34:18.020Z",
        | "aggregateIdentifier": "agg1",
        | "occurredOn": "2023-04-27 07:39:32.000",
        | "tenantId": "5744b599-8a80-4d9d-a299-ae72e7cfd42q",
        |	"eventDateDw": 20230427
        |}]
      """.stripMargin
    val archived_value =
      """
        |{
        |	"eventType":"TestArchivedIntegrationEvent",
        | "testId": "3c16d9fa-275c-4747-88c6-c477c4c31733",
        | "status": "ARCHIVED",
        | "updatedBy": "user123",
        | "updatedAt": "2024-03-12T11:34:18Z",
        | "aggregateIdentifier": "3c16d9fa-275c-4747-88c6-c477c4c31733",
        | "occurredOn": "2023-04-27 07:50:32.000",
        | "tenantId": "5744b599-8a80-4d9d-a299-ae72e7cfd42q",
        |	"eventDateDw": 20230427
        |}
      """.stripMargin

    val sprk = spark
    import sprk.implicits._

    when(service.getStartIdUpdateStatus("dim_teacher_test")).thenReturn(1)

    val EventDF = spark.read.json(Seq(created_value).toDS())
    when(service.readOptional("parquet-teacher-test-created-source", sprk, extraProps = List(("mergeSchema", "true"))))
      .thenReturn(Some(EventDF))
    when(service.readOptional("parquet-teacher-test-published-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(None)
    when(service.readOptional("parquet-teacher-test-deleted-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(None)
    when(service.readOptional("parquet-teacher-test-archived-source", sprk, extraProps = List(("mergeSchema", "true"))))
      .thenReturn(Some(spark.read.json(Seq(archived_value).toDS())))

    when(service.readOptional("delta-teacher-test-sink", sprk)).thenReturn(None)

    val transformer = new GenericSCD1Transformation(sprk, service, "teacher-test-mutated-transform")
    val sinks = transformer.transform()

    val df = sinks.get.output
    assert(df.count === 1)
    df.first().getAs[Int]("tt_status") shouldBe 4
    df.first().getAs[String]("tt_test_status") shouldBe "ARCHIVED"
  }
}
