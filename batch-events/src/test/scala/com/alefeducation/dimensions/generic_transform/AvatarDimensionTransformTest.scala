package com.alefeducation.dimensions.generic_transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.dimensions.BaseDimensionSpec
import org.apache.spark.sql.SparkSession
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

class AvatarDimensionTransformTest extends SparkSuite with BaseDimensionSpec {
  val sprk: SparkSession = spark
  val service: SparkBatchService = mock[SparkBatchService]

  private val Entity = "avatar"
  val MutatedExpectedFields = Set(
    s"${Entity}_created_time",
    s"${Entity}_updated_time",
    s"${Entity}_deleted_time",
    s"${Entity}_dw_created_time",
    s"${Entity}_dw_updated_time",
    s"${Entity}_dw_id",
    s"${Entity}_id",
    s"${Entity}_file_id",
    s"${Entity}_app_status",
    s"${Entity}_type",
    s"${Entity}_name",
    s"${Entity}_description",
    s"${Entity}_valid_from",
    s"${Entity}_valid_till",
    s"${Entity}_category",
    s"${Entity}_star_cost",
    s"${Entity}_genders",
    s"${Entity}_tenants",
    s"${Entity}_organizations",
    s"${Entity}_status",
    s"${Entity}_created_by",
    s"${Entity}_created_at",
    s"${Entity}_updated_by",
    s"${Entity}_updated_at",
    s"${Entity}_is_enabled_for_all_orgs"
  )

  test("Should transform created, updated and deleted comes in same batch") {

    val created_value = """
                          |[{
                          |	"eventType": "AvatarCreatedEvent",
                          |	"id": "b95a7743-4e45-4ffc-986a-99de62396038",
                          |	"type": "PREMIUM",
                          |	"status": "ENABLED",
                          |	"name": "meme.png",
                          |	"fileId": "5744b599-8a80-4d9d-a299-ae72e7cfd42c",
                          |	"description": "some description",
                          |	"validFrom": "2024-04-23T10:35:36.418102203",
                          |	"validTill": "2024-04-29T20:00:00",
                          |	"organizations": [
                          |		"MSCL",
                          |		"NC"
                          |	],
                          |	"customization": null,
                          |	"createdAt": "2024-04-23T10:35:36.41813526",
                          |	"createdBy": "ebe4d97e-c2c0-481f-b90a-ccd5138234ee",
                          |	"updatedAt": "2024-04-23T10:35:36.418140282",
                          |	"updatedBy": "ebe4d97e-c2c0-481f-b90a-ccd5138234ee",
                          |	"starCost": 1,
                          |	"genders": [
                          |		"MALE"
                          |	],
                          |	"category": "CHARACTERS",
                          |	"tenants": [
                          |		"e2e"
                          |	],
                          |	"eventId": "3d7ac395-cfe6-4ad6-8ac7-f792f7d2cb0c",
                          | "isEnabledForAllOrgs": true,
                          | "occurredOn": "2023-04-27 07:39:32.000",
                          |	"eventDateDw": 20230427
                          |}]
      """.stripMargin
    val updated_value = """
                          |{
                          |	"eventType": "AvatarUpdatedEvent",
                          |	"type": "PREMIUM",
                          |	"status": "ENABLED",
                          |	"name": "meme_updated.png",
                          |	"id": "b95a7743-4e45-4ffc-986a-99de62396038",
                          |	"fileId": "5744b599-8a80-4d9d-a299-ae72e7cfd42c",
                          |	"description": "some description",
                          |	"validFrom": "2024-04-23T10:35:36.418102203",
                          |	"validTill": "2024-04-29T20:00:00",
                          |	"organizations": [
                          |		"MSCL",
                          |		"NC"
                          |	],
                          |	"customization": null,
                          |	"createdAt": "2024-04-23T10:35:36.41813526",
                          |	"createdBy": "ebe4d97e-c2c0-481f-b90a-ccd5138234ee",
                          |	"updatedAt": "2024-04-23T10:35:36.418140282",
                          |	"updatedBy": "ebe4d97e-c2c0-481f-b90a-ccd5138234ee",
                          |	"starCost": 1,
                          |	"genders": [
                          |		"MALE"
                          |	],
                          |	"category": "CHARACTERS",
                          |	"tenants": [
                          |		"e2e"
                          |	],
                          |	"eventId": "3d7ac395-cfe6-4ad6-8ac7-f792f7d2cb0c",
                          | "isEnabledForAllOrgs": true,
                          | "occurredOn": "2023-04-28 07:39:32.000",
                          |	"eventDateDw": 20230428
                          |}
      """.stripMargin
    val deleted_value = """
                          |{
                          |	"eventType": "AvatarDeletedEvent",
                          |	"avatarId": "b95a7743-4e45-4ffc-986a-99de62396038",
                          |	"id": "b95a7743-4e45-4ffc-986a-99de62396038",
                          |	"fileId": "5744b599-8a80-4d9d-a299-ae72e7cfd42c",
                          |	"deletedBy": "ebe4d97e-c2c0-481f-b90a-ccd5138234ee",
                          |	"deletedAt": "2024-04-23T10:42:25.553661907",
                          |	"avatarType": "PREMIUM",
                          |	"avatarCategory": "OTHERS",
                          |	"eventId": "610dbdaa-8680-41f7-821a-1d42e65dc967",
                          | "isEnabledForAllOrgs": true,
                          | "occurredOn": "2023-04-29 07:39:32.000",
                          |	"eventDateDw": 20230429
                          |}
      """.stripMargin

    val sprk = spark
    import sprk.implicits._

    when(service.getStartIdUpdateStatus("dim_avatar")).thenReturn(1)

    val EventDF = spark.read.json(Seq(created_value).toDS())
    when(service.readOptional("parquet-ccl-marketplace-avatar-created-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(EventDF))
    when(service.readOptional("parquet-ccl-marketplace-avatar-updated-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(spark.read.json(Seq(updated_value).toDS())))
    when(service.readOptional("parquet-ccl-marketplace-avatar-deleted-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(spark.read.json(Seq(deleted_value).toDS())))
    when(service.readOptional("parquet-marketplace-avatar-created-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(EventDF))
    when(service.readOptional("parquet-marketplace-avatar-updated-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(spark.read.json(Seq(updated_value).toDS())))
    when(service.readOptional("parquet-marketplace-avatar-deleted-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(spark.read.json(Seq(deleted_value).toDS())))
    when(service.readOptional("delta-marketplace-avatar-sink", sprk)).thenReturn(None)

    val transformer = new GenericSCD1Transformation(sprk, service, "ccl-marketplace-avatar-mutated-transform")
    val sinks = transformer.transform()

    val df = sinks.get.output

    assert(df.columns.toSet === MutatedExpectedFields)
    assert(df.count === 1)
    assert[String](df, s"${Entity}_id", "b95a7743-4e45-4ffc-986a-99de62396038")

    val orgs = df.first()
    orgs.getAs[Array[String]](s"${Entity}_organizations") shouldBe  Array("MSCL", "NC")
    assert[Int](df, s"${Entity}_dw_id", 1)
    assert[String](df, s"${Entity}_id", "b95a7743-4e45-4ffc-986a-99de62396038")
    assert[String](df, s"${Entity}_file_id", "5744b599-8a80-4d9d-a299-ae72e7cfd42c")
    assert[String](df, s"${Entity}_name", "meme_updated.png")
    assert[String](df, s"${Entity}_created_time", "2023-04-27 07:39:32.0")
    assert[String](df, s"${Entity}_updated_time", "2023-04-28 07:39:32.0")
    assert[String](df, s"${Entity}_deleted_time", "2023-04-29 07:39:32.0")
    assert[Boolean](df, s"${Entity}_is_enabled_for_all_orgs", true)
    assert[Int](df, s"${Entity}_status", 4)

  }

  test("Should transform created and update event") {

    val created_value = """
                          |[{
                          |	"eventType": "AvatarCreatedEvent",
                          |	"id": "b95a7743-4e45-4ffc-986a-99de62396038",
                          |	"type": "PREMIUM",
                          |	"status": "ENABLED",
                          |	"name": "meme.png",
                          |	"fileId": "5744b599-8a80-4d9d-a299-ae72e7cfd42c",
                          |	"description": "some description",
                          |	"validFrom": "2024-04-23T10:35:36.418102203",
                          |	"validTill": "2024-04-29T20:00:00",
                          |	"organizations": [
                          |		"MSCL",
                          |		"NC"
                          |	],
                          |	"customization": null,
                          |	"createdAt": "2024-04-23T10:35:36.41813526",
                          |	"createdBy": "ebe4d97e-c2c0-481f-b90a-ccd5138234ee",
                          |	"updatedAt": "2024-04-23T10:35:36.418140282",
                          |	"updatedBy": "ebe4d97e-c2c0-481f-b90a-ccd5138234ee",
                          |	"starCost": 1,
                          |	"genders": [
                          |		"MALE"
                          |	],
                          |	"category": "CHARACTERS",
                          |	"tenants": [
                          |		"e2e"
                          |	],
                          |	"eventId": "3d7ac395-cfe6-4ad6-8ac7-f792f7d2cb0c",
                          | "isEnabledForAllOrgs": true,
                          | "occurredOn": "2023-04-27 07:39:32.000",
                          |	"eventDateDw": 20230427
                          |}]
      """.stripMargin
    val updated_value = """
                          |{
                          |	"eventType": "AvatarUpdatedEvent",
                          |	"type": "PREMIUM",
                          |	"status": "ENABLED",
                          |	"name": "meme_updated.png",
                          |	"id": "b95a7743-4e45-4ffc-986a-99de62396038",
                          |	"fileId": "5744b599-8a80-4d9d-a299-ae72e7cfd42c",
                          |	"description": "some description",
                          |	"validFrom": "2024-04-23T10:35:36.418102203",
                          |	"validTill": "2024-04-29T20:00:00",
                          |	"organizations": [
                          |		"MSCL",
                          |		"NC"
                          |	],
                          |	"customization": null,
                          |	"createdAt": "2024-04-23T10:35:36.41813526",
                          |	"createdBy": "ebe4d97e-c2c0-481f-b90a-ccd5138234ee",
                          |	"updatedAt": "2024-04-23T10:35:36.418140282",
                          |	"updatedBy": "ebe4d97e-c2c0-481f-b90a-ccd5138234ee",
                          |	"starCost": 1,
                          |	"genders": [
                          |		"MALE"
                          |	],
                          |	"category": "CHARACTERS",
                          |	"tenants": [
                          |		"e2e"
                          |	],
                          |	"eventId": "3d7ac395-cfe6-4ad6-8ac7-f792f7d2cb0c",
                          | "isEnabledForAllOrgs": true,
                          | "occurredOn": "2023-04-28 07:39:32.000",
                          |	"eventDateDw": 20230428
                          |}
      """.stripMargin

    val sprk = spark
    import sprk.implicits._

    when(service.getStartIdUpdateStatus("dim_avatar")).thenReturn(1)

    val EventDF = spark.read.json(Seq(created_value).toDS())
    when(service.readOptional("parquet-ccl-marketplace-avatar-created-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(EventDF))
    when(service.readOptional("parquet-ccl-marketplace-avatar-updated-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(spark.read.json(Seq(updated_value).toDS())))
    when(service.readOptional("parquet-ccl-marketplace-avatar-deleted-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(None)
    when(service.readOptional("parquet-marketplace-avatar-created-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(EventDF))
    when(service.readOptional("parquet-marketplace-avatar-updated-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(spark.read.json(Seq(updated_value).toDS())))
    when(service.readOptional("parquet-marketplace-avatar-deleted-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(None)
    when(service.readOptional("delta-marketplace-avatar-sink", sprk)).thenReturn(None)

    val transformer = new GenericSCD1Transformation(sprk, service, "ccl-marketplace-avatar-mutated-transform")
    val sinks = transformer.transform()

    val df = sinks.get.output

    assert(df.columns.toSet === MutatedExpectedFields)
    assert(df.count === 1)
    assert[String](df, s"${Entity}_id", "b95a7743-4e45-4ffc-986a-99de62396038")

    val orgs = df.first()
    orgs.getAs[Array[String]](s"${Entity}_organizations") shouldBe  Array("MSCL", "NC")
    assert[Int](df, s"${Entity}_dw_id", 1)
    assert[String](df, s"${Entity}_id", "b95a7743-4e45-4ffc-986a-99de62396038")
    assert[String](df, s"${Entity}_file_id", "5744b599-8a80-4d9d-a299-ae72e7cfd42c")
    assert[String](df, s"${Entity}_name", "meme_updated.png")
    assert[String](df, s"${Entity}_created_time", "2023-04-27 07:39:32.0")
    assert[String](df, s"${Entity}_updated_time", "2023-04-28 07:39:32.0")
    assert[String](df, s"${Entity}_deleted_time", null)
    assert[Boolean](df, s"${Entity}_is_enabled_for_all_orgs", true)
    assert[Int](df, s"${Entity}_status", 1)
  }

  test("Should transform created and deleted comes in same batch") {

    val created_value = """
                          |[{
                          |	"eventType": "AvatarCreatedEvent",
                          |	"id": "b95a7743-4e45-4ffc-986a-99de62396038",
                          |	"type": "PREMIUM",
                          |	"status": "ENABLED",
                          |	"name": "meme.png",
                          |	"fileId": "5744b599-8a80-4d9d-a299-ae72e7cfd42c",
                          |	"description": "some description",
                          |	"validFrom": "2024-04-23T10:35:36.418102203",
                          |	"validTill": "2024-04-29T20:00:00",
                          |	"organizations": [
                          |		"MSCL",
                          |		"NC"
                          |	],
                          |	"customization": null,
                          |	"createdAt": "2024-04-23T10:35:36.41813526",
                          |	"createdBy": "ebe4d97e-c2c0-481f-b90a-ccd5138234ee",
                          |	"updatedAt": "2024-04-23T10:35:36.418140282",
                          |	"updatedBy": "ebe4d97e-c2c0-481f-b90a-ccd5138234ee",
                          |	"starCost": 1,
                          |	"genders": [
                          |		"MALE"
                          |	],
                          |	"category": "CHARACTERS",
                          |	"tenants": [
                          |		"e2e"
                          |	],
                          |	"eventId": "3d7ac395-cfe6-4ad6-8ac7-f792f7d2cb0c",
                          | "isEnabledForAllOrgs": true,
                          | "occurredOn": "2023-04-27 07:39:32.000",
                          |	"eventDateDw": 20230427
                          |}]
      """.stripMargin
    val deleted_value = """
                          |{
                          |	"eventType": "AvatarDeletedEvent",
                          |	"avatarId": "b95a7743-4e45-4ffc-986a-99de62396038",
                          |	"id": "b95a7743-4e45-4ffc-986a-99de62396038",
                          |	"fileId": "5744b599-8a80-4d9d-a299-ae72e7cfd42c",
                          |	"deletedBy": "ebe4d97e-c2c0-481f-b90a-ccd5138234ee",
                          |	"deletedAt": "2024-04-23T10:42:25.553661907",
                          |	"avatarType": "PREMIUM",
                          |	"avatarCategory": "OTHERS",
                          |	"eventId": "610dbdaa-8680-41f7-821a-1d42e65dc967",
                          | "isEnabledForAllOrgs": true,
                          | "occurredOn": "2023-04-29 07:39:32.000",
                          |	"eventDateDw": 20230429
                          |}
      """.stripMargin

    val sprk = spark
    import sprk.implicits._

    when(service.getStartIdUpdateStatus("dim_avatar")).thenReturn(1)

    val EventDF = spark.read.json(Seq(created_value).toDS())
    when(service.readOptional("parquet-ccl-marketplace-avatar-created-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(EventDF))
    when(service.readOptional("parquet-ccl-marketplace-avatar-updated-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(None)
    when(service.readOptional("parquet-ccl-marketplace-avatar-deleted-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(spark.read.json(Seq(deleted_value).toDS())))
    when(service.readOptional("parquet-marketplace-avatar-created-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(EventDF))
    when(service.readOptional("parquet-marketplace-avatar-updated-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(None)
    when(service.readOptional("parquet-marketplace-avatar-deleted-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(spark.read.json(Seq(deleted_value).toDS())))
    when(service.readOptional("delta-marketplace-avatar-sink",  sprk)).thenReturn(None)

    val transformer = new GenericSCD1Transformation(sprk, service, "ccl-marketplace-avatar-mutated-transform")
    val sinks = transformer.transform()

    val df = sinks.get.output

    assert(df.columns.toSet === MutatedExpectedFields)
    assert(df.count === 1)
    assert[String](df, s"${Entity}_id", "b95a7743-4e45-4ffc-986a-99de62396038")

    val orgs = df.first()
    orgs.getAs[Array[String]](s"${Entity}_organizations") shouldBe  Array("MSCL", "NC")
    assert[Int](df, s"${Entity}_dw_id", 1)
    assert[String](df, s"${Entity}_id", "b95a7743-4e45-4ffc-986a-99de62396038")
    assert[String](df, s"${Entity}_file_id", "5744b599-8a80-4d9d-a299-ae72e7cfd42c")
    assert[String](df, s"${Entity}_name", "meme.png")
    assert[String](df, s"${Entity}_created_time", "2023-04-27 07:39:32.0")
    assert[String](df, s"${Entity}_updated_time", null)
    assert[String](df, s"${Entity}_deleted_time", "2023-04-29 07:39:32.0")
    assert[Boolean](df, s"${Entity}_is_enabled_for_all_orgs", true)
    assert[Int](df, s"${Entity}_status", 4)
  }

  test("Should transform created event") {

    val created_value = """
                          |[{
                          |	"eventType": "AvatarCreatedEvent",
                          |	"id": "b95a7743-4e45-4ffc-986a-99de62396038",
                          |	"type": "PREMIUM",
                          |	"status": "ENABLED",
                          |	"name": "meme.png",
                          |	"fileId": "5744b599-8a80-4d9d-a299-ae72e7cfd42c",
                          |	"description": "some description",
                          |	"validFrom": "2024-04-23T10:35:36.418102203",
                          |	"validTill": "2024-04-29T20:00:00",
                          |	"organizations": [
                          |		"MSCL",
                          |		"NC"
                          |	],
                          |	"customization": null,
                          |	"createdAt": "2024-04-23T10:35:36.41813526",
                          |	"createdBy": "ebe4d97e-c2c0-481f-b90a-ccd5138234ee",
                          |	"updatedAt": "2024-04-23T10:35:36.418140282",
                          |	"updatedBy": "ebe4d97e-c2c0-481f-b90a-ccd5138234ee",
                          |	"starCost": 1,
                          |	"genders": [
                          |		"MALE"
                          |	],
                          |	"category": "CHARACTERS",
                          |	"tenants": [
                          |		"e2e"
                          |	],
                          |	"eventId": "3d7ac395-cfe6-4ad6-8ac7-f792f7d2cb0c",
                          | "isEnabledForAllOrgs": true,
                          | "occurredOn": "2023-04-27 07:39:32.000",
                          |	"eventDateDw": 20230427
                          |}]
      """.stripMargin

    val sprk = spark
    import sprk.implicits._

    when(service.getStartIdUpdateStatus("dim_avatar")).thenReturn(1)

    val EventDF = spark.read.json(Seq(created_value).toDS())
    when(service.readOptional("parquet-ccl-marketplace-avatar-created-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(EventDF))
    when(service.readOptional("parquet-ccl-marketplace-avatar-updated-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(None)
    when(service.readOptional("parquet-ccl-marketplace-avatar-deleted-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(None)
    when(service.readOptional("parquet-marketplace-avatar-created-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(EventDF))
    when(service.readOptional("parquet-marketplace-avatar-updated-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(None)
    when(service.readOptional("parquet-marketplace-avatar-deleted-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(None)
    when(service.readOptional("delta-marketplace-avatar-sink",  sprk)).thenReturn(None)

    val transformer = new GenericSCD1Transformation(sprk, service, "ccl-marketplace-avatar-mutated-transform")
    val sinks = transformer.transform()

    val df = sinks.get.output

    assert(df.columns.toSet === MutatedExpectedFields)
    assert(df.count === 1)
    assert[String](df, s"${Entity}_id", "b95a7743-4e45-4ffc-986a-99de62396038")

    val orgs = df.first()
    orgs.getAs[Array[String]](s"${Entity}_organizations") shouldBe  Array("MSCL", "NC")
    assert[Int](df, s"${Entity}_dw_id", 1)
    assert[String](df, s"${Entity}_id", "b95a7743-4e45-4ffc-986a-99de62396038")
    assert[String](df, s"${Entity}_file_id", "5744b599-8a80-4d9d-a299-ae72e7cfd42c")
    assert[String](df, s"${Entity}_name", "meme.png")
    assert[String](df, s"${Entity}_created_time", "2023-04-27 07:39:32.0")
    assert[String](df, s"${Entity}_updated_time", null)
    assert[String](df, s"${Entity}_deleted_time", null)
    assert[Boolean](df, s"${Entity}_is_enabled_for_all_orgs", true)
    assert[Int](df, s"${Entity}_status", 1)
  }

  test("Should transform data when only updated and deleted for same id in same batch") {

    val created_value = """
                          |{
                          |	"eventType": "AvatarCreatedEvent",
                          |	"type": "PREMIUM",
                          |	"status": "ENABLED",
                          |	"name": "meme.png",
                          |	"id": "avatar-id-10",
                          |	"fileId": "avatar-file-id-10",
                          |	"description": "some description",
                          |	"validFrom": "2024-04-23T10:35:36.418102203",
                          |	"validTill": "2024-04-29T20:00:00",
                          |	"organizations": [
                          |		"MSCL",
                          |		"NC"
                          |	],
                          |	"customization": null,
                          |	"createdAt": "2024-04-23T10:35:36.41813526",
                          |	"createdBy": "ebe4d97e-c2c0-481f-b90a-ccd5138234ee",
                          |	"updatedAt": "2024-04-23T10:35:36.418140282",
                          |	"updatedBy": "ebe4d97e-c2c0-481f-b90a-ccd5138234ee",
                          |	"starCost": 1,
                          |	"genders": [
                          |		"MALE"
                          |	],
                          |	"category": "CHARACTERS",
                          |	"tenants": [
                          |		"e2e"
                          |	],
                          |	"eventId": "3d7ac395-cfe6-4ad6-8ac7-f792f7d2cb0c",
                          | "isEnabledForAllOrgs": true,
                          | "occurredOn": "2023-04-28 07:39:32.000",
                          |	"eventDateDw": 20230427
                          |}
      """.stripMargin
    val updated_value = """
                          |{
                          |	"eventType": "AvatarUpdatedEvent",
                          |	"type": "PREMIUM",
                          |	"status": "ENABLED",
                          |	"name": "meme.png",
                          |	"id": "avatar-id-1",
                          |	"fileId": "avatar-file-id-1",
                          |	"description": "some description",
                          |	"validFrom": "2024-04-23T10:35:36.418102203",
                          |	"validTill": "2024-04-29T20:00:00",
                          |	"organizations": [
                          |		"MSCL",
                          |		"NC"
                          |	],
                          |	"customization": null,
                          |	"createdAt": "2024-04-23T10:35:36.41813526",
                          |	"createdBy": "ebe4d97e-c2c0-481f-b90a-ccd5138234ee",
                          |	"updatedAt": "2024-04-23T10:35:36.418140282",
                          |	"updatedBy": "ebe4d97e-c2c0-481f-b90a-ccd5138234ee",
                          |	"starCost": 1,
                          |	"genders": [
                          |		"MALE"
                          |	],
                          |	"category": "CHARACTERS",
                          |	"tenants": [
                          |		"e2e"
                          |	],
                          |	"eventId": "3d7ac395-cfe6-4ad6-8ac7-f792f7d2cb0c",
                          | "isEnabledForAllOrgs": true,
                          | "occurredOn": "2023-04-28 07:39:32.000",
                          |	"eventDateDw": 20230427
                          |}
      """.stripMargin
    val deleted_value = """
                          |{
                          |	"eventType": "AvatarDeletedEvent",
                          |	"id": "avatar-id-1",
                          |	"fileId": "avatar-file-id-1",
                          |	"deletedBy": "ebe4d97e-c2c0-481f-b90a-ccd5138234ee",
                          |	"deletedAt": "2024-04-23T10:42:25.553661907",
                          |	"avatarType": "PREMIUM",
                          |	"avatarCategory": "OTHERS",
                          |	"eventId": "610dbdaa-8680-41f7-821a-1d42e65dc967",
                          | "isEnabledForAllOrgs": true,
                          | "occurredOn": "2023-04-29 07:39:32.000",
                          |	"eventDateDw": 20230427
                          |}
      """.stripMargin

    val dwh_value = """
                      |[{
                      |	"avatar_dw_id": 1,
                      |	"avatar_id": "avatar-id-1",
                      |	"avatar_type": "PREMIUM",
                      |	"avatar_status": 1,
                      |	"avatar_app_status": "ENABLED",
                      |	"avatar_name": "meme.png",
                      |	"avatar_file_id": "avatar-file-id-1",
                      |	"avatar_description": "some description",
                      |	"avatar_valid_from": "2024-04-23T10:35:36.418102203",
                      |	"avatar_valid_till": "2024-04-29T20:00:00",
                      |	"avatar_organizations": [
                      |		"MSCL",
                      |		"NC"
                      |	],
                      |	"avatar_created_at": "2024-04-23T10:35:36.41813526",
                      |	"avatar_created_by": "ebe4d97e-c2c0-481f-b90a-ccd5138234ee",
                      |	"avatar_updated_at": "2024-04-23T10:35:36.418140282",
                      |	"avatar_updated_by": "ebe4d97e-c2c0-481f-b90a-ccd5138234ee",
                      |	"avatar_star_cost": 1,
                      |	"avatar_genders": [
                      |		"MALE"
                      |	],
                      |	"avatar_category": "CHARACTERS",
                      |	"avatar_tenants": [
                      |		"e2e"
                      |	],
                      | "avatar_created_time": "2023-03-27 07:39:32.000",
                      | "avatar_dw_created_time": "2023-03-27 07:39:32.000",
                      | "avatar_updated_time": null,
                      | "avatar_dw_updated_time": null,
                      | "avatar_deleted_time": null
                      |},
                      |{
                      |	"avatar_dw_id": 2,
                      |	"avatar_id": "avatar-id-2",
                      |	"avatar_status": 1,
                      |	"avatar_app_status": "ENABLED",
                      |	"avatar_name": "meme.png",
                      |	"avatar_file_id": "avatar-file-id-2",
                      |	"avatar_description": "some description",
                      |	"avatar_valid_from": "2024-04-23T10:35:36.418102203",
                      |	"avatar_valid_till": "2024-04-29T20:00:00",
                      |	"avatar_organizations": [
                      |		"MSCL",
                      |		"NC"
                      |	],
                      |	"avatar_created_at": "2024-04-23T10:35:36.41813526",
                      |	"avatar_created_by": "ebe4d97e-c2c0-481f-b90a-ccd5138234ee",
                      |	"avatar_updated_at": "2024-04-23T10:35:36.418140282",
                      |	"avatar_updated_by": "ebe4d97e-c2c0-481f-b90a-ccd5138234ee",
                      |	"avatar_star_cost": 1,
                      |	"avatar_genders": [
                      |		"MALE"
                      |	],
                      |	"avatar_category": "CHARACTERS",
                      |	"avatar_tenants": [
                      |		"e2e"
                      |	],
                      | "avatar_created_time": "2023-03-11 07:39:32.000",
                      | "avatar_dw_created_time": "2023-03-11 07:39:32.000",
                      | "avatar_updated_time": null,
                      | "avatar_dw_updated_time": null,
                      | "avatar_deleted_time": null
                      |}]
      """.stripMargin

    val sprk = spark
    import sprk.implicits._

    when(service.getStartIdUpdateStatus("dim_avatar")).thenReturn(3)

    when(service.readOptional("parquet-ccl-marketplace-avatar-created-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(spark.read.json(Seq(created_value).toDS())))
    when(service.readOptional("parquet-ccl-marketplace-avatar-updated-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(spark.read.json(Seq(updated_value).toDS())))
    when(service.readOptional("parquet-ccl-marketplace-avatar-deleted-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(spark.read.json(Seq(deleted_value).toDS())))
    when(service.readOptional("parquet-marketplace-avatar-created-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(None)
    when(service.readOptional("parquet-marketplace-avatar-updated-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(spark.read.json(Seq(updated_value).toDS())))
    when(service.readOptional("parquet-marketplace-avatar-deleted-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(spark.read.json(Seq(deleted_value).toDS())))
    when(service.readOptional("delta-marketplace-avatar-sink",  sprk)).thenReturn(Some(spark.read.json(Seq(dwh_value).toDS())))

    val transformer = new GenericSCD1Transformation(sprk, service, "ccl-marketplace-avatar-mutated-transform")
    val sinks = transformer.transform()

    val df = sinks.get.output

    assert(df.columns.toSet === MutatedExpectedFields)
    assert(df.count === 2)
    assert[String](df, s"${Entity}_id", "avatar-id-1")

    val orgs = df.first()
    orgs.getAs[Array[String]](s"${Entity}_organizations") shouldBe  Array("MSCL", "NC")
    assert[Int](df, s"${Entity}_dw_id", 3)
    assert[String](df, s"${Entity}_file_id", "avatar-file-id-1")
    assert[String](df, s"${Entity}_name", "meme.png")
    assert[String](df, s"${Entity}_created_time", "2023-03-27 07:39:32.000")
    assert[String](df, s"${Entity}_updated_time", "2023-04-28 07:39:32")
    assert[String](df, s"${Entity}_deleted_time", "2023-04-29 07:39:32")
    assert[Boolean](df, s"${Entity}_is_enabled_for_all_orgs", true)
    assert[Int](df, s"${Entity}_status", 4)
  }

  test("Should transform data when only updated for same id in same batch") {


    val updated_value = """
                          |{
                          |	"eventType": "AvatarUpdatedEvent",
                          |	"type": "PREMIUM",
                          |	"status": "ENABLED",
                          |	"name": "meme.png",
                          |	"id": "avatar-id-1",
                          |	"fileId": "avatar-file-id-1",
                          |	"description": "some description",
                          |	"validFrom": "2024-04-23T10:35:36.418102203",
                          |	"validTill": "2024-04-29T20:00:00",
                          |	"organizations": [
                          |		"MSCL",
                          |		"NC"
                          |	],
                          |	"customization": null,
                          |	"createdAt": "2024-04-23T10:35:36.41813526",
                          |	"createdBy": "ebe4d97e-c2c0-481f-b90a-ccd5138234ee",
                          |	"updatedAt": "2024-04-23T10:35:36.418140282",
                          |	"updatedBy": "ebe4d97e-c2c0-481f-b90a-ccd5138234ee",
                          |	"starCost": 1,
                          |	"genders": [
                          |		"MALE"
                          |	],
                          |	"category": "CHARACTERS",
                          |	"tenants": [
                          |		"e2e"
                          |	],
                          |	"eventId": "3d7ac395-cfe6-4ad6-8ac7-f792f7d2cb0c",
                          | "isEnabledForAllOrgs": true,
                          | "occurredOn": "2023-04-28 07:39:32.000",
                          |	"eventDateDw": 20230427
                          |}
      """.stripMargin
    val dwh_value = """
                      |[{
                      |	"avatar_dw_id": 1,
                      |	"avatar_id": "avatar-id-1",
                      |	"avatar_type": "PREMIUM",
                      |	"avatar_status": 1,
                      |	"avatar_app_status": "ENABLED",
                      |	"avatar_name": "meme.png",
                      |	"avatar_file_id": "avatar-file-id-1",
                      |	"avatar_description": "some description",
                      |	"avatar_valid_from": "2024-04-23T10:35:36.418102203",
                      |	"avatar_valid_till": "2024-04-29T20:00:00",
                      |	"avatar_organizations": [
                      |		"MSCL",
                      |		"NC"
                      |	],
                      |	"avatar_created_at": "2024-04-23T10:35:36.41813526",
                      |	"avatar_created_by": "ebe4d97e-c2c0-481f-b90a-ccd5138234ee",
                      |	"avatar_updated_at": "2024-04-23T10:35:36.418140282",
                      |	"avatar_updated_by": "ebe4d97e-c2c0-481f-b90a-ccd5138234ee",
                      |	"avatar_star_cost": 1,
                      |	"avatar_genders": [
                      |		"MALE"
                      |	],
                      |	"avatar_category": "CHARACTERS",
                      |	"avatar_tenants": [
                      |		"e2e"
                      |	],
                      | "avatar_created_time": "2023-03-27 07:39:32.000",
                      | "avatar_dw_created_time": "2023-03-27 07:39:32.000",
                      | "avatar_updated_time": null,
                      | "avatar_dw_updated_time": null,
                      | "avatar_deleted_time": null
                      |},
                      |{
                      |	"avatar_dw_id": 2,
                      |	"avatar_id": "avatar-id-2",
                      |	"avatar_status": 1,
                      |	"avatar_app_status": "ENABLED",
                      |	"avatar_name": "meme.png",
                      |	"avatar_file_id": "avatar-file-id-2",
                      |	"avatar_description": "some description",
                      |	"avatar_valid_from": "2024-04-23T10:35:36.418102203",
                      |	"avatar_valid_till": "2024-04-29T20:00:00",
                      |	"avatar_organizations": [
                      |		"MSCL",
                      |		"NC"
                      |	],
                      |	"avatar_created_at": "2024-04-23T10:35:36.41813526",
                      |	"avatar_created_by": "ebe4d97e-c2c0-481f-b90a-ccd5138234ee",
                      |	"avatar_updated_at": "2024-04-23T10:35:36.418140282",
                      |	"avatar_updated_by": "ebe4d97e-c2c0-481f-b90a-ccd5138234ee",
                      |	"avatar_star_cost": 1,
                      |	"avatar_genders": [
                      |		"MALE"
                      |	],
                      |	"avatar_category": "CHARACTERS",
                      |	"avatar_tenants": [
                      |		"e2e"
                      |	],
                      | "avatar_created_time": "2023-03-11 07:39:32.000",
                      | "avatar_dw_created_time": "2023-03-11 07:39:32.000",
                      | "avatar_updated_time": null,
                      | "avatar_dw_updated_time": null,
                      | "avatar_deleted_time": null
                      |}]
      """.stripMargin

    val sprk = spark
    import sprk.implicits._

    when(service.getStartIdUpdateStatus("dim_avatar")).thenReturn(1)

    when(service.readOptional("parquet-ccl-marketplace-avatar-created-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(None)
    when(service.readOptional("parquet-ccl-marketplace-avatar-updated-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(spark.read.json(Seq(updated_value).toDS())))
    when(service.readOptional("parquet-ccl-marketplace-avatar-deleted-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(None)
    when(service.readOptional("parquet-marketplace-avatar-created-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(None)
    when(service.readOptional("parquet-marketplace-avatar-updated-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(spark.read.json(Seq(updated_value).toDS())))
    when(service.readOptional("parquet-marketplace-avatar-deleted-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(None)
    when(service.readOptional("delta-marketplace-avatar-sink", sprk)).thenReturn(Some(spark.read.json(Seq(dwh_value).toDS())))

    val transformer = new GenericSCD1Transformation(sprk, service, "ccl-marketplace-avatar-mutated-transform")
    val sinks = transformer.transform()

    val df = sinks.get.output

    assert(df.columns.toSet === MutatedExpectedFields)
    assert(df.count === 1)

    val orgs = df.first()
    orgs.getAs[Array[String]](s"${Entity}_organizations") shouldBe  Array("MSCL", "NC")
    assert[String](df, s"${Entity}_id", "avatar-id-1")
    assert[String](df, s"${Entity}_file_id", "avatar-file-id-1")
    assert[String](df, s"${Entity}_name", "meme.png")
    assert[String](df, s"${Entity}_created_time", "2023-03-27 07:39:32.000")
    assert[String](df, s"${Entity}_updated_time", "2023-04-28 07:39:32")
    assert[String](df, s"${Entity}_deleted_time", null)
    assert[Boolean](df, s"${Entity}_is_enabled_for_all_orgs", true)
    assert[Int](df, s"${Entity}_status", 1)
  }

  test("Should transform data when only deleted for same id in same batch") {
    val deleted_value = """
                          |{
                          |	"eventType": "AvatarDeletedEvent",
                          |	"id": "avatar-id-1",
                          |	"fileId": "avatar-file-id-1",
                          |	"deletedBy": "ebe4d97e-c2c0-481f-b90a-ccd5138234ee",
                          |	"deletedAt": "2024-04-23T10:42:25.553661907",
                          |	"avatarType": "PREMIUM",
                          |	"avatarCategory": "OTHERS",
                          |	"eventId": "610dbdaa-8680-41f7-821a-1d42e65dc967",
                          | "isEnabledForAllOrgs": true,
                          | "occurredOn": "2023-04-29 07:39:32.000",
                          |	"eventDateDw": 20230427
                          |}
      """.stripMargin
    val dwh_value = """
                      |[{
                      |	"avatar_dw_id": 1,
                      |	"avatar_id": "avatar-id-1",
                      |	"avatar_type": "PREMIUM",
                      |	"avatar_status": 1,
                      |	"avatar_app_status": "ENABLED",
                      |	"avatar_name": "meme.png",
                      |	"avatar_file_id": "avatar-file-id-1",
                      |	"avatar_description": "some description",
                      |	"avatar_valid_from": "2024-04-23T10:35:36.418102203",
                      |	"avatar_valid_till": "2024-04-29T20:00:00",
                      |	"avatar_organizations": [
                      |		"MSCL",
                      |		"NC"
                      |	],
                      |	"avatar_created_at": "2024-04-23T10:35:36.41813526",
                      |	"avatar_created_by": "ebe4d97e-c2c0-481f-b90a-ccd5138234ee",
                      |	"avatar_updated_at": "2024-04-23T10:35:36.418140282",
                      |	"avatar_updated_by": "ebe4d97e-c2c0-481f-b90a-ccd5138234ee",
                      |	"avatar_star_cost": 1,
                      |	"avatar_genders": [
                      |		"MALE"
                      |	],
                      |	"avatar_category": "CHARACTERS",
                      |	"avatar_tenants": [
                      |		"e2e"
                      |	],
                      | "avatar_created_time": "2023-03-27 07:39:32.000",
                      | "avatar_dw_created_time": "2023-03-27 07:39:32.000",
                      | "avatar_updated_time": null,
                      | "avatar_dw_updated_time": null,
                      | "avatar_deleted_time": null,
                      | "avatar_is_enabled_for_all_orgs": null
                      |},
                      |{
                      |	"avatar_dw_id": 2,
                      |	"avatar_id": "avatar-id-2",
                      |	"avatar_status": 1,
                      |	"avatar_app_status": "ENABLED",
                      |	"avatar_name": "meme.png",
                      |	"avatar_file_id": "avatar-file-id-2",
                      |	"avatar_description": "some description",
                      |	"avatar_valid_from": "2024-04-23T10:35:36.418102203",
                      |	"avatar_valid_till": "2024-04-29T20:00:00",
                      |	"avatar_organizations": [
                      |		"MSCL",
                      |		"NC"
                      |	],
                      |	"avatar_created_at": "2024-04-23T10:35:36.41813526",
                      |	"avatar_created_by": "ebe4d97e-c2c0-481f-b90a-ccd5138234ee",
                      |	"avatar_updated_at": "2024-04-23T10:35:36.418140282",
                      |	"avatar_updated_by": "ebe4d97e-c2c0-481f-b90a-ccd5138234ee",
                      |	"avatar_star_cost": 1,
                      |	"avatar_genders": [
                      |		"MALE"
                      |	],
                      |	"avatar_category": "CHARACTERS",
                      |	"avatar_tenants": [
                      |		"e2e"
                      |	],
                      | "avatar_created_time": "2023-03-11 07:39:32.000",
                      | "avatar_dw_created_time": "2023-03-11 07:39:32.000",
                      | "avatar_updated_time": null,
                      | "avatar_dw_updated_time": null,
                      | "avatar_deleted_time": null,
                      | "avatar_is_enabled_for_all_orgs": false
                      |}]
      """.stripMargin

    val sprk = spark
    import sprk.implicits._

    when(service.getStartIdUpdateStatus("dim_avatar")).thenReturn(3)

    when(service.readOptional("parquet-ccl-marketplace-avatar-created-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(None)
    when(service.readOptional("parquet-ccl-marketplace-avatar-updated-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(None)
    when(service.readOptional("parquet-ccl-marketplace-avatar-deleted-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(spark.read.json(Seq(deleted_value).toDS())))
    when(service.readOptional("parquet-marketplace-avatar-created-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(None)
    when(service.readOptional("parquet-marketplace-avatar-updated-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(None)
    when(service.readOptional("parquet-marketplace-avatar-deleted-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(spark.read.json(Seq(deleted_value).toDS())))
    when(service.readOptional("delta-marketplace-avatar-sink",  sprk)).thenReturn(Some(spark.read.json(Seq(dwh_value).toDS())))

    val transformer = new GenericSCD1Transformation(sprk, service, "ccl-marketplace-avatar-mutated-transform")
    val sinks = transformer.transform()

    val df = sinks.get.output

    assert(df.columns.toSet === MutatedExpectedFields)
    assert(df.count === 1)
    assert[String](df, s"${Entity}_id", "avatar-id-1")

    val orgs = df.first()
    orgs.getAs[Array[String]](s"${Entity}_organizations") shouldBe  Array("MSCL", "NC")
    assert[Int](df, s"${Entity}_dw_id", 3)
    assert[String](df, s"${Entity}_file_id", "avatar-file-id-1")
    assert[String](df, s"${Entity}_name", "meme.png")
    assert[String](df, s"${Entity}_created_time", "2023-03-27 07:39:32.000")
    assert[String](df, s"${Entity}_updated_time", null)
    assert[String](df, s"${Entity}_deleted_time", "2023-04-29 07:39:32")
    assert[Boolean](df, s"${Entity}_is_enabled_for_all_orgs", true)
    assert[Int](df, s"${Entity}_status", 4)
  }

  test("Should transform dim avatar customization from avatar created event") {

    val created_value = """
                          |[{
                          |	"eventType": "AvatarCreatedEvent",
                          |	"id": "b95a7743-4e45-4ffc-986a-99de62396038",
                          |	"type": "PREMIUM",
                          |	"status": "ENABLED",
                          |	"name": "meme.png",
                          |	"fileId": "5744b599-8a80-4d9d-a299-ae72e7cfd42c",
                          |	"description": "some description",
                          |	"validFrom": "2024-04-23T10:35:36.418102203",
                          |	"validTill": "2024-04-29T20:00:00",
                          |	"organizations": [
                          |		"MSCL",
                          |		"NC"
                          |	],
                          |	"customization": ["custom-layer-id-1", "custom-layer-id-2"],
                          |	"createdAt": "2024-04-23T10:35:36.41813526",
                          |	"createdBy": "ebe4d97e-c2c0-481f-b90a-ccd5138234ee",
                          |	"updatedAt": "2024-04-23T10:35:36.418140282",
                          |	"updatedBy": "ebe4d97e-c2c0-481f-b90a-ccd5138234ee",
                          |	"starCost": 1,
                          |	"genders": [
                          |		"MALE"
                          |	],
                          |	"category": "CHARACTERS",
                          |	"tenants": [
                          |		"e2e"
                          |	],
                          |	"eventId": "3d7ac395-cfe6-4ad6-8ac7-f792f7d2cb0c",
                          | "isEnabledForAllOrgs": true,
                          | "occurredOn": "2023-04-27 07:39:32.000",
                          |	"eventDateDw": 20230427
                          |}]
      """.stripMargin

    val sprk = spark
    import sprk.implicits._

    when(service.getStartIdUpdateStatus("dim_avatar_layer_customization")).thenReturn(1)

    val EventDF = spark.read.json(Seq(created_value).toDS())
    when(service.readOptional("parquet-marketplace-avatar-created-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(EventDF))
    when(service.readOptional("delta-marketplace-avatar-customization-sink", sprk)).thenReturn(None)

    val transformer = new GenericSCD1Transformation(sprk, service, "ccl-marketplace-avatar-customization-transform")
    val sinks = transformer.transform()

    val df = sinks.get.output
    val MutatedExpectedFields = Set(
      "ala_created_time",
      "ala_updated_time",
      "ala_deleted_time",
      "ala_dw_created_time",
      "ala_dw_updated_time",
      "ala_dw_id",
      "ala_avatar_id",
      "ala_layer_id",
      "ala_status"
    )
    assert(df.columns.toSet === MutatedExpectedFields)
    assert(df.count === 2)
    assert[String](df, "ala_avatar_id", "b95a7743-4e45-4ffc-986a-99de62396038")
    assert[Int](df, "ala_dw_id", 1)
    assert[String](df, "ala_layer_id", "custom-layer-id-1")
    assert[String](df, "ala_created_time", "2023-04-27 07:39:32.0")
    assert[Int](df, "ala_status", 1)

  }

}
