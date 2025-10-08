package com.alefeducation.dimensions.course_activity_container.transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.dimensions.course_activity_container.transform.ContainerDwIdMappingTransform.ContainerAddedParquetSource
import com.alefeducation.dimensions.course_activity_container.transform.ContainerGradeTransform.{ContainerGradeEntity, ContainerGradeTransformService, ContainerGroupKey, UniqueColNames}
import com.alefeducation.dimensions.course_activity_container.transform.ContainerMutatedTransform.{ContainerPublishedParquetSource, ContainerUpdatedParquetSource}
import com.alefeducation.dimensions.course_activity_container.transform.ContainerTransformUtils.addMetadata
import com.alefeducation.util.DataFrameEqualityUtils.{assertSmallDatasetEquality, createDfFromJson}
import com.alefeducation.util.Resources.{getSink, getSource}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

class ContainerGradeTransformTest extends SparkSuite {

  val sprk: SparkSession = spark
  val service: SparkBatchService = mock[SparkBatchService]

  val expectedColumns = Set(
    "cacga_updated_time",
    "cacga_course_id",
    "cacga_status",
    "cacga_grade",
    "cacga_container_id",
    "cacga_dw_updated_time",
    "cacga_created_time",
    "cacga_dw_created_time",
    "cacga_dw_id"
  )

  test("should construct course activity container grade association dataframe") {
    val publishedValue =
      """
        |[
        |  {
        | "eventType": "ContainerPublishedWithCourseEvent",
        |	"id": "level1_id",
        |	"courseId": "pathwayId",
        |	"index": 0,
        |	"title": "Level-1",
        |	"settings": {
        |		"pacing": "SELF_PACED",
        |		"hideWhenPublishing": false,
        |		"isOptional": false
        |	},
        |	"longName": "Phantoms long name",
        |	"description": "Level description",
        |	"sequences": [
        |		{
        |			"domain": "Geometry",
        |			"sequence": 1
        |		},
        |		{
        |			"domain": "Algebra and Algebraic Thinking",
        |			"sequence": 1
        |		}
        |	],
        | "courseVersion": "228",
        |	"occurredOn": "2021-06-23 05:33:24.921",
        | "isAccelerated": false
        |}]
        """.stripMargin

    val metadataPublishedValue =
      """{
        |   "tags": [
        |     {
        |       "key": "Grade",
        |       "values": ["1"],
        |       "type": "LIST"
        |     },
        |     {
        |       "key": "Domain",
        |       "values": [
        |         "{\"name\": \"Geometry\",\"icon\": \"Geometry\",\"color\": \"lightBlue\"}",
        |         "{\"name\": \"Algebra and Algebraic Thinking\",\"icon\": \"AlgebraAndAlgebraicThinking\",\"color\": \"lightGreen\"}"
        |       ],
        |       "attributes": [
        |         {"value": "val01", "color": "green", "translation": "tr01"}
        |       ],
        |       "type": "DOMAIN"
        |     }
        |   ]
        |}""".stripMargin

    val addedValue =
      """
        |[
        | {
        | "eventType": "ContainerAddedInCourseEvent",
        |	"id": "level2_id",
        |	"courseId": "pathwayId",
        |	"index": 0,
        |	"title": "Level-2",
        |	"settings": {
        |		"pacing": "SELF_PACED",
        |		"hideWhenPublishing": false,
        |		"isOptional": false
        |	},
        |	"items": [
        |		{
        |			"id": "03583155-b890-4ab6-a3c4-000000028179",
        |			"settings": {
        |				"pacing": "UN_LOCKED",
        |				"hideWhenPublishing": false,
        |				"isOptional": false
        |			},
        |			"type": "ACTIVITY"
        |		},
        |		{
        |			"id": "5c0bbf43-bc98-49d7-93ae-000000028400",
        |			"settings": {
        |				"pacing": "UN_LOCKED",
        |				"hideWhenPublishing": false,
        |				"isOptional": false
        |			},
        |			"type": "ACTIVITY"
        |	  }
        |	],
        |	"longName": "Phantoms long name",
        |	"description": "Level description",
        | "sequences": [
        |		{
        |			"domain": "Geometry",
        |			"sequence": 1
        |		},
        |		{
        |			"domain": "Algebra and Algebraic Thinking",
        |			"sequence": 1
        |		}
        |	],
        | "courseVersion": "228",
        |	"occurredOn": "2021-06-23 06:33:24.921",
        | "isAccelerated":false
        | }
        |]
        |""".stripMargin

    val metadataAddValue =
      """
        |{
        |   "tags": [
        |     {
        |       "key": "Grade",
        |       "values": ["1"],
        |       "type": "LIST"
        |     },
        |     {
        |       "key": "Domain",
        |       "values": [
        |         "{\"name\": \"Geometry\",\"icon\": \"Geometry\",\"color\": \"lightBlue\"}",
        |         "{\"name\": \"Algebra and Algebraic Thinking\",\"icon\": \"AlgebraAndAlgebraicThinking\",\"color\": \"lightGreen\"}"
        |       ],
        |       "attributes": [
        |         {"value": "val01", "color": "green", "translation": "tr01"}
        |       ],
        |       "type": "DOMAIN"
        |     }
        |   ]
        |}
        |""".stripMargin

    val updatedValue =
      """
        |[
        | {
        | "eventType": "ContainerUpdatedInCourseEvent",
        |	"id": "level2_id",
        |	"courseId": "pathwayId",
        |	"index": 0,
        |	"title": "Level-22",
        |	"settings": {
        |		"pacing": "SELF_PACED",
        |		"hideWhenPublishing": false,
        |		"isOptional": false
        |	},
        |	"items": [
        |		{
        |			"id": "03583155-b890-4ab6-a3c4-000000028179",
        |			"settings": {
        |				"pacing": "UN_LOCKED",
        |				"hideWhenPublishing": false,
        |				"isOptional": false
        |			},
        |			"type": "ACTIVITY"
        |		},
        |		{
        |			"id": "5c0bbf43-bc98-49d7-93ae-000000028400",
        |			"settings": {
        |				"pacing": "UN_LOCKED",
        |				"hideWhenPublishing": false,
        |				"isOptional": false
        |			},
        |			"type": "ACTIVITY"
        |	  }
        |	],
        |	"longName": "Phantoms long name",
        |	"description": "Level description",
        |	"sequences": [
        |		{
        |			"domain": "Geometry",
        |			"sequence": 1
        |		}
        |	],
        | "courseVersion": "228",
        |	"occurredOn": "2021-06-23 07:33:24.921"
        | }
        |]
        |""".stripMargin

    val metadataUpdatedValue =
      """
        |{
        |   "tags": [
        |     {
        |       "key": "Grade",
        |       "values": ["1", "2"],
        |       "type": "LIST"
        |     },
        |     {
        |       "key": "Domain",
        |       "values": [
        |         "{\"name\": \"Geometry\",\"icon\": \"Geometry\",\"color\": \"lightBlue\"}"
        |       ],
        |       "attributes": [
        |         {"value": "val01", "color": "green", "translation": "tr01"}
        |       ],
        |       "type": "DOMAIN"
        |     }
        |   ]
        | }
        |""".stripMargin

    val expJson =
      """
        |[
        |{"cacga_course_id":"pathwayId","cacga_status":1,"cacga_grade":"1","cacga_container_id":"level1_id","cacga_created_time":"2021-06-23T05:33:24.921Z","cacga_dw_created_time":"2024-03-19T06:54:01.723Z","cacga_dw_id":1001,"cacga_dw_updated_time":null},
        |{"cacga_updated_time":"2021-06-23T07:33:24.921Z","cacga_course_id":"pathwayId","cacga_status":4,"cacga_grade":"1","cacga_container_id":"level2_id","cacga_created_time":"2021-06-23T06:33:24.921Z","cacga_dw_created_time":"2024-03-19T06:54:01.723Z","cacga_dw_id":1002,"cacga_dw_updated_time":null},
        |{"cacga_updated_time":null,"cacga_course_id":"pathwayId","cacga_status":1,"cacga_grade":"1","cacga_container_id":"level2_id","cacga_created_time":"2021-06-23T07:33:24.921Z","cacga_dw_created_time":"2024-03-19T06:54:01.723Z","cacga_dw_id":1003,"cacga_dw_updated_time":null},
        |{"cacga_updated_time":null,"cacga_course_id":"pathwayId","cacga_status":1,"cacga_grade":"2","cacga_container_id":"level2_id","cacga_created_time":"2021-06-23T07:33:24.921Z","cacga_dw_created_time":"2024-03-19T06:54:01.723Z","cacga_dw_id":1004,"cacga_dw_updated_time":null}
        |]
        |""".stripMargin

    val sprk = spark
    import sprk.implicits._

    val publishedInput = spark.read.json(Seq(publishedValue).toDS()).transform(addMetadata(metadataPublishedValue))
    when(service.readUniqueOptional(ContainerPublishedParquetSource, sprk, uniqueColNames = UniqueColNames)).thenReturn(Some(publishedInput))

    val addedInput = spark.read.json(Seq(addedValue).toDS()).transform(addMetadata(metadataAddValue))
    when(service.readUniqueOptional(ContainerAddedParquetSource, sprk, uniqueColNames = UniqueColNames)).thenReturn(Some(addedInput))
    val updatedInput = spark.read.json(Seq(updatedValue).toDS()).transform(addMetadata(metadataUpdatedValue))
    when(service.readUniqueOptional(ContainerUpdatedParquetSource, sprk, uniqueColNames = UniqueColNames)).thenReturn(Some(updatedInput))

    when(service.getStartIdUpdateStatus(ContainerGroupKey)).thenReturn(1001)

    val transformer = new ContainerGradeTransform(sprk, service)

    val sourceNames = getSource(ContainerGradeTransformService)
    val sinkName = getSink(ContainerGradeTransformService).head

    val sink = transformer.transform(sourceNames, sinkName)

    val df = sink.get.output

    assert(df.columns.toSet === expectedColumns)
    assert(df.count() === 4)

    val expDf = createDfFromJsonWithTimeCols(spark, ContainerGradeEntity, expJson)
    assertSmallDatasetEquality(ContainerGradeEntity, df, expDf)
  }

  test("addGrade should add null grade field when metadata is empty") {
    val publishedValue =
      """
        |[
        | {
        | "eventType": "ContainerPublishedWithCourseEvent",
        |	"id": "level1_id",
        |	"courseId": "pathwayId",
        |	"longName": "Phantoms long name",
        |	"description": "Level description",
        |	"metadata": {
        |   "tags": []
        | },
        | "courseVersion": "228",
        |	"occurredOn": "2021-06-23 05:33:24.921"
        |}]
        """.stripMargin

    val sprk = spark
    import sprk.implicits._
    val transformer = new ContainerGradeTransform(sprk, service)

    val publishedInput = spark.read.json(Seq(publishedValue).toDS())
    val levelWithGrade: DataFrame = transformer.addGrade(Some(publishedInput)).getOrElse(throw new RuntimeException)

    val expectedColumns: Array[String] = publishedInput.columns :+ "cacga_grade"
    assert(expectedColumns sameElements levelWithGrade.columns)

    assert[String](levelWithGrade, "cacga_grade", null)
  }

  test("addGrade should add null grade field when 'Grade' key is not found") {
    val publishedValue =
      """
        |[
        | {
        | "eventType": "ContainerPublishedWithCourseEvent",
        |	"id": "level1_id",
        |	"courseId": "pathwayId",
        |	"longName": "Phantoms long name",
        |	"description": "Level description",
        |	"metadata": {
        |   "tags": [
        |     {
        |       "key": "Domain",
        |       "values": [
        |         "{\"name\": \"Geometry\",\"icon\": \"Geometry\",\"color\": \"lightBlue\"}",
        |         "{\"name\": \"Algebra and Algebraic Thinking\",\"icon\": \"AlgebraAndAlgebraicThinking\",\"color\": \"lightGreen\"}"
        |       ],
        |       "attributes": [
        |         {"value": "val01", "color": "green", "translation": "tr01"}
        |       ],
        |       "type": "DOMAIN"
        |     }
        |   ]
        | },
        | "courseVersion": "228",
        |	"occurredOn": "2021-06-23 05:33:24.921"
        |}]
        """.stripMargin

    val sprk = spark
    import sprk.implicits._
    val transformer = new ContainerGradeTransform(sprk, service)

    val publishedInput = spark.read.json(Seq(publishedValue).toDS())
    val levelWithGrade: DataFrame = transformer.addGrade(Some(publishedInput)).getOrElse(throw new RuntimeException)

    val expectedColumns: Array[String] = publishedInput.columns :+ "cacga_grade"
    assert(expectedColumns sameElements levelWithGrade.columns)

    assert[String](levelWithGrade, "cacga_grade", null)
  }

  def createDfFromJsonWithTimeCols(spark: SparkSession, prefix: String, json: String): DataFrame = {
    val df = createDfFromJson(spark, json)
      .withColumn(s"${prefix}_created_time", col(s"${prefix}_created_time").cast(TimestampType))

    if (df.columns.contains(s"${prefix}_updated_time")) {
      df.withColumn(s"${prefix}_updated_time", col(s"${prefix}_updated_time").cast(TimestampType))
    } else df
  }
}
