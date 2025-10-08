package com.alefeducation.dimensions.course_activity_association.transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatestplus.mockito.MockitoSugar.mock

class ActivityAssociationTransformationsSpec extends SparkSuite {
  val sprk: SparkSession = spark
  val service: SparkBatchService = mock[SparkBatchService]

  test("addGrade should add grade field and explode when metadata is not empty") {
    val publishedValue =
      """
        |[
        |  {
        |    "id": "id1",
        |    "metadata": {
        |      "tags": [
        |        {
        |          "key": "Grade",
        |          "values": [
        |            "1",
        |            "2"
        |          ],
        |          "type": "LIST"
        |        },
        |        {
        |          "key": "Domain",
        |          "values": [
        |            "{\"name\": \"Geometry\",\"icon\": \"Geometry\",\"color\": \"lightBlue\"}",
        |            "{\"name\": \"Algebra and Algebraic Thinking\",\"icon\": \"AlgebraAndAlgebraicThinking\",\"color\": \"lightGreen\"}"
        |          ],
        |          "attributes": [
        |            {
        |              "value": "val01",
        |              "color": "green",
        |              "translation": "tr01"
        |            }
        |          ],
        |          "type": "DOMAIN"
        |        }
        |      ]
        |    },
        |    "occurredOn": "2021-06-23 05:33:24.921"
        |  }
        |]
        |""".stripMargin

    val sprk = spark
    import sprk.implicits._

    val publishedInput = spark.read.json(Seq(publishedValue).toDS())
    val levelWithGrade: DataFrame = ActivityAssociationTransformations.addGrade("caa_grade", sprk)(publishedInput)

    assert(levelWithGrade.count() == 2)
    assert[String](levelWithGrade, "caa_grade", "1")
  }

  test("addGrade should add null grade field when metadata is empty") {
    val publishedValue =
      """
        |[
        | {
        |	"id": "id1",
        |	"metadata": {
        |   "tags": []
        | },
        |	"occurredOn": "2021-06-23 05:33:24.921"
        |}]
        """.stripMargin

    val sprk = spark
    import sprk.implicits._

    val publishedInput = spark.read.json(Seq(publishedValue).toDS())
    val levelWithGrade: DataFrame = ActivityAssociationTransformations.addGrade("caa_grade", sprk)(publishedInput)

    assert[String](levelWithGrade, "caa_grade", null)
  }

  test("addGrade should add null grade field when 'Grade' key is not found") {
    val publishedValue =
      """
        |[
        | {
        |	"id": "id1",
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
        |	"occurredOn": "2021-06-23 05:33:24.921"
        |}]
        """.stripMargin

    val sprk = spark
    import sprk.implicits._

    val publishedInput = spark.read.json(Seq(publishedValue).toDS())
    val levelWithGrade: DataFrame = ActivityAssociationTransformations.addGrade("caa_grade", sprk)(publishedInput)

    assert[String](levelWithGrade, "caa_grade", null)
  }
}
