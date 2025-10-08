package com.alefeducation.facts.pathway_placement

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.facts.pathway_placement.PathwayPlacementTransform.PathwayPlacementEntity
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.SparkSession
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

class PathwayPlacementTransformTest extends SparkSuite {
  val sprk: SparkSession = spark
  val service = mock[SparkBatchService]

  val expectedColumns = Set(
    "fpp_pathway_id",
    "fpp_new_pathway_domain",
    "fpp_dw_created_time",
    "eventdate",
    "fpp_new_pathway_grade",
    "fpp_date_dw_id",
    "fpp_class_id",
    "fpp_student_id",
    "fpp_tenant_id",
    "fpp_placement_type",
    "fpp_overall_grade",
    "fpp_is_initial",
    "fpp_created_by",
    "fpp_created_time",
    "fpp_has_accelerated_domains"
  )

  test("transform PlacementCompletionEvent successfully") {

    val placementCompletionEvent =
      """
        |{
        |  "eventType": "PlacementCompletionEvent",
        |  "tenantId": "tenantId1",
        |  "learnerId": "studentId1",
        |  "pathwayId": "pathwayId1",
        |  "academicYear": "2023-2024",
        |  "classId": "classId1",
        |  "recommendationType": "AT_COURSE_BEGINNING",
        |  "gradeLevel": 6,
        |  "placedBy": "teacherId1",
        |  "uuid": "uuid",
        |  "isInitial": true,
        |  "domainGrades": [
        |    {
        |      "domainName": "Measurement, Data and Statistics",
        |      "grade": 6
        |    },
        |    {
        |      "domainName": "Numbers and Operations",
        |      "grade": 6
        |    }
        |  ],
        |  "hasAcceleratedDomains": true,
        |  "recommendedLevels": [
        |      {
        |        "id": "7ae72f7a-5a95-464a-8114-48de4bb66645",
        |        "name": "Level-2",
        |        "status": "ACTIVE"
        |      }
        |  ],
        |  "occurredOn": "2023-08-13T07:57:35.148"
        |}
        |""".stripMargin

    val sprk = spark
    import sprk.implicits._

    val studentManualPlacementDF = spark.read.json(Seq(placementCompletionEvent).toDS())
    when(service.readOptional("parquet-learning-placement-completed-source", sprk)).thenReturn(Some(studentManualPlacementDF))

    val transformer = new PathwayPlacementTransform(sprk, service)

    val df = transformer.transform().get.output

    assert(df.columns.toSet === expectedColumns)
    assert(df.count() == 2)

    assert[String](df, s"${PathwayPlacementEntity}_tenant_id", "tenantId1")
    assert[String](df, s"${PathwayPlacementEntity}_student_id", "studentId1")
    assert[String](df, s"${PathwayPlacementEntity}_pathway_id", "pathwayId1")
    assert[String](df, s"${PathwayPlacementEntity}_class_id", "classId1")
    assert[Int](df, s"${PathwayPlacementEntity}_placement_type", 6)
    assert[Int](df, s"${PathwayPlacementEntity}_overall_grade", 6)
    assert[Boolean](df, s"${PathwayPlacementEntity}_is_initial", true)
    assert[String](df, s"${PathwayPlacementEntity}_created_by", "teacherId1")
    assert[String](df, s"${PathwayPlacementEntity}_new_pathway_domain", "Measurement, Data and Statistics")
    assert[Int](df, s"${PathwayPlacementEntity}_new_pathway_grade", 6)
    assert[String](df, s"${PathwayPlacementEntity}_created_time", "2023-08-13 07:57:35.148")
    assert[String](df, "eventdate", "2023-08-13")
  }

  test("transform PlacementCompletionEvent when isInitial field is not present in the event successfully") {

    val placementCompletionEvent =
      """
        |{
        |  "eventType": "PlacementCompletionEvent",
        |  "tenantId": "tenantId1",
        |  "learnerId": "studentId1",
        |  "pathwayId": "pathwayId1",
        |  "academicYear": "2023-2024",
        |  "classId": "classId1",
        |  "recommendationType": "AT_COURSE_BEGINNING",
        |  "gradeLevel": 6,
        |  "placedBy": "teacherId1",
        |  "uuid": "uuid",
        |  "domainGrades": [
        |    {
        |      "domainName": "Measurement, Data and Statistics",
        |      "grade": 6
        |    },
        |    {
        |      "domainName": "Numbers and Operations",
        |      "grade": 6
        |    }
        |  ],
        |  "recommendedLevels": [
        |      {
        |        "id": "7ae72f7a-5a95-464a-8114-48de4bb66645",
        |        "name": "Level-2",
        |        "status": "ACTIVE"
        |      }
        |  ],
        |  "hasAcceleratedDomains": true,
        |  "occurredOn": "2023-08-13T07:57:35.148"
        |}
        |""".stripMargin

    val sprk = spark
    import sprk.implicits._

    val studentManualPlacementDF = spark.read.json(Seq(placementCompletionEvent).toDS())
    when(service.readOptional("parquet-learning-placement-completed-source", sprk)).thenReturn(Some(studentManualPlacementDF))

    val transformer = new PathwayPlacementTransform(sprk, service)

    val df = transformer.transform().get.output

    assert(df.columns.toSet === expectedColumns)
    assert(df.count() == 2)

    assert[String](df, s"${PathwayPlacementEntity}_tenant_id", "tenantId1")
    assert[String](df, s"${PathwayPlacementEntity}_student_id", "studentId1")
    assert[String](df, s"${PathwayPlacementEntity}_pathway_id", "pathwayId1")
    assert[String](df, s"${PathwayPlacementEntity}_class_id", "classId1")
    assert[Int](df, s"${PathwayPlacementEntity}_placement_type", 6)
    assert[Int](df, s"${PathwayPlacementEntity}_overall_grade", 6)
    assert[String](df, s"${PathwayPlacementEntity}_created_by", "teacherId1")
    assert[String](df, s"${PathwayPlacementEntity}_new_pathway_domain", "Measurement, Data and Statistics")
    assert[Int](df, s"${PathwayPlacementEntity}_new_pathway_grade", 6)
    assert[String](df, s"${PathwayPlacementEntity}_created_time", "2023-08-13 07:57:35.148")
    assert[String](df, "eventdate", "2023-08-13")

    assert(df.filter(col(s"${PathwayPlacementEntity}_is_initial").isNull).count() == 2)
  }

  test("transform PlacementCompletionEvent when placement type is not defined successfully") {

    val placementCompletionEvent =
      """
        |{
        |  "eventType": "PlacementCompletionEvent",
        |  "tenantId": "tenantId1",
        |  "learnerId": "studentId1",
        |  "pathwayId": "pathwayId1",
        |  "academicYear": "2023-2024",
        |  "classId": "classId1",
        |  "recommendationType": "SOME_RANDOM_PLACEMENT",
        |  "gradeLevel": 6,
        |  "placedBy": "teacherId1",
        |  "uuid": "uuid",
        |  "isInitial": true,
        |  "domainGrades": [
        |    {
        |      "domainName": "Measurement, Data and Statistics",
        |      "grade": 6
        |    },
        |    {
        |      "domainName": "Numbers and Operations",
        |      "grade": 6
        |    }
        |  ],
        |  "recommendedLevels": [
        |      {
        |        "id": "7ae72f7a-5a95-464a-8114-48de4bb66645",
        |        "name": "Level-2",
        |        "status": "ACTIVE"
        |      }
        |  ],
        |  "hasAcceleratedDomains": true,
        |  "occurredOn": "2023-08-13T07:57:35.148"
        |}
        |""".stripMargin

    val sprk = spark
    import sprk.implicits._

    val studentManualPlacementDF = spark.read.json(Seq(placementCompletionEvent).toDS())
    when(service.readOptional("parquet-learning-placement-completed-source", sprk)).thenReturn(Some(studentManualPlacementDF))

    val transformer = new PathwayPlacementTransform(sprk, service)

    val df = transformer.transform().get.output

    assert(df.columns.toSet === expectedColumns)
    assert(df.count() == 2)

    assert[String](df, s"${PathwayPlacementEntity}_tenant_id", "tenantId1")
    assert[String](df, s"${PathwayPlacementEntity}_student_id", "studentId1")
    assert[String](df, s"${PathwayPlacementEntity}_pathway_id", "pathwayId1")
    assert[String](df, s"${PathwayPlacementEntity}_class_id", "classId1")
    assert[Int](df, s"${PathwayPlacementEntity}_overall_grade", 6)
    assert[Boolean](df, s"${PathwayPlacementEntity}_is_initial", true)
    assert[String](df, s"${PathwayPlacementEntity}_created_by", "teacherId1")
    assert[String](df, s"${PathwayPlacementEntity}_new_pathway_domain", "Measurement, Data and Statistics")
    assert[Int](df, s"${PathwayPlacementEntity}_new_pathway_grade", 6)
    assert[String](df, s"${PathwayPlacementEntity}_created_time", "2023-08-13 07:57:35.148")
    assert[String](df, "eventdate", "2023-08-13")

    assert(df.filter(col(s"${PathwayPlacementEntity}_placement_type").isNull).count() == 2)
  }
}
