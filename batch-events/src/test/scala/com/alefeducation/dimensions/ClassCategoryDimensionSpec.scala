package com.alefeducation.dimensions

import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.dimensions.ClassCategoryDimension._

class ClassCategoryDimensionSpec extends SparkSuite with BaseDimensionSpec {

  private val commonColsRS = Seq(
    "class_category_created_time",
    "class_category_dw_created_time",
    "class_category_updated_time",
    "class_category_dw_updated_time"
  )

  test("Test empty DataFrame case") {
    implicit val transformer = new ClassCategoryDimension(spark)

    val fixtures = List(
      SparkFixture(key = ClassCategorySinkParquet, value = "")
    )

    withSparkFixtures(fixtures, sinks => assert(sinks.isEmpty))
  }

  test("ClassCategoryCreatedEvent / ClassCategoryUpdatedEvent combined should be written to parquet sink") {
    implicit val transformer = new ClassCategoryDimension(spark)

    val fixtures = List(
      SparkFixture(
        key = ClassCategorySourceParquet,
        value =
          """
            |[
            |{"eventType":"ClassCategoryCreatedEvent","loadtime":"2020-04-15T08:43:00.357Z","classCategoryId":"1a8d4564-c694-4b02-a5ff-4333f6b498d1","name":"Advance","occurredOn":"2020-04-15 08:43:00.326","eventDateDw":"20200415"},
            |{"eventType":"ClassCategoryCreatedEvent","loadtime":"2020-04-15T08:43:00.357Z","classCategoryId":"1a8d4564-c694-4b02-a5ff-4333f6b498d2","name":"Advance 2","occurredOn":"2020-04-15 08:43:00.326","eventDateDw":"20200415","organisationGlobal":"mora"},
            |{"eventType":"ClassCategoryUpdatedEvent","loadtime":"2020-04-16T08:43:00.357Z","classCategoryId":"1a8d4564-c694-4b02-a5ff-4333f6b498d2","name":"Basic","occurredOn":"2020-04-16 08:43:00.326","eventDateDw":"20200416","organisationGlobal":"mora"},
            |{"eventType":"BadEvent","loadtime":"2020-04-16T08:43:00.357Z","classCategoryId":"1a8d4564-c694-4b02-a5ff-4333f6b498d2","name":"Basic","occurredOn":"2020-04-16 08:43:00.326","eventDateDw":"20200416","organisationGlobal":"mora"}
            |]""".stripMargin
      )
    )

    withSparkFixtures(
      fixtures, { sinks =>
        val expectedCols = (Seq("eventType", "loadtime", "eventDateDw", "eventdate") ++ SourceToSinkColumnMappings.keys.toSeq)

        testSinkBySinkName(sinks, ClassCategorySinkParquet, expectedCols, 4)
      }
    )
  }

  test("ClassCategoryCreatedEvent should be processed to redshift") {
    implicit val transformer = new ClassCategoryDimension(spark)

    val fixtures = List(
      SparkFixture(
        key = ClassCategorySourceParquet,
        value =
          """
            |[
            |{"eventType":"ClassCategoryCreatedEvent","loadtime":"2020-04-15T08:43:00.357Z","classCategoryId":"1a8d4564-c694-4b02-a5ff-4333f6b498d1","name":"Advance","occurredOn":"2020-04-15 08:43:00.326","eventDateDw":"20200415","organisationGlobal":"mora"},
            |{"eventType":"ClassCategoryCreatedEvent","loadtime":"2020-04-15T08:43:00.357Z","classCategoryId":"1a8d4564-c694-4b02-a5ff-4333f6b498d2","name":"Advance 2","occurredOn":"2020-04-15 08:43:00.326","eventDateDw":"20200415","organisationGlobal":"mora"},
            |{"eventType":"BadEvent","loadtime":"2020-04-16T08:43:00.357Z","classCategoryId":"1a8d4564-c694-4b02-a5ff-4333f6b498d2","name":"Basic","occurredOn":"2020-04-16 08:43:00.326","eventDateDw":"20200416","organisationGlobal":"mora"}
            |]""".stripMargin
      )
    )

    withSparkFixtures(
      fixtures, { sinks =>
        val expectedCreatedColsRS = Seq("class_category_id","class_category_name", "class_category_status", "class_category_organization_code") ++ commonColsRS
        testSinkBySinkName(sinks, ClassCategoryCreatedSinkRedshift, expectedCreatedColsRS, 2)
      }
    )
  }

  test("ClassCategoryUpdatedEvent should be processed to redshift") {
    implicit val transformer = new ClassCategoryDimension(spark)

    val fixtures = List(
      SparkFixture(
        key = ClassCategorySourceParquet,
        value =
          """
            |[
            |{"eventType":"ClassCategoryUpdatedEvent","loadtime":"2020-04-15T08:43:00.357Z","classCategoryId":"1a8d4564-c694-4b02-a5ff-4333f6b498d1","name":"Advance","occurredOn":"2020-04-15 08:43:00.326","eventDateDw":"20200415","organisationGlobal":"mora"},
            |{"eventType":"ClassCategoryUpdatedEvent","loadtime":"2020-04-15T08:43:00.357Z","classCategoryId":"1a8d4564-c694-4b02-a5ff-4333f6b498d2","name":"Advance 2","occurredOn":"2020-04-15 08:43:00.326","eventDateDw":"20200415","organisationGlobal":"mora"},
            |{"eventType":"BadEvent","loadtime":"2020-04-16T08:43:00.357Z","classCategoryId":"1a8d4564-c694-4b02-a5ff-4333f6b498d2","name":"Basic","occurredOn":"2020-04-16 08:43:00.326","eventDateDw":"20200416","organisationGlobal":"mora"}
            |]""".stripMargin
      )
    )

    withSparkFixtures(
      fixtures, { sinks =>
        val expectedColsRS = Seq("class_category_id","class_category_name", "class_category_status","class_category_organization_code") ++ commonColsRS
        testSinkBySinkName(sinks, ClassCategoryUpdatedSinkRedshift, expectedColsRS, 2)
      }
    )
  }

  test("ClassCategoryDeletedEvent should be processed to redshift") {
    implicit val transformer = new ClassCategoryDimension(spark)

    val fixtures = List(
      SparkFixture(
        key = ClassCategorySourceParquet,
        value =
          """
            |[
            |{"eventType":"ClassCategoryDeletedEvent","loadtime":"2020-04-15T08:43:00.357Z","classCategoryId":"1a8d4564-c694-4b02-a5ff-4333f6b498d1","name":"Advance","occurredOn":"2020-04-15 08:43:00.326","eventDateDw":"20200415","organisationGlobal":"mora"},
            |{"eventType":"ClassCategoryDeletedEvent","loadtime":"2020-04-15T08:43:00.357Z","classCategoryId":"1a8d4564-c694-4b02-a5ff-4333f6b498d2","name":"Advance 2","occurredOn":"2020-04-15 08:43:00.326","eventDateDw":"20200415","organisationGlobal":"mora"},
            |{"eventType":"BadEvent","loadtime":"2020-04-16T08:43:00.357Z","classCategoryId":"1a8d4564-c694-4b02-a5ff-4333f6b498d2","name":"Basic","occurredOn":"2020-04-16 08:43:00.326","eventDateDw":"20200416","organisationGlobal":"mora"}
            |]""".stripMargin
      )
    )

    withSparkFixtures(
      fixtures, { sinks =>
        val expectedColsRS = Seq("class_category_id","class_category_name","class_category_status","class_category_organization_code") ++ commonColsRS
        testSinkBySinkName(sinks, ClassCategoryDeletedSinkRedshift, expectedColsRS, 2)
      }
    )
  }

  test("ClassCategoryCreatedEvent / ClassCategoryUpdatedEvent combined should be processed to redshift") {
    implicit val transformer = new ClassCategoryDimension(spark)

    val fixtures = List(
      SparkFixture(
        key = ClassCategorySourceParquet,
        value =
          """
            |[
            |{"eventType":"ClassCategoryCreatedEvent","loadtime":"2020-04-15T08:43:00.357Z","classCategoryId":"1a8d4564-c694-4b02-a5ff-4333f6b498d1","name":"Advance","occurredOn":"2020-04-15 08:43:00.326","eventDateDw":"20200415","organisationGlobal":"mora"},
            |{"eventType":"ClassCategoryCreatedEvent","loadtime":"2020-04-15T08:43:00.357Z","classCategoryId":"1a8d4564-c694-4b02-a5ff-4333f6b498d1","name":"Advance","occurredOn":"2020-04-15 08:43:00.326","eventDateDw":"20200415","organisationGlobal":"mora"},
            |{"eventType":"ClassCategoryCreatedEvent","loadtime":"2020-04-15T08:43:00.357Z","classCategoryId":"1a8d4564-c694-4b02-a5ff-4333f6b498d2","name":"Advance","occurredOn":"2020-04-15 08:43:00.326","eventDateDw":"20200415","organisationGlobal":"mora"},
            |{"eventType":"ClassCategoryUpdatedEvent","loadtime":"2020-04-16T08:43:00.357Z","classCategoryId":"1a8d4564-c694-4b02-a5ff-4333f6b498d2","name":"Basic","occurredOn":"2020-04-16 08:43:00.326","eventDateDw":"20200416","organisationGlobal":"mora"},
            |{"eventType":"BadEvent","loadtime":"2020-04-16T08:43:00.357Z","classCategoryId":"1a8d4564-c694-4b02-a5ff-4333f6b498d2","name":"Basic","occurredOn":"2020-04-16 08:43:00.326","eventDateDw":"20200416","organisationGlobal":"mora"}
            |]""".stripMargin
      )
    )

    withSparkFixtures(
      fixtures, { sinks =>
        val expectedColsRS = Seq("class_category_id","class_category_name", "class_category_status","class_category_organization_code") ++ commonColsRS

        testSinkBySinkName(sinks, ClassCategoryCreatedSinkRedshift, expectedColsRS, 2)
        testSinkBySinkName(sinks, ClassCategoryUpdatedSinkRedshift, expectedColsRS, 1)
      }
    )
  }

  test("ClassCategoryCreatedEvent should be processed to delta") {
    implicit val transformer = new ClassCategoryDimension(spark)

    val fixtures = List(
      SparkFixture(
        key = ClassCategorySourceParquet,
        value =
          """
            |[
            |{"eventType":"ClassCategoryCreatedEvent","loadtime":"2020-04-15T08:43:00.357Z","classCategoryId":"1a8d4564-c694-4b02-a5ff-4333f6b498d1","name":"Advance","occurredOn":"2020-04-15 08:43:00.326","eventDateDw":"20200415","organisationGlobal":"mora"},
            |{"eventType":"ClassCategoryCreatedEvent","loadtime":"2020-04-15T08:43:00.357Z","classCategoryId":"1a8d4564-c694-4b02-a5ff-4333f6b498d2","name":"Advance 2","occurredOn":"2020-04-15 08:43:00.326","eventDateDw":"20200415","organisationGlobal":"mora"},
            |{"eventType":"BadEvent","loadtime":"2020-04-16T08:43:00.357Z","classCategoryId":"1a8d4564-c694-4b02-a5ff-4333f6b498d2","name":"Basic","occurredOn":"2020-04-16 08:43:00.326","eventDateDw":"20200416","organisationGlobal":"mora"}
            |]""".stripMargin
      )
    )

    withSparkFixtures(
      fixtures, { sinks =>
        val expectedCreatedCols = Seq("class_category_id","class_category_name", "class_category_status","class_category_organization_code") ++ commonColsRS
        testSinkBySinkName(sinks, ClassCategoryCreatedSinkDelta, expectedCreatedCols, 2)
      }
    )
  }

  test("ClassCategoryUpdatedEvent should be processed to delta") {
    implicit val transformer = new ClassCategoryDimension(spark)

    val fixtures = List(
      SparkFixture(
        key = ClassCategorySourceParquet,
        value =
          """
            |[
            |{"eventType":"ClassCategoryUpdatedEvent","loadtime":"2020-04-15T08:43:00.357Z","classCategoryId":"1a8d4564-c694-4b02-a5ff-4333f6b498d1","name":"Advance","occurredOn":"2020-04-15 08:43:00.326","eventDateDw":"20200415","organisationGlobal":"mora"},
            |{"eventType":"ClassCategoryUpdatedEvent","loadtime":"2020-04-15T08:43:00.357Z","classCategoryId":"1a8d4564-c694-4b02-a5ff-4333f6b498d2","name":"Advance 2","occurredOn":"2020-04-15 08:43:00.326","eventDateDw":"20200415","organisationGlobal":"mora"},
            |{"eventType":"BadEvent","loadtime":"2020-04-16T08:43:00.357Z","classCategoryId":"1a8d4564-c694-4b02-a5ff-4333f6b498d2","name":"Basic","occurredOn":"2020-04-16 08:43:00.326","eventDateDw":"20200416","organisationGlobal":"mora"}
            |]""".stripMargin
      )
    )

    withSparkFixtures(
      fixtures, { sinks =>
        val expectedCols = Seq("class_category_id","class_category_name", "class_category_status","class_category_organization_code") ++ commonColsRS
        testSinkBySinkName(sinks, ClassCategoryUpdatedSinkDelta, expectedCols, 2)
      }
    )
  }

  test("ClassCategoryDeletedEvent should be processed to delta") {
    implicit val transformer = new ClassCategoryDimension(spark)

    val fixtures = List(
      SparkFixture(
        key = ClassCategorySourceParquet,
        value =
          """
            |[
            |{"eventType":"ClassCategoryDeletedEvent","loadtime":"2020-04-15T08:43:00.357Z","classCategoryId":"1a8d4564-c694-4b02-a5ff-4333f6b498d1","name":"Advance","occurredOn":"2020-04-15 08:43:00.326","eventDateDw":"20200415","organisationGlobal":"mora"},
            |{"eventType":"ClassCategoryDeletedEvent","loadtime":"2020-04-15T08:43:00.357Z","classCategoryId":"1a8d4564-c694-4b02-a5ff-4333f6b498d2","name":"Advance 2","occurredOn":"2020-04-15 08:43:00.326","eventDateDw":"20200415","organisationGlobal":"mora"},
            |{"eventType":"BadEvent","loadtime":"2020-04-16T08:43:00.357Z","classCategoryId":"1a8d4564-c694-4b02-a5ff-4333f6b498d2","name":"Basic","occurredOn":"2020-04-16 08:43:00.326","eventDateDw":"20200416","organisationGlobal":"mora"}
            |]""".stripMargin
      )
    )

    withSparkFixtures(
      fixtures, { sinks =>
        val expectedCols = Seq("class_category_id","class_category_name", "class_category_status","class_category_organization_code") ++ commonColsRS
        testSinkBySinkName(sinks, ClassCategoryDeletedSinkDelta, expectedCols, 2)
      }
    )
  }

  test("ClassCategoryCreatedEvent / ClassCategoryUpdatedEvent combined should be processed to delta") {
    implicit val transformer = new ClassCategoryDimension(spark)

    val fixtures = List(
      SparkFixture(
        key = ClassCategorySourceParquet,
        value =
          """
            |[
            |{"eventType":"ClassCategoryCreatedEvent","loadtime":"2020-04-15T08:43:00.357Z","classCategoryId":"1a8d4564-c694-4b02-a5ff-4333f6b498d1","name":"Advance","occurredOn":"2020-04-15 08:43:00.326","eventDateDw":"20200415","organisationGlobal":"mora"},
            |{"eventType":"ClassCategoryCreatedEvent","loadtime":"2020-04-15T08:43:00.357Z","classCategoryId":"1a8d4564-c694-4b02-a5ff-4333f6b498d1","name":"Advance","occurredOn":"2020-04-15 08:43:00.326","eventDateDw":"20200415","organisationGlobal":"mora"},
            |{"eventType":"ClassCategoryCreatedEvent","loadtime":"2020-04-15T08:43:00.357Z","classCategoryId":"1a8d4564-c694-4b02-a5ff-4333f6b498d2","name":"Advance","occurredOn":"2020-04-15 08:43:00.326","eventDateDw":"20200415","organisationGlobal":"mora"},
            |{"eventType":"ClassCategoryUpdatedEvent","loadtime":"2020-04-16T08:43:00.357Z","classCategoryId":"1a8d4564-c694-4b02-a5ff-4333f6b498d2","name":"Basic","occurredOn":"2020-04-16 08:43:00.326","eventDateDw":"20200416","organisationGlobal":"mora"},
            |{"eventType":"BadEvent","loadtime":"2020-04-16T08:43:00.357Z","classCategoryId":"1a8d4564-c694-4b02-a5ff-4333f6b498d2","name":"Basic","occurredOn":"2020-04-16 08:43:00.326","eventDateDw":"20200416"}
            |]""".stripMargin
      )
    )

    withSparkFixtures(
      fixtures, { sinks =>
        val expectedCols = Seq("class_category_id","class_category_name", "class_category_status","class_category_organization_code") ++ commonColsRS

        testSinkBySinkName(sinks, ClassCategoryCreatedSinkDelta, expectedCols, 2)
        testSinkBySinkName(sinks, ClassCategoryUpdatedSinkDelta, expectedCols, 1)
      }
    )
  }
}
