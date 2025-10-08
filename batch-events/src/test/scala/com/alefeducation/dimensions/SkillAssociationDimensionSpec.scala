package com.alefeducation.dimensions

import com.alefeducation.bigdata.Sink
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.service.DataSink
import com.alefeducation.util.Helpers._
import org.apache.spark.sql.functions._
import org.scalatest.matchers.must.Matchers

class SkillAssociationDimensionSpec extends SparkSuite with Matchers {

  private val SkillAssociationExpectedCols = SkillAssociationColumns.values.toSet
    .diff(Set("occurredOn")) ++ dimDateCols(SkillAssociationEntity).toSet
    .diff(Set("skill_association_deleted_time", "skill_association_active_until"))

  trait Setup {
    implicit val transformer = new SkillAssociationDimension(SkillAssociationDimensionName, spark)
  }

  test("Test empty DataFrame case") {
    new Setup {
      val fixtures = List(
        SparkFixture(key = ParquetSkillLinkToggleSource, value = ""),
        SparkFixture(key = ParquetSkillCategoryLinkToggleSource, value = "")
      )

      withSparkFixtures(fixtures, sinks => assert(sinks.isEmpty))
    }
  }

  test("Skill Linked with Skill ") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetSkillLinkToggleSource,
          value = """
              |{
              |  "eventType": "SkillLinkedEvent",
              |  "skillId": "skill1",
              |  "skillCode": "code1",
              |  "nextSkillId": "skillNext",
              |  "nextSkillCode": "code2",
              |  "occurredOn": "2019-01-08 02:40:00.0"
              |}
      """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val s3Sink: Sink = sinks.filter(_.name == ParquetSkillLinkToggleSource).head
          assert(s3Sink.asInstanceOf[DataSink].options.get("partitionBy") === Some("eventdate"))
          assert[String](s3Sink.output, "eventdate", "2019-01-08")

          val redshiftSink = sinks.filter(_.name == RedshiftSkillAssociationSink).head
          val skillAssociationDf = redshiftSink.output.cache
          assert(skillAssociationDf.count == 1)
          assert(skillAssociationDf.columns.toSet === SkillAssociationExpectedCols)
          assert[String](skillAssociationDf, "skill_association_id", "skillNext")
          assert[String](skillAssociationDf, "skill_association_code", "code2")
          assert[String](skillAssociationDf, "skill_association_skill_id", "skill1")
          assert[Int](skillAssociationDf, "skill_association_status", 1)
          assert[Int](skillAssociationDf, "skill_association_type", 1)
          assert[Int](skillAssociationDf, "skill_association_attach_status", 1)
          assert[String](skillAssociationDf, "skill_association_skill_code", "code1")
          assert[Boolean](skillAssociationDf, "skill_association_is_previous", false)

          val deltaSkillAssociationSink = sinks.filter(_.name == DeltaSkillAssociationSink).head
          val deltaSkillAssociationDf = deltaSkillAssociationSink.output.cache
          assert(deltaSkillAssociationDf.count == 1)
          assert(deltaSkillAssociationDf.columns.toSet === SkillAssociationExpectedCols)
          assert[String](deltaSkillAssociationDf, "skill_association_id", "skillNext")
          assert[String](deltaSkillAssociationDf, "skill_association_code", "code2")
          assert[String](deltaSkillAssociationDf, "skill_association_skill_id", "skill1")
          assert[Int](deltaSkillAssociationDf, "skill_association_status", 1)
          assert[Int](deltaSkillAssociationDf, "skill_association_type", 1)
          assert[Int](deltaSkillAssociationDf, "skill_association_attach_status", 1)
          assert[String](deltaSkillAssociationDf, "skill_association_skill_code", "code1")
          assert[Boolean](deltaSkillAssociationDf, "skill_association_is_previous", false)
        }
      )
    }
  }

  test("Skill Linked with Skill V2") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetSkillLinkToggleV2Source,
          value = """
                    |{
                    |  "eventType": "SkillLinkedEventV2",
                    |  "skillId":"b961ee38-b4a2-4888-93e3-ae770d8a2d08",
                    |  "skillCode":"SK.169",
                    |  "previousSkillId":"4b453063-82db-4cbd-8fbc-9a98a6077220",
                    |  "previousSkillCode":"SK.155",
                    |  "occurredOn": "2022-03-14 06:06:11.0"
                    |}
      """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val s3Sink: Sink = sinks.filter(_.name == ParquetSkillLinkToggleV2Source).head
          assert(s3Sink.asInstanceOf[DataSink].options.get("partitionBy") === Some("eventdate"))
          assert[String](s3Sink.output, "eventdate", "2022-03-14")

          val redshiftSink = sinks.filter(_.name == RedshiftSkillAssociationSink).head
          val skillAssociationDf = redshiftSink.output.cache
          assert(skillAssociationDf.count == 1)
          assert(skillAssociationDf.columns.toSet === SkillAssociationExpectedCols)
          assert[String](skillAssociationDf, "skill_association_id", "4b453063-82db-4cbd-8fbc-9a98a6077220")
          assert[String](skillAssociationDf, "skill_association_code", "SK.155")
          assert[String](skillAssociationDf, "skill_association_skill_id", "b961ee38-b4a2-4888-93e3-ae770d8a2d08")
          assert[Int](skillAssociationDf, "skill_association_status", 1)
          assert[Int](skillAssociationDf, "skill_association_type", 1)
          assert[Int](skillAssociationDf, "skill_association_attach_status", 1)
          assert[String](skillAssociationDf, "skill_association_skill_code", "SK.169")
          assert[Boolean](skillAssociationDf, "skill_association_is_previous", true)

          val deltaSkillAssociationSink = sinks.filter(_.name == DeltaSkillAssociationSink).head
          val deltaSkillAssociationDf = deltaSkillAssociationSink.output.cache
          assert(deltaSkillAssociationDf.count == 1)
          assert(deltaSkillAssociationDf.columns.toSet === SkillAssociationExpectedCols)
          assert[String](deltaSkillAssociationDf, "skill_association_id", "4b453063-82db-4cbd-8fbc-9a98a6077220")
          assert[String](deltaSkillAssociationDf, "skill_association_code", "SK.155")
          assert[String](deltaSkillAssociationDf, "skill_association_skill_id", "b961ee38-b4a2-4888-93e3-ae770d8a2d08")
          assert[Int](deltaSkillAssociationDf, "skill_association_status", 1)
          assert[Int](deltaSkillAssociationDf, "skill_association_type", 1)
          assert[Int](deltaSkillAssociationDf, "skill_association_attach_status", 1)
          assert[String](deltaSkillAssociationDf, "skill_association_skill_code", "SK.169")
          assert[Boolean](deltaSkillAssociationDf, "skill_association_is_previous", true)

        }
      )
    }
  }

  test("Skill Linked/Unlinked with Skill ") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetSkillLinkToggleSource,
          value = """
                    |[{
                    |  "eventType": "SkillLinkedEvent",
                    |  "skillId": "skill1",
                    |  "skillCode": "code1",
                    |  "nextSkillId": "skillNext",
                    |  "nextSkillCode": "code2",
                    |  "occurredOn": "2019-01-08 02:40:00.0"
                    |},
                    |{
                    |  "eventType": "SkillLinkedEvent",
                    |  "skillId": "skill2",
                    |  "skillCode": "code3",
                    |  "nextSkillId": "skillNext",
                    |  "nextSkillCode": "code2",
                    |  "occurredOn": "2019-01-08 03:40:00.0"
                    |},
                    |{
                    |  "eventType": "SkillUnLinkedEvent",
                    |  "skillId": "skill1",
                    |  "skillCode": "code1",
                    |  "nextSkillId": "skillNext",
                    |  "nextSkillCode": "code2",
                    |  "occurredOn": "2019-01-08 04:40:00.0"
                    |},
                    |{
                    |  "eventType": "SkillLinkedEvent",
                    |  "skillId": "skill1",
                    |  "skillCode": "code1",
                    |  "nextSkillId": "skillNext",
                    |  "nextSkillCode": "code2",
                    |  "occurredOn": "2019-01-08 05:40:00.0"
                    |},
                    |{
                    |  "eventType": "SkillUnLinkedEvent",
                    |  "skillId": "skill2",
                    |  "skillCode": "code3",
                    |  "nextSkillId": "skillNext",
                    |  "nextSkillCode": "code2",
                    |  "occurredOn": "2019-01-08 03:42:00.0"
                    |}]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val s3Sink: Sink = sinks.filter(_.name == ParquetSkillLinkToggleSource).head
          assert(s3Sink.asInstanceOf[DataSink].options.get("partitionBy") === Some("eventdate"))
          assert[String](s3Sink.output, "eventdate", "2019-01-08")

          val redshiftSink = sinks.filter(_.name == RedshiftSkillAssociationSink).head
          val skillAssociationDf = redshiftSink.output.cache
          assert(skillAssociationDf.count == 5)
          assert(skillAssociationDf.columns.toSet === SkillAssociationExpectedCols)
          val skillLinkedDfLatest =
            skillAssociationDf.filter(col("skill_association_attach_status") === 1 && col("skill_association_status") === 1)
          val skillUnLinkedDfLatest =
            skillAssociationDf.filter(col("skill_association_attach_status") === 0 && col("skill_association_status") === 1)
          assert[String](skillLinkedDfLatest, "skill_association_id", "skillNext")
          assert[String](skillLinkedDfLatest, "skill_association_code", "code2")
          assert[String](skillLinkedDfLatest, "skill_association_skill_id", "skill1")
          assert[Int](skillLinkedDfLatest, "skill_association_type", 1)
          assert[String](skillLinkedDfLatest, "skill_association_skill_code", "code1")
          assert[Boolean](skillLinkedDfLatest, "skill_association_is_previous", false)

          assert[String](skillUnLinkedDfLatest, "skill_association_id", "skillNext")
          assert[String](skillUnLinkedDfLatest, "skill_association_code", "code2")
          assert[String](skillUnLinkedDfLatest, "skill_association_skill_id", "skill2")
          assert[Int](skillUnLinkedDfLatest, "skill_association_type", 1)
          assert[String](skillUnLinkedDfLatest, "skill_association_skill_code", "code3")
          assert[Boolean](skillUnLinkedDfLatest, "skill_association_is_previous", false)

          val deltaSkillAssociationSink = sinks.filter(_.name == DeltaSkillAssociationSink).head
          val deltaSkillAssociationDf = deltaSkillAssociationSink.output.cache
          assert(deltaSkillAssociationDf.count == 5)
          assert(deltaSkillAssociationDf.columns.toSet === SkillAssociationExpectedCols)
          val skillLinkedDfDeltaLatest =
            deltaSkillAssociationDf.filter(col("skill_association_attach_status") === 1 && col("skill_association_status") === 1)
          val skillUnLinkedDfDeltaLatest =
            deltaSkillAssociationDf.filter(col("skill_association_attach_status") === 0 && col("skill_association_status") === 1)
          assert[String](skillLinkedDfDeltaLatest, "skill_association_id", "skillNext")
          assert[String](skillLinkedDfDeltaLatest, "skill_association_code", "code2")
          assert[String](skillLinkedDfDeltaLatest, "skill_association_skill_id", "skill1")
          assert[Int](skillLinkedDfDeltaLatest, "skill_association_type", 1)
          assert[String](skillLinkedDfDeltaLatest, "skill_association_skill_code", "code1")
          assert[Boolean](skillLinkedDfDeltaLatest, "skill_association_is_previous", false)

          assert[String](skillUnLinkedDfDeltaLatest, "skill_association_id", "skillNext")
          assert[String](skillUnLinkedDfDeltaLatest, "skill_association_code", "code2")
          assert[String](skillUnLinkedDfDeltaLatest, "skill_association_skill_id", "skill2")
          assert[Int](skillUnLinkedDfDeltaLatest, "skill_association_type", 1)
          assert[String](skillUnLinkedDfDeltaLatest, "skill_association_skill_code", "code3")
          assert[Boolean](skillUnLinkedDfDeltaLatest, "skill_association_is_previous", false)
        }
      )
    }
  }

  test("Skill Linked/Unlinked with Skill V2") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetSkillLinkToggleV2Source,
          value = """
                    |[{
                    |  "eventType": "SkillLinkedEventV2",
                    |  "skillId": "skill1",
                    |  "skillCode": "code1",
                    |  "previousSkillId": "skillPrev",
                    |  "previousSkillCode": "code2",
                    |  "occurredOn": "2019-01-08 02:40:00.0"
                    |},
                    |{
                    |  "eventType": "SkillLinkedEventV2",
                    |  "skillId": "skill2",
                    |  "skillCode": "code3",
                    |  "previousSkillId": "skillPrev",
                    |  "previousSkillCode": "code2",
                    |  "occurredOn": "2019-01-08 03:40:00.0"
                    |},
                    |{
                    |  "eventType": "SkillUnLinkedEventV2",
                    |  "skillId": "skill1",
                    |  "skillCode": "code1",
                    |  "previousSkillId": "skillPrev",
                    |  "previousSkillCode": "code2",
                    |  "occurredOn": "2019-01-08 04:40:00.0"
                    |},
                    |{
                    |  "eventType": "SkillLinkedEventV2",
                    |  "skillId": "skill1",
                    |  "skillCode": "code1",
                    |  "previousSkillId": "skillPrev",
                    |  "previousSkillCode": "code2",
                    |  "occurredOn": "2019-01-08 05:40:00.0"
                    |},
                    |{
                    |  "eventType": "SkillUnLinkedEventV2",
                    |  "skillId": "skill2",
                    |  "skillCode": "code3",
                    |  "previousSkillId": "skillPrev",
                    |  "previousSkillCode": "code2",
                    |  "occurredOn": "2019-01-08 03:42:00.0"
                    |}]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val s3Sink: Sink = sinks.filter(_.name == ParquetSkillLinkToggleV2Source).head
          assert(s3Sink.asInstanceOf[DataSink].options.get("partitionBy") === Some("eventdate"))
          assert[String](s3Sink.output, "eventdate", "2019-01-08")

          val redshiftSink = sinks.filter(_.name == RedshiftSkillAssociationSink).head
          val skillAssociationDf = redshiftSink.output.cache
          assert(skillAssociationDf.count == 5)
          assert(skillAssociationDf.columns.toSet === SkillAssociationExpectedCols)
          val skillLinkedDfLatest =
            skillAssociationDf.filter(col("skill_association_attach_status") === 1 && col("skill_association_status") === 1)
          val skillUnLinkedDfLatest =
            skillAssociationDf.filter(col("skill_association_attach_status") === 0 && col("skill_association_status") === 1)
          assert[String](skillLinkedDfLatest, "skill_association_id", "skillPrev")
          assert[String](skillLinkedDfLatest, "skill_association_code", "code2")
          assert[String](skillLinkedDfLatest, "skill_association_skill_id", "skill1")
          assert[Int](skillLinkedDfLatest, "skill_association_type", 1)
          assert[String](skillLinkedDfLatest, "skill_association_skill_code", "code1")
          assert[Boolean](skillLinkedDfLatest, "skill_association_is_previous", true)

          assert[String](skillUnLinkedDfLatest, "skill_association_id", "skillPrev")
          assert[String](skillUnLinkedDfLatest, "skill_association_code", "code2")
          assert[String](skillUnLinkedDfLatest, "skill_association_skill_id", "skill2")
          assert[Int](skillUnLinkedDfLatest, "skill_association_type", 1)
          assert[String](skillUnLinkedDfLatest, "skill_association_skill_code", "code3")
          assert[Boolean](skillUnLinkedDfLatest, "skill_association_is_previous", true)

          val deltaSkillAssociationSink = sinks.filter(_.name == DeltaSkillAssociationSink).head
          val deltaSkillAssociationDf = deltaSkillAssociationSink.output.cache
          assert(deltaSkillAssociationDf.count == 5)
          assert(deltaSkillAssociationDf.columns.toSet === SkillAssociationExpectedCols)
          val skillLinkedDfDeltaLatest =
            deltaSkillAssociationDf.filter(col("skill_association_attach_status") === 1 && col("skill_association_status") === 1)
          val skillUnLinkedDfDeltaLatest =
            deltaSkillAssociationDf.filter(col("skill_association_attach_status") === 0 && col("skill_association_status") === 1)
          assert[String](skillLinkedDfDeltaLatest, "skill_association_id", "skillPrev")
          assert[String](skillLinkedDfDeltaLatest, "skill_association_code", "code2")
          assert[String](skillLinkedDfDeltaLatest, "skill_association_skill_id", "skill1")
          assert[Int](skillLinkedDfDeltaLatest, "skill_association_type", 1)
          assert[String](skillLinkedDfDeltaLatest, "skill_association_skill_code", "code1")
          assert[Boolean](skillLinkedDfDeltaLatest, "skill_association_is_previous", true)

          assert[String](skillUnLinkedDfDeltaLatest, "skill_association_id", "skillPrev")
          assert[String](skillUnLinkedDfDeltaLatest, "skill_association_code", "code2")
          assert[String](skillUnLinkedDfDeltaLatest, "skill_association_skill_id", "skill2")
          assert[Int](skillUnLinkedDfDeltaLatest, "skill_association_type", 1)
          assert[String](skillUnLinkedDfDeltaLatest, "skill_association_skill_code", "code3")
          assert[Boolean](skillUnLinkedDfDeltaLatest, "skill_association_is_previous", true)
        }
      )
    }
  }

  test("Skill Linked/Unlinked with Category ") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetSkillCategoryLinkToggleSource,
          value = """
                    |[{
                    |  "eventType": "SkillCategoryLinkedEvent",
                    |  "skillId": "skill1",
                    |  "skillCode": "code1",
                    |  "categoryId": "cat1",
                    |  "occurredOn": "2019-01-08 02:40:00.0"
                    |},
                    |{
                    |  "eventType": "SkillCategoryLinkedEvent",
                    |  "skillId": "skill2",
                    |  "skillCode": "code3",
                    |  "categoryId": "cat2",
                    |  "occurredOn": "2019-01-08 03:40:00.0"
                    |},
                    |{
                    |  "eventType": "SkillCategoryUnLinkedEvent",
                    |  "skillId": "skill1",
                    |  "skillCode": "code1",
                    |  "categoryId": "cat1",
                    |  "occurredOn": "2019-01-08 04:40:00.0"
                    |},
                    |{
                    |  "eventType": "SkillCategoryLinkedEvent",
                    |  "skillId": "skill1",
                    |  "skillCode": "code1",
                    |  "categoryId": "cat1",
                    |  "occurredOn": "2019-01-08 05:40:00.0"
                    |},
                    |{
                    |  "eventType": "SkillCategoryUnLinkedEvent",
                    |  "skillId": "skill2",
                    |  "skillCode": "code3",
                    |  "categoryId": "cat2",
                    |  "occurredOn": "2019-01-08 03:42:00.0"
                    |}]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val s3Sink: Sink = sinks.filter(_.name == ParquetSkillCategoryLinkToggleSource).head
          assert(s3Sink.asInstanceOf[DataSink].options.get("partitionBy") === Some("eventdate"))
          assert[String](s3Sink.output, "eventdate", "2019-01-08")

          val redshiftSink = sinks.filter(_.name == RedshiftSkillAssociationSink).head
          val skillAssociationDf = redshiftSink.output.cache
          assert(skillAssociationDf.count == 5)
          assert(skillAssociationDf.columns.toSet === SkillAssociationExpectedCols)
          val skillCategoryLinkedDfLatest =
            skillAssociationDf.filter(col("skill_association_attach_status") === 1 && col("skill_association_status") === 1)
          val skillCategoryUnLinkedDfLatest =
            skillAssociationDf.filter(col("skill_association_attach_status") === 0 && col("skill_association_status") === 1)
          assert[String](skillCategoryLinkedDfLatest, "skill_association_id", "cat1")
          assert[String](skillCategoryLinkedDfLatest, "skill_association_code", null)
          assert[String](skillCategoryLinkedDfLatest, "skill_association_skill_id", "skill1")
          assert[Int](skillCategoryLinkedDfLatest, "skill_association_type", 2)
          assert[String](skillCategoryLinkedDfLatest, "skill_association_skill_code", "code1")
          assert[Boolean](skillCategoryLinkedDfLatest, "skill_association_is_previous", false)

          assert[String](skillCategoryUnLinkedDfLatest, "skill_association_id", "cat2")
          assert[String](skillCategoryUnLinkedDfLatest, "skill_association_code", null)
          assert[String](skillCategoryUnLinkedDfLatest, "skill_association_skill_id", "skill2")
          assert[Int](skillCategoryUnLinkedDfLatest, "skill_association_type", 2)
          assert[String](skillCategoryUnLinkedDfLatest, "skill_association_skill_code", "code3")
          assert[Boolean](skillCategoryUnLinkedDfLatest, "skill_association_is_previous", false)

          val deltaSkillAssociationSink = sinks.filter(_.name == DeltaSkillAssociationSink).head
          val deltaSkillAssociationDf = deltaSkillAssociationSink.output.cache
          assert(deltaSkillAssociationDf.count == 5)
          assert(deltaSkillAssociationDf.columns.toSet === SkillAssociationExpectedCols)
          val skillCategoryLinkedDeltaDfLatest =
            deltaSkillAssociationDf.filter(col("skill_association_attach_status") === 1 && col("skill_association_status") === 1)
          val skillCategoryUnLinkedDeltaDfLatest =
            deltaSkillAssociationDf.filter(col("skill_association_attach_status") === 0 && col("skill_association_status") === 1)
          assert[String](skillCategoryLinkedDeltaDfLatest, "skill_association_id", "cat1")
          assert[String](skillCategoryLinkedDeltaDfLatest, "skill_association_code", null)
          assert[String](skillCategoryLinkedDeltaDfLatest, "skill_association_skill_id", "skill1")
          assert[Int](skillCategoryLinkedDeltaDfLatest, "skill_association_type", 2)
          assert[String](skillCategoryLinkedDeltaDfLatest, "skill_association_skill_code", "code1")
          assert[Boolean](skillCategoryLinkedDeltaDfLatest, "skill_association_is_previous", false)

          assert[String](skillCategoryUnLinkedDeltaDfLatest, "skill_association_id", "cat2")
          assert[String](skillCategoryUnLinkedDeltaDfLatest, "skill_association_code", null)
          assert[String](skillCategoryUnLinkedDeltaDfLatest, "skill_association_skill_id", "skill2")
          assert[Int](skillCategoryUnLinkedDeltaDfLatest, "skill_association_type", 2)
          assert[String](skillCategoryUnLinkedDeltaDfLatest, "skill_association_skill_code", "code3")
          assert[Boolean](skillCategoryUnLinkedDeltaDfLatest, "skill_association_is_previous", false)

        }
      )
    }
  }

  test("Skill Linked/Unlinked with Category and Skill ") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetSkillCategoryLinkToggleSource,
          value = """
                    |[{
                    |  "eventType": "SkillCategoryLinkedEvent",
                    |  "skillId": "skill1",
                    |  "skillCode": "code1",
                    |  "categoryId": "cat1",
                    |  "occurredOn": "2019-01-08 02:40:00.0"
                    |},
                    |{
                    |  "eventType": "SkillCategoryLinkedEvent",
                    |  "skillId": "skill2",
                    |  "skillCode": "code3",
                    |  "categoryId": "cat2",
                    |  "occurredOn": "2019-01-08 03:40:00.0"
                    |},
                    |{
                    |  "eventType": "SkillCategoryUnLinkedEvent",
                    |  "skillId": "skill1",
                    |  "skillCode": "code1",
                    |  "categoryId": "cat1",
                    |  "occurredOn": "2019-01-08 04:40:00.0"
                    |},
                    |{
                    |  "eventType": "SkillCategoryLinkedEvent",
                    |  "skillId": "skill1",
                    |  "skillCode": "code1",
                    |  "categoryId": "cat1",
                    |  "occurredOn": "2019-01-08 05:40:00.0"
                    |},
                    |{
                    |  "eventType": "SkillCategoryUnLinkedEvent",
                    |  "skillId": "skill2",
                    |  "skillCode": "code3",
                    |  "categoryId": "cat2",
                    |  "occurredOn": "2019-01-08 03:42:00.0"
                    |}]
                  """.stripMargin
        ),
        SparkFixture(
          key = ParquetSkillLinkToggleSource,
          value = """
                    |[{
                    |  "eventType": "SkillLinkedEvent",
                    |  "skillId": "skill1",
                    |  "skillCode": "code1",
                    |  "nextSkillId": "skillNext",
                    |  "nextSkillCode": "code2",
                    |  "occurredOn": "2019-01-08 02:40:00.0"
                    |},
                    |{
                    |  "eventType": "SkillLinkedEvent",
                    |  "skillId": "skill2",
                    |  "skillCode": "code3",
                    |  "nextSkillId": "skillNext",
                    |  "nextSkillCode": "code2",
                    |  "occurredOn": "2019-01-08 03:40:00.0"
                    |},
                    |{
                    |  "eventType": "SkillUnLinkedEvent",
                    |  "skillId": "skill1",
                    |  "skillCode": "code1",
                    |  "nextSkillId": "skillNext",
                    |  "nextSkillCode": "code2",
                    |  "occurredOn": "2019-01-08 04:40:00.0"
                    |}
                    |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val redshiftSink = sinks.filter(_.name == RedshiftSkillAssociationSink)
          val skillAssociationDf = redshiftSink.head.output.cache()
          assert(skillAssociationDf.count == 8)
          assert(skillAssociationDf.columns.toSet === SkillAssociationExpectedCols)
          val skillCategoryLinkedDfLatest =
            skillAssociationDf.filter(
              col("skill_association_attach_status") === 1 && col("skill_association_status") === 1
                && col("skill_association_type") === 2)
          val skillCategoryUnLinkedDfLatest =
            skillAssociationDf.filter(
              col("skill_association_attach_status") === 0 && col("skill_association_status") === 1
                && col("skill_association_type") === 2)
          assert[String](skillCategoryLinkedDfLatest, "skill_association_id", "cat1")
          assert[String](skillCategoryLinkedDfLatest, "skill_association_code", null)
          assert[String](skillCategoryLinkedDfLatest, "skill_association_skill_id", "skill1")
          assert[String](skillCategoryLinkedDfLatest, "skill_association_skill_code", "code1")
          assert[Boolean](skillCategoryLinkedDfLatest, "skill_association_is_previous", false)

          assert[String](skillCategoryUnLinkedDfLatest, "skill_association_id", "cat2")
          assert[String](skillCategoryUnLinkedDfLatest, "skill_association_code", null)
          assert[String](skillCategoryUnLinkedDfLatest, "skill_association_skill_id", "skill2")
          assert[Boolean](skillCategoryUnLinkedDfLatest, "skill_association_is_previous", false)

          val skillLinkedDfLatest =
            skillAssociationDf.filter(
              col("skill_association_attach_status") === 1 && col("skill_association_status") === 1
                && col("skill_association_type") === 1)
          val skillUnLinkedDfLatest =
            skillAssociationDf.filter(
              col("skill_association_attach_status") === 0 && col("skill_association_status") === 1
                && col("skill_association_type") === 1)
          assert[String](skillLinkedDfLatest, "skill_association_id", "skillNext")
          assert[String](skillLinkedDfLatest, "skill_association_code", "code2")
          assert[String](skillLinkedDfLatest, "skill_association_skill_id", "skill2")
          assert[String](skillLinkedDfLatest, "skill_association_skill_code", "code3")
          assert[Boolean](skillLinkedDfLatest, "skill_association_is_previous", false)

          assert[String](skillUnLinkedDfLatest, "skill_association_id", "skillNext")
          assert[String](skillUnLinkedDfLatest, "skill_association_code", "code2")
          assert[String](skillUnLinkedDfLatest, "skill_association_skill_id", "skill1")
          assert[String](skillUnLinkedDfLatest, "skill_association_skill_code", "code1")
          assert[Boolean](skillUnLinkedDfLatest, "skill_association_is_previous", false)

          val deltaSink = sinks.filter(_.name == DeltaSkillAssociationSink)
          val deltaAssociationDf = deltaSink.head.output.cache()
          assert(deltaAssociationDf.count == 8)
          assert(deltaAssociationDf.columns.toSet === SkillAssociationExpectedCols)
          val skillCategoryLinkedDeltaDfLatest =
            deltaAssociationDf.filter(
              col("skill_association_attach_status") === 1 && col("skill_association_status") === 1
                && col("skill_association_type") === 2)
          val skillCategoryUnLinkedDeltaDfLatest =
            deltaAssociationDf.filter(
              col("skill_association_attach_status") === 0 && col("skill_association_status") === 1
                && col("skill_association_type") === 2)
          assert[String](skillCategoryLinkedDeltaDfLatest, "skill_association_id", "cat1")
          assert[String](skillCategoryLinkedDeltaDfLatest, "skill_association_code", null)
          assert[String](skillCategoryLinkedDeltaDfLatest, "skill_association_skill_id", "skill1")
          assert[String](skillCategoryLinkedDeltaDfLatest, "skill_association_skill_code", "code1")
          assert[Boolean](skillCategoryLinkedDeltaDfLatest, "skill_association_is_previous", false)

          assert[String](skillCategoryUnLinkedDeltaDfLatest, "skill_association_id", "cat2")
          assert[String](skillCategoryUnLinkedDeltaDfLatest, "skill_association_code", null)
          assert[String](skillCategoryUnLinkedDeltaDfLatest, "skill_association_skill_id", "skill2")
          assert[String](skillCategoryUnLinkedDeltaDfLatest, "skill_association_skill_code", "code3")
          assert[Boolean](skillCategoryUnLinkedDeltaDfLatest, "skill_association_is_previous", false)

          val skillLinkedDfDeltaLatest =
            deltaAssociationDf.filter(
              col("skill_association_attach_status") === 1 && col("skill_association_status") === 1
                && col("skill_association_type") === 1)
          val skillUnLinkedDeltaDfLatest =
            deltaAssociationDf.filter(
              col("skill_association_attach_status") === 0 && col("skill_association_status") === 1
                && col("skill_association_type") === 1)
          assert[String](skillLinkedDfDeltaLatest, "skill_association_id", "skillNext")
          assert[String](skillLinkedDfDeltaLatest, "skill_association_code", "code2")
          assert[String](skillLinkedDfDeltaLatest, "skill_association_skill_id", "skill2")
          assert[String](skillLinkedDfDeltaLatest, "skill_association_skill_code", "code3")
          assert[Boolean](skillLinkedDfDeltaLatest, "skill_association_is_previous", false)

          assert[String](skillUnLinkedDeltaDfLatest, "skill_association_id", "skillNext")
          assert[String](skillUnLinkedDeltaDfLatest, "skill_association_code", "code2")
          assert[String](skillUnLinkedDeltaDfLatest, "skill_association_skill_id", "skill1")
          assert[String](skillUnLinkedDeltaDfLatest, "skill_association_skill_code", "code1")
          assert[Boolean](skillUnLinkedDeltaDfLatest, "skill_association_is_previous", false)
        }
      )
    }
  }

  test("Skill Linked/Unlinked with Category and Skill V2") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetSkillCategoryLinkToggleSource,
          value = """
                    |[{
                    |  "eventType": "SkillCategoryLinkedEvent",
                    |  "skillId": "skill1",
                    |  "skillCode": "code1",
                    |  "categoryId": "cat1",
                    |  "occurredOn": "2019-01-08 02:40:00.0"
                    |},
                    |{
                    |  "eventType": "SkillCategoryLinkedEvent",
                    |  "skillId": "skill2",
                    |  "skillCode": "code3",
                    |  "categoryId": "cat2",
                    |  "occurredOn": "2019-01-08 03:40:00.0"
                    |},
                    |{
                    |  "eventType": "SkillCategoryUnLinkedEvent",
                    |  "skillId": "skill1",
                    |  "skillCode": "code1",
                    |  "categoryId": "cat1",
                    |  "occurredOn": "2019-01-08 04:40:00.0"
                    |},
                    |{
                    |  "eventType": "SkillCategoryLinkedEvent",
                    |  "skillId": "skill1",
                    |  "skillCode": "code1",
                    |  "categoryId": "cat1",
                    |  "occurredOn": "2019-01-08 05:40:00.0"
                    |},
                    |{
                    |  "eventType": "SkillCategoryUnLinkedEvent",
                    |  "skillId": "skill2",
                    |  "skillCode": "code3",
                    |  "categoryId": "cat2",
                    |  "occurredOn": "2019-01-08 03:42:00.0"
                    |}]
                  """.stripMargin
        ),
        SparkFixture(
          key = ParquetSkillLinkToggleV2Source,
          value = """
                    |[{
                    |  "eventType": "SkillLinkedEventV2",
                    |  "skillId": "skill1",
                    |  "skillCode": "code1",
                    |  "previousSkillId": "skillPrev",
                    |  "previousSkillCode": "code2",
                    |  "occurredOn": "2019-01-08 02:40:00.0"
                    |},
                    |{
                    |  "eventType": "SkillLinkedEventV2",
                    |  "skillId": "skill2",
                    |  "skillCode": "code3",
                    |  "previousSkillId": "skillPrev",
                    |  "previousSkillCode": "code2",
                    |  "occurredOn": "2019-01-08 03:40:00.0"
                    |},
                    |{
                    |  "eventType": "SkillUnLinkedEventV2",
                    |  "skillId": "skill1",
                    |  "skillCode": "code1",
                    |  "previousSkillId": "skillPrev",
                    |  "previousSkillCode": "code2",
                    |  "occurredOn": "2019-01-08 04:40:00.0"
                    |}
                    |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val redshiftSink = sinks.filter(_.name == RedshiftSkillAssociationSink)
          val skillAssociationDf = redshiftSink.head.output.cache()
          assert(skillAssociationDf.count == 8)
          assert(skillAssociationDf.columns.toSet === SkillAssociationExpectedCols)
          val skillCategoryLinkedDfLatest =
            skillAssociationDf.filter(
              col("skill_association_attach_status") === 1 && col("skill_association_status") === 1
                && col("skill_association_type") === 2)
          val skillCategoryUnLinkedDfLatest =
            skillAssociationDf.filter(
              col("skill_association_attach_status") === 0 && col("skill_association_status") === 1
                && col("skill_association_type") === 2)
          assert[String](skillCategoryLinkedDfLatest, "skill_association_id", "cat1")
          assert[String](skillCategoryLinkedDfLatest, "skill_association_code", null)
          assert[String](skillCategoryLinkedDfLatest, "skill_association_skill_id", "skill1")
          assert[String](skillCategoryLinkedDfLatest, "skill_association_skill_code", "code1")
          assert[Boolean](skillCategoryLinkedDfLatest, "skill_association_is_previous", false)

          assert[String](skillCategoryUnLinkedDfLatest, "skill_association_id", "cat2")
          assert[String](skillCategoryUnLinkedDfLatest, "skill_association_code", null)
          assert[String](skillCategoryUnLinkedDfLatest, "skill_association_skill_id", "skill2")
          assert[String](skillCategoryUnLinkedDfLatest, "skill_association_skill_code", "code3")
          assert[Boolean](skillCategoryUnLinkedDfLatest, "skill_association_is_previous", false)

          val skillLinkedDfLatest =
            skillAssociationDf.filter(
              col("skill_association_attach_status") === 1 && col("skill_association_status") === 1
                && col("skill_association_type") === 1)
          val skillUnLinkedDfLatest =
            skillAssociationDf.filter(
              col("skill_association_attach_status") === 0 && col("skill_association_status") === 1
                && col("skill_association_type") === 1)
          assert[String](skillLinkedDfLatest, "skill_association_id", "skillPrev")
          assert[String](skillLinkedDfLatest, "skill_association_code", "code2")
          assert[String](skillLinkedDfLatest, "skill_association_skill_id", "skill2")
          assert[String](skillLinkedDfLatest, "skill_association_skill_code", "code3")
          assert[Boolean](skillLinkedDfLatest, "skill_association_is_previous", true)

          assert[String](skillUnLinkedDfLatest, "skill_association_id", "skillPrev")
          assert[String](skillUnLinkedDfLatest, "skill_association_code", "code2")
          assert[String](skillUnLinkedDfLatest, "skill_association_skill_id", "skill1")
          assert[String](skillUnLinkedDfLatest, "skill_association_skill_code", "code1")
          assert[Boolean](skillUnLinkedDfLatest, "skill_association_is_previous", true)

          val deltaSink = sinks.filter(_.name == DeltaSkillAssociationSink)
          val deltaAssociationDf = deltaSink.head.output.cache()
          assert(deltaAssociationDf.count == 8)
          assert(deltaAssociationDf.columns.toSet === SkillAssociationExpectedCols)
          val skillCategoryLinkedDeltaDfLatest =
            deltaAssociationDf.filter(
              col("skill_association_attach_status") === 1 && col("skill_association_status") === 1
                && col("skill_association_type") === 2)
          val skillCategoryUnLinkedDeltaDfLatest =
            deltaAssociationDf.filter(
              col("skill_association_attach_status") === 0 && col("skill_association_status") === 1
                && col("skill_association_type") === 2)
          assert[String](skillCategoryLinkedDeltaDfLatest, "skill_association_id", "cat1")
          assert[String](skillCategoryLinkedDeltaDfLatest, "skill_association_code", null)
          assert[String](skillCategoryLinkedDeltaDfLatest, "skill_association_skill_id", "skill1")
          assert[String](skillCategoryLinkedDeltaDfLatest, "skill_association_skill_code", "code1")
          assert[Boolean](skillCategoryLinkedDeltaDfLatest, "skill_association_is_previous", false)

          assert[String](skillCategoryUnLinkedDeltaDfLatest, "skill_association_id", "cat2")
          assert[String](skillCategoryUnLinkedDeltaDfLatest, "skill_association_code", null)
          assert[String](skillCategoryUnLinkedDeltaDfLatest, "skill_association_skill_id", "skill2")
          assert[String](skillCategoryUnLinkedDeltaDfLatest, "skill_association_skill_code", "code3")
          assert[Boolean](skillCategoryUnLinkedDeltaDfLatest, "skill_association_is_previous", false)

          val skillLinkedDfDeltaLatest =
            deltaAssociationDf.filter(
              col("skill_association_attach_status") === 1 && col("skill_association_status") === 1
                && col("skill_association_type") === 1)
          val skillUnLinkedDeltaDfLatest =
            deltaAssociationDf.filter(
              col("skill_association_attach_status") === 0 && col("skill_association_status") === 1
                && col("skill_association_type") === 1)
          assert[String](skillLinkedDfDeltaLatest, "skill_association_id", "skillPrev")
          assert[String](skillLinkedDfDeltaLatest, "skill_association_code", "code2")
          assert[String](skillLinkedDfDeltaLatest, "skill_association_skill_id", "skill2")
          assert[String](skillLinkedDfDeltaLatest, "skill_association_skill_code", "code3")
          assert[Boolean](skillLinkedDfDeltaLatest, "skill_association_is_previous", true)

          assert[String](skillUnLinkedDeltaDfLatest, "skill_association_id", "skillPrev")
          assert[String](skillUnLinkedDeltaDfLatest, "skill_association_code", "code2")
          assert[String](skillUnLinkedDeltaDfLatest, "skill_association_skill_id", "skill1")
          assert[String](skillUnLinkedDeltaDfLatest, "skill_association_skill_code", "code1")
          assert[Boolean](skillUnLinkedDeltaDfLatest, "skill_association_is_previous", true)

        }
      )
    }
  }

}
