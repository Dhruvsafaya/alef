package com.alefeducation.dimensions.interim_checkpoint

import com.alefeducation.bigdata.Sink
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.dimensions.interim_checkpoint.InterimCheckpointDimension._
import com.alefeducation.service.DataSink
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class InterimCheckpointDimensionSpec extends SparkSuite with Matchers {

  private val IcEntity = "ic"
  private val IcUuid = "interimCheckpointId-1"

  private val IcExpectedDateFields = Set(
    "_created_time",
    "_updated_time",
    "_deleted_time",
    "_dw_created_time",
    "_dw_updated_time",
    "_status"
  ).map(IcEntity + _)

  private val expectedIcFields = IcExpectedDateFields ++ Set(
    "ic_id",
    "ic_language",
    "ic_title",
    "ic_material_type"
  )

  private val IcLessonEntity = "ic_lesson"

  private val IcLessonExpectedDateFields = Set(
    "_created_time",
    "_updated_time",
    "_dw_created_time",
    "_dw_updated_time",
    "_status"
  ).map(IcLessonEntity + _)

  private val expectedIcLessonFields = IcLessonExpectedDateFields ++ Set(
    "ic_lesson_attach_status",
    "ic_lesson_type",
    "ic_uuid",
    "lo_uuid"
  )

  private val IcRuleEntity = "ic_rule"

  private val IcRuleExpectedDateFields = Set(
    "_created_time",
    "_updated_time",
    "_dw_created_time",
    "_dw_updated_time",
    "_status"
  ).map(IcRuleEntity + _)

  private val expectedIcRuleFields = IcRuleExpectedDateFields ++ Set(
    "ic_rule_attach_status",
    "ic_rule_type",
    "ic_uuid",
    "outcome_uuid",
    "ic_rule_resource_type",
    "ic_rule_no_questions"
  )

  private val defaultSinkCount = 1;

  trait Setup {
    implicit val transformer = InterimCheckpointDimension(spark)
  }

  def icRulesMsgTemplate(ipic: String, eventType: String, append: String = "", occurredOn: String = "1634210749601"): String = {
    s"""{
      |  "eventType": "$eventType",
      |  "createdOn": 1634210749601,
      |   $ipic,
      |  "learningOutcomes": [
      |    {
      |      "id": 746548,
      |      "type": "sub_standard"
      |    }
      |  ],
      |  "lessonToLearningOutcomesMap": [
      |    {
      |      "lessonId": "lesson-id-1",
      |      "outcomes": [
      |        {
      |          "id": 746548,
      |          "type": "sub_standard"
      |        }
      |      ]
      |    }
      |  ],
      |  "lessonsWithPools": [
      |    {
      |      "id": "lesson-id-1",
      |      "poolIds": [
      |        "5d53e3e89668090001c3aaf3"
      |      ]
      |    }
      |  ],
      |  "occurredOn": $occurredOn,
      |  "rules": [
      |    {
      |      "learningOutcome": {
      |        "id": 746548,
      |        "type": "sub_standard"
      |      },
      |      "numQuestions": 1,
      |      "poolIds": [
      |        "5d53e3e89668090001c3aaf3"
      |      ],
      |      "resourceType": "TEQ2"
      |    }
      |  ],
      |  "title": "ic-title",
      |  "updatedOn": 1634210749601,
      |  "tenantId": "",
      |  "language": "AR",
      |  "eventdate": "2021-10-20" $append
      |}
      """.stripMargin
  }

  def icRulesCreatedMsgTemplate(ipic: String, append: String = "", occurredOn: String = "1634210749601"): String =
    icRulesMsgTemplate(ipic, "InterimCheckpointRulesCreatedEvent", append, occurredOn)

  val ipIcUuids = s"""|
                      |"instructionalPlanId": "instructionalPlanId-1",
                      |"interimCheckpointId": "$IcUuid"
                      |""".stripMargin

  val icRulesCreatedMsg = icRulesCreatedMsgTemplate(ipIcUuids)

  val icRulesUpdatedMsg = icRulesMsgTemplate(ipIcUuids, "InterimCheckpointRulesUpdatedEvent")

  val icRulesCreatedMsgWithPathway: String = icRulesCreatedMsgTemplate(ipIcUuids,
     """|,
        |  "materialId": "bf714258-1f6a-4890-b1ec-46c88f71a9e2",
        |  "materialType": "PATHWAY"
        |""".stripMargin)

  val icRulesCreatedMsgWithIpAndPathway: String = icRulesCreatedMsgTemplate(
    s"""|   "instructionalPlanId": "instructionalPlanId-2",
        |   "interimCheckpointId": "$IcUuid"
        |""".stripMargin,
     """|,
        |  "materialId": "bf714258-1f6a-4890-b1ec-46c88f71a9e2",
        |  "materialType": "PATHWAY"
        |""".stripMargin)

  val ic2RulesCreatedMsg = icRulesCreatedMsgTemplate(
    """|
       |"instructionalPlanId": "instructionalPlanId-1",
       |"interimCheckpointId": "interimCheckpointId-2"
       |""".stripMargin)

  val icRuleCreatedMsgs: String =
    s"""
      |[
      |$icRulesCreatedMsgWithIpAndPathway,$ic2RulesCreatedMsg
      |]
      |""".stripMargin

  test("should process Interim Checkpoint Rules Created Event") {
      val fixtures = List(
          SparkFixture(
            key = ParquetInterimCheckpointSource,
            value = icRulesCreatedMsgWithPathway
          )
        )
    executeTest(fixtures, ParquetInterimCheckpointSink)
  }

  test("should process Interim Checkpoint Rules Updated Event") {
    val fixtures = List(
      SparkFixture(
        key = ParquetInterimCheckpointUpdatedSource,
        value = icRulesUpdatedMsg
      )
    )
    executeTest(fixtures, ParquetInterimCheckpointUpdatedSink)
  }

  test("should process Interim Checkpoint Rules Created Event without materialType") {
    val fixtures = List(
      SparkFixture(
        key = ParquetInterimCheckpointSource,
        value = icRulesCreatedMsg
      )
    )

    executeTest(fixtures, ParquetInterimCheckpointSink, { sinks =>
      val redshiftIcSink = sinks.filter(_.name == RedshiftInterimCheckpointSink).head
      val icDF = redshiftIcSink.output.cache
      assert[String](icDF, "ic_material_type", InstructionPlan)
    })
  }

  test("should process Published Interim Checkpoint Deleted Event") {
    val fixtures = List(
      SparkFixture(
        key = ParquetInterimCheckpointSource,
        value = icRulesCreatedMsgWithPathway
      ),
      SparkFixture(
        key = ParquetPublishedCheckpointDeletedSource,
        value = s"""
            |{
            |  "eventType": "PublishedCheckpointDeletedEvent",
            |  "createdOn": 1634210749601,
            |  "id": "$IcUuid",
            |  "occurredOn": "1634210749601",
            |  "updatedOn": 1634210749601,
            |  "tenantId": "",
            |  "eventdate": "2021-10-20"
            |}
    """.stripMargin
      )
    )
    executeTest(fixtures, ParquetInterimCheckpointSink, {
      sinks =>
        val s3DeletedSink: Sink = sinks.filter(_.name == ParquetPublishedCheckpointDeletedSink).head
        assert(s3DeletedSink.asInstanceOf[DataSink].options.get("partitionBy") === Some("eventdate"))
        assert[String](s3DeletedSink.output, "eventdate", "2021-10-14")
    })
  }

  test("should process Interim Checkpoint Deleted Event") {
    val fixtures = List(
      SparkFixture(
        key = ParquetInterimCheckpointSource,
        value = icRulesCreatedMsgWithPathway
      ),
      SparkFixture(
        key = ParquetInterimCheckpointDeletedSource,
        value = s"""
                  |{
                  |   "eventType":"InterimCheckpointDeletedEvent",
                  |   "id":"$IcUuid",
                  |   "occurredOn":1665554569649,
                  |   "partitionKey":"shared",
                  |   "userTenant":"shared"
                  |}
    """.stripMargin
      )
    )
    executeTest(fixtures, ParquetInterimCheckpointSink, {
      sinks =>
        val s3DeletedSink: Sink = sinks.filter(_.name == ParquetInterimCheckpointDeletedSink).head
        assert(s3DeletedSink.asInstanceOf[DataSink].options.get("partitionBy") === Some("eventdate"))
        val s3df = s3DeletedSink.output
        assert[String](s3df, "eventdate", "2022-10-12")
        assert[String](s3df, "id", IcUuid)
        assert[String](s3df, "eventType", "InterimCheckpointDeletedEvent")
        val rsDeletedSink: Sink = sinks.filter(_.name == RedshiftInterimCheckpointDeletedSink).head
        val df = rsDeletedSink.output
        assert[String](df, "ic_id", IcUuid)
        assert[Int](df, "ic_status", 4)
        assert[String](df, "ic_deleted_time", "2022-10-12 06:02:49.649")
    })
  }

  test("should process Published Interim Checkpoint Deleted and Interim Checkpoint Deleted Event") {
    val fixtures = List(
      SparkFixture(
        key = ParquetInterimCheckpointSource,
        value = icRulesCreatedMsgWithPathway
      ),
      SparkFixture(
        key = ParquetPublishedCheckpointDeletedSource,
        value = s"""
                  |{
                  |  "eventType": "PublishedCheckpointDeletedEvent",
                  |  "createdOn": 1634210749601,
                  |  "id": "$IcUuid",
                  |  "occurredOn": "1634210749601",
                  |  "updatedOn": 1634210749601,
                  |  "tenantId": "",
                  |  "eventdate": "2021-10-20"
                  |}
    """.stripMargin
      ),
      SparkFixture(
        key = ParquetInterimCheckpointDeletedSource,
        value = """
                  |{
                  |   "eventType":"InterimCheckpointDeletedEvent",
                  |   "id":"interimCheckpointId-2",
                  |   "occurredOn":1665554569649,
                  |   "partitionKey":"shared",
                  |   "userTenant":"shared"
                  |}
    """.stripMargin
      )
    )
    executeTest(fixtures, ParquetInterimCheckpointSink, {
      sinks =>
        val cnt = sinks.filter(_.name == RedshiftInterimCheckpointDeletedSink).head.output.count()
        assert(cnt == 2)
    })
  }

  test("should process Interim Checkpoint Rules Created Event when row does not have materialType and another have") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetInterimCheckpointSource,
          value = icRuleCreatedMsgs
        )
      )
      executeTest(fixtures, ParquetInterimCheckpointSink, { sinks =>
          val redshiftIcSink = sinks.filter(_.name == RedshiftInterimCheckpointSink).head
          val icDF = redshiftIcSink.output.cache
          assert[String](icDF, "ic_material_type", "PATHWAY")
          icDF.select("ic_material_type").collect().map(_(0)).toSeq.tail.mkString("") shouldBe "INSTRUCTIONAL_PLAN"
      })
    }
  }

  test("should process Interim Checkpoint for Rules and Lesson Associations only one from two duplicate events") {
    val icRulesCreatedMsg1 = icRulesCreatedMsgTemplate(ipIcUuids, append =
      """,
        |"title1": "title_one"
        |""".stripMargin)
    val icRulesCreatedMsg2 = icRulesCreatedMsgTemplate(ipIcUuids, append =
      """,
        |"title1": "title_another"
        |""".stripMargin)
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetInterimCheckpointSource,
          value = s"""
                     |[
                     |$icRulesCreatedMsg1,$icRulesCreatedMsg2
                     |]
                     |""".stripMargin
        )
      )
      executeTest(fixtures, ParquetInterimCheckpointSink, { sinks =>
        val redshiftIcRuleSink = sinks.filter(_.name == RedshiftInterimCheckpointRulesSink).head
        assert(redshiftIcRuleSink.output.count() == 1)

        val redshiftIcLessonSink = sinks.filter(_.name == RedshiftInterimCheckpointLessonSink).head
        assert(redshiftIcLessonSink.output.count() == 1)
      })
    }
  }

  def executeTest(fixtures: List[SparkFixture], parquetSinkName: String, f: List[Sink] => Unit = sinks => ()): Unit = {
    new Setup {
      withSparkFixtures(
        fixtures, { sinks =>
          val s3Sink: Sink = sinks.filter(_.name == parquetSinkName).head
          assert(s3Sink.asInstanceOf[DataSink].options.get("partitionBy") === Some("eventdate"))
          assert[String](s3Sink.output, "eventdate", "2021-10-14")

          val redshiftIcSink = sinks.filter(_.name == RedshiftInterimCheckpointSink).head
          val icDF = redshiftIcSink.output.cache
          assert(icDF.columns.toSet === expectedIcFields)
          assert[String](icDF, "ic_id", IcUuid)
          assert[String](icDF, "ic_language", "AR")
          assert[String](icDF, "ic_title", "ic-title")
          assert[Int](icDF, "ic_status", 1)

          val deltaIcSinkDF = sinks.find(_.name === DeltaInterimCheckpointSink).get.output.cache()
          assert(deltaIcSinkDF.columns.toSet === expectedIcFields)


          val redshiftIcLessonSink = sinks.filter(_.name == RedshiftInterimCheckpointLessonSink).head
          val redshiftIcLessonDf = redshiftIcLessonSink.output.cache
          assert(redshiftIcLessonDf.columns.toSet === expectedIcLessonFields)
          assert[String](redshiftIcLessonDf, "ic_uuid", IcUuid)
          assert[String](redshiftIcLessonDf, "lo_uuid", "lesson-id-1")
          assert[Int](redshiftIcLessonDf, "ic_lesson_attach_status", 1)
          assert[Int](redshiftIcLessonDf, "ic_lesson_status", 1)
          assert[Int](redshiftIcLessonDf, "ic_lesson_attach_status", 1)
          assert[Int](redshiftIcLessonDf, "ic_lesson_type", 1)

          val deltaIcLessonSinkDF = sinks.find(_.name === DeltaInterimCheckpointLessonSink).get.output.cache()
          assert(deltaIcLessonSinkDF.columns.toSet === expectedIcLessonFields.diff(expectedIcLessonFields.filter(_.endsWith("uuid"))) ++
            expectedIcLessonFields.filter(_.endsWith("uuid")).map(f => "ic_lesson_" + f.replace("_uuid", "_id")))

          val redshiftIcRulesSink = sinks.filter(_.name == RedshiftInterimCheckpointRulesSink).head
          val redshhiftIcRuleDf = redshiftIcRulesSink.output.cache
          assert(redshhiftIcRuleDf.columns.toSet === expectedIcRuleFields)
          assert[String](redshhiftIcRuleDf, "ic_uuid", IcUuid)
          assert[String](redshhiftIcRuleDf, "outcome_uuid", "substandard-746548")
          assert[String](redshhiftIcRuleDf, "ic_rule_resource_type", "TEQ2")
          assert[Int](redshhiftIcRuleDf, "ic_rule_type", 1)
          assert[Int](redshhiftIcRuleDf, "ic_rule_attach_status", 1)
          assert[Int](redshhiftIcRuleDf, "ic_rule_status", 1)

          val deltaIcRuleSinkDF = sinks.find(_.name === DeltaInterimCheckpointRulesSink).get.output.cache()
          assert(deltaIcRuleSinkDF.columns.toSet === expectedIcRuleFields.diff(expectedIcRuleFields.filter(_.endsWith("uuid"))) ++
            expectedIcRuleFields.filter(_.endsWith("uuid")).map(f => "ic_rule_" + f.replace("_uuid", "_id")))

          f(sinks)
        }
      )
    }
  }
}
