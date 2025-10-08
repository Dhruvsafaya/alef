package com.alefeducation.dimensions

import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.util.Helpers._

class ContentAssociationDimensionSpec extends SparkSuite with BaseDimensionSpec {
  test("Test empty DataFrame case") {
    implicit val transformer = new ContentAssociationDimension(ContentAssociationDimensionName, spark)

    val fixtures = List(
      SparkFixture(key = ParquetContentOutcomeSink, value = ""),
      SparkFixture(key = ParquetContentSkillSink, value = "")
    )

    withSparkFixtures(fixtures, sinks => assert(sinks.isEmpty))
  }

  test("Test Outcome/Skill Attached/Detached events") {
    implicit val transformer = new ContentAssociationDimension(ContentAssociationDimensionName, spark)

    val fixtures = List(
      SparkFixture(
        key = ParquetContentOutcomeSink,
        value =
          """
            |[
            |{"eventType":"ContentOutcomeAttachedEvent","loadtime":"2020-05-27T10:28:57.011Z","contentId":33336,"outcomeId":"substandard-63434","occurredOn":"2020-05-27 10:28:56.910","eventDateDw":"20200527"},
            |{"eventType":"ContentOutcomeAttachedEvent","loadtime":"2020-05-28T07:18:23.749Z","contentId":33337,"outcomeId":"substandard-658603","occurredOn":"2020-05-28 07:18:23.735","eventDateDw":"20200528"},
            |{"eventType":"ContentOutcomeAttachedEvent","loadtime":"2020-05-28T07:18:23.749Z","contentId":33337,"outcomeId":"substandard-363071","occurredOn":"2020-05-28 07:18:23.735","eventDateDw":"20200528"},
            |{"eventType":"ContentOutcomeDetachedEvent","loadtime":"2020-05-28T07:18:23.749Z","contentId":33337,"outcomeId":"substandard-363071","occurredOn":"2020-05-28 07:18:25.735","eventDateDw":"20200528"},
            |{"eventType":"ContentOutcomeDetachedEvent","loadtime":"2020-05-28T07:18:41.414Z","contentId":33337,"outcomeId":"substandard-658603","occurredOn":"2020-05-28 07:18:41.408","eventDateDw":"20200528"},
            |{"eventType":"ContentOutcomeAttachedEvent","loadtime":"2020-05-28T07:19:28.144Z","contentId":33337,"outcomeId":"sublevel-694975","occurredOn":"2020-05-28 07:19:28.138","eventDateDw":"20200528"},
            |{"eventType":"ContentOutcomeAttachedEvent","loadtime":"2020-05-28T07:19:28.144Z","contentId":33337,"outcomeId":"substandard-841474","occurredOn":"2020-05-28 07:19:28.138","eventDateDw":"20200528"},
            |{"eventType":"ContentOutcomeAttachedEvent","loadtime":"2020-05-28T07:19:28.145Z","contentId":33337,"outcomeId":"sublevel-826309","occurredOn":"2020-05-28 07:19:28.138","eventDateDw":"20200528"},
            |{"eventType":"ContentOutcomeAttachedEvent","loadtime":"2020-05-28T07:19:28.145Z","contentId":33337,"outcomeId":"substandard-900703","occurredOn":"2020-05-28 07:19:28.138","eventDateDw":"20200528"},
            |{"eventType":"ContentOutcomeDetachedEvent","loadtime":"2020-05-28T07:19:38.456Z","contentId":33337,"outcomeId":"sublevel-826309","occurredOn":"2020-05-28 07:19:38.454","eventDateDw":"20200528"}
            |]
            |""".stripMargin
      ),
      SparkFixture(
        key = ParquetContentSkillSink,
        value =
          """
          |[
          |{"eventType":"ContentSkillAttachedEvent","loadtime":"2020-05-27T10:28:57.011Z","contentId":33336,"skillId":"substandard-63434","occurredOn":"2020-05-27 10:28:56.910","eventDateDw":"20200527"},
          |{"eventType":"ContentSkillAttachedEvent","loadtime":"2020-05-28T07:18:23.749Z","contentId":33337,"skillId":"substandard-658603","occurredOn":"2020-05-28 07:18:23.735","eventDateDw":"20200528"},
          |{"eventType":"ContentSkillAttachedEvent","loadtime":"2020-05-28T07:18:23.749Z","contentId":33337,"skillId":"substandard-363071","occurredOn":"2020-05-28 07:18:23.735","eventDateDw":"20200528"},
          |{"eventType":"ContentSkillDetachedEvent","loadtime":"2020-05-28T07:18:23.749Z","contentId":33337,"skillId":"substandard-363071","occurredOn":"2020-05-28 07:18:25.735","eventDateDw":"20200528"},
          |{"eventType":"ContentSkillDetachedEvent","loadtime":"2020-05-28T07:18:41.414Z","contentId":33337,"skillId":"substandard-658603","occurredOn":"2020-05-28 07:18:41.408","eventDateDw":"20200528"},
          |{"eventType":"ContentSkillAttachedEvent","loadtime":"2020-05-28T07:19:28.144Z","contentId":33337,"skillId":"sublevel-694975","occurredOn":"2020-05-28 07:19:28.138","eventDateDw":"20200528"},
          |{"eventType":"ContentSkillAttachedEvent","loadtime":"2020-05-28T07:19:28.144Z","contentId":33337,"skillId":"substandard-841474","occurredOn":"2020-05-28 07:19:28.138","eventDateDw":"20200528"},
          |{"eventType":"ContentSkillAttachedEvent","loadtime":"2020-05-28T07:19:28.145Z","contentId":33337,"skillId":"sublevel-826309","occurredOn":"2020-05-28 07:19:28.138","eventDateDw":"20200528"},
          |{"eventType":"ContentSkillAttachedEvent","loadtime":"2020-05-28T07:19:28.145Z","contentId":33337,"skillId":"substandard-900703","occurredOn":"2020-05-28 07:19:28.138","eventDateDw":"20200528"},
          |{"eventType":"ContentSkillDetachedEvent","loadtime":"2020-05-28T07:19:38.456Z","contentId":33337,"skillId":"sublevel-826309","occurredOn":"2020-05-28 07:19:38.454","eventDateDw":"20200528"}
          |]
          |""".stripMargin
      )
    )

    withSparkFixtures(
      fixtures, { sinks =>
        val expectedPqCols = Seq(
          "contentId",
          "eventDateDw",
          "eventType",
          "loadtime",
          "occurredOn",
          "outcomeId",
          "eventdate"
        )
        val expectedRsCols = Seq(
          "content_association_created_time",
          "content_association_updated_time",
          "content_association_dw_created_time",
          "content_association_dw_updated_time",
          "content_association_status",
          "content_association_content_id",
          "content_association_id",
          "content_association_type",
          "content_association_attach_status"
        )

        //outcome
        testSinkBySinkName(sinks, ParquetContentOutcomeSink, expectedPqCols, 10)
        testSinkByEventType(sinks, RedshiftContentAssociationSink, "ContentOutcomeAttachDetachEvent", expectedRsCols, 10)
        testSinkByEventType(sinks, DeltaContentAssociationSink, "ContentOutcomeAttachDetachEvent", expectedRsCols, 10)

        //skill
        testSinkBySinkName(sinks, ParquetContentSkillSink, expectedPqCols.diff(Seq("outcomeId")) :+ "skillId", 10)
        testSinkByEventType(sinks, RedshiftContentAssociationSink, "ContentSkillAttachDetachEvent", expectedRsCols, 10)
        testSinkByEventType(sinks, DeltaContentAssociationSink, "ContentSkillAttachDetachEvent", expectedRsCols, 10)
      }
    )
  }

}
