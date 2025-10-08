package com.alefeducation.dimensions

import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.util.Helpers._
import com.alefeducation.util.Constants._

class CCLSkillDimensionSpec extends SparkSuite with BaseDimensionSpec {

  test("Test empty DataFrame case") {
    implicit val transformer: CCLSkillDimension = CCLSkillDimension(spark)

    val fixtures = List(
      SparkFixture(key = ParquetCCLSkillMutatedSource, value = ""),
      SparkFixture(key = ParquetCCLSkillDeletedSource, value = "")
    )

    withSparkFixtures(fixtures, sinks => assert(sinks.isEmpty))
  }

  test("Test CCL Skill Created/Updated/Deleted Events ") {
    implicit val transformer: CCLSkillDimension = CCLSkillDimension(spark)
    val fixtures = List(
      SparkFixture(
        key = ParquetCCLSkillMutatedSource,
        value =
          """
            |[
            |{"eventType":"SkillCreatedEvent","loadtime":"2020-07-08T05:18:02.053Z","id":"skill1","code":"code1","name":"test1","description":"desc1","subjectId":571671,"occurredOn":"2020-07-08 05:18:02.030","translations": [{"uuid":"6fb869d4-6373-4310-b58e-d9324fe01db5","languageCode":"AR","value":"سمي هو خان","name": "سمي هو خان", "description": "translated description"}],"eventDateDw":"20200708"},
            |{"eventType":"SkillCreatedEvent","loadtime":"2020-07-08T05:18:15.311Z","id":"skill2","code":"code2","name":"test2","description":"desc2","subjectId":571671,"occurredOn":"2020-07-08 05:18:15.302","translations": [{"uuid":"6fb869d4-6373-4310-b58e-d9324fe01db5","languageCode":"AR","value":"سمي هو خان","name": "سمي هو خان", "description": "translated description"}],"eventDateDw":"20200708"},
            |{"eventType":"SkillCreatedEvent","loadtime":"2020-07-08T05:18:49.529Z","id":"skill3","code":"code3","name":"test3","description":"desc3","subjectId":571671,"occurredOn":"2020-07-08 05:18:49.516","translations": [{"uuid":"6fb869d4-6373-4310-b58e-d9324fe01db5","languageCode":"AR","value":"سمي هو خان","name": "سمي هو خان", "description": "translated description"}],"eventDateDw":"20200708"},
            |{"eventType":"SkillCreatedEvent","loadtime":"2020-07-08T05:27:42.448Z","id":"skill4","code":"code4","name":"test4","description":"desc4","subjectId":571671,"occurredOn":"2020-07-08 05:27:42.442","translations": [{"uuid":"6fb869d4-6373-4310-b58e-d9324fe01db5","languageCode":"AR","value":"سمي هو خان","name": "سمي هو خان", "description": "translated description"}],"eventDateDw":"20200708"},
            |{"eventType":"SkillUpdatedEvent","loadtime":"2020-07-09T05:18:02.053Z","subjectId":571671,"id":"skill1","code":"codeNew","name":"test1","description":"desc1","occurredOn":"2020-07-09 05:18:02.030","translations": [{"uuid":"6fb869d4-6373-4310-b58e-d9324fe01db5","languageCode":"AR","value":"سمي هو خان","name": "سمي هو خان", "description": "translated description"}],"eventDateDw":"20200709"},
            |{"eventType":"SkillUpdatedEvent","loadtime":"2020-07-09T05:18:15.311Z","subjectId":571671,"id":"skill1","code":"codeNewTest","name":"test1new","description":"desc1 Brand New","occurredOn":"2020-07-09 05:18:15.302","translations": [{"uuid":"6fb869d4-6373-4310-b58e-d9324fe01db5","languageCode":"AR","value":"سمي هو خان","name": "سمي هو خان", "description": "translated description"}],"eventDateDw":"20200709"},
            |{"eventType":"SkillUpdatedEvent","loadtime":"2020-07-10T05:18:49.529Z","subjectId":521671,"id":"skill4","code":"code41","name":"test3","description":"desc3","occurredOn":"2020-07-10 05:18:49.516","translations": [{"uuid":"6fb869d4-6373-4310-b58e-d9324fe01db5","languageCode":"AR","value":"سمي هو خان","name": "سمي هو خان", "description": "translated description"}],"eventDateDw":"20200710"},
            |{"eventType":"SkillUpdatedEvent","loadtime":"2020-07-10T05:27:42.448Z","subjectId":521671,"id":"skill4","code":"code4new","name":"test4new","description":"desc4 Brand New","occurredOn":"2020-07-10 05:27:42.442","translations": [{"uuid":"6fb869d4-6373-4310-b58e-d9324fe01db5","languageCode":"AR","value":"سمي هو خان","name": "سمي هو خان", "description": "translated description"}],"eventDateDw":"20200710"}
            |]
            |""".stripMargin
      ),
      SparkFixture(
        key = ParquetCCLSkillDeletedSource,
        value =
          """
            |[
            |{"eventType":"SkillDeletedEvent","loadtime":"2020-07-10T11:19:45.401Z","id":"skill2","occurredOn":"2020-07-10 11:19:45.397","eventDateDw":"20200710"}
            |]
            |""".stripMargin
      )
    )

    withSparkFixtures(
      fixtures, { sinks =>
        val expectedColsMutatedPqt: Seq[String] = CCLSkillColumns.keys.toSeq
          .diff(Seq("ccl_skill_status")) ++ commonParquetColumns
        val expectedColsDeletedPqt: Seq[String] = CCLSkillDeletedColumns.keys.toSeq
          .diff(Seq("ccl_skill_status")) ++ commonParquetColumns

        val expectedColsCreatedRst: Seq[String] = CCLSkillColumns.values.toSeq.diff(Seq("occurredOn", "ccl_skill_translations")) ++ commonRedshiftColumns(
          CCLSkillEntity)
        val expectedColsUpdatedRst: Seq[String] = CCLSkillColumns.values.toSeq.diff(Seq("occurredOn", "ccl_skill_translations")) ++ commonRedshiftColumns(
          CCLSkillEntity)
        val expectedColsDeletedRst: Seq[String] = CCLSkillDeletedColumns.values.toSeq.diff(Seq("occurredOn", "ccl_skill_translations")) ++ commonRedshiftColumns(
          CCLSkillEntity)

        testSinkBySinkName(sinks, ParquetCCLSkillMutatedSource, expectedColsMutatedPqt, 8)
        testSinkBySinkName(sinks, ParquetCCLSkillDeletedSource, expectedColsDeletedPqt, 1)

        testSinkByEventType(sinks, RedshiftCCLSkillDimSink, SkillCreated, expectedColsCreatedRst, 4)
        testSinkByEventType(sinks, RedshiftCCLSkillDimSink, SkillUpdatedEvent, expectedColsUpdatedRst, 2)
        testSinkByEventType(sinks, RedshiftCCLSkillDimSink, SkillDeleted, expectedColsDeletedRst, 1)

        testSinkByEventType(sinks, DeltaCCLSkillSink, SkillCreated, expectedColsCreatedRst ++ Seq("ccl_skill_translations"), 4)
        testSinkByEventType(sinks, DeltaCCLSkillSink, SkillUpdatedEvent, expectedColsUpdatedRst ++ Seq("ccl_skill_translations"), 2)
        testSinkByEventType(sinks, DeltaCCLSkillSink, SkillDeleted, expectedColsDeletedRst, 1)

      }
    )
  }

}
