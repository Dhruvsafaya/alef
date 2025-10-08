package com.alefeducation.facts

import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.util.Helpers._

class NoSkillContentSpec extends SparkSuite {

  trait Setup {
    implicit val transformer = new NoSkillContent("no-skill-content", spark)
  }

  test("transform no MLO found for skills events successfully") {

    val expectedColumns = Set(
      "scu_created_time",
      "scu_dw_created_time",
      "scu_date_dw_id",
      "lo_uuid",
      "skill_uuid",
      "tenant_uuid"
    )
    val expectedDeltaColumns = Set(
      "scu_created_time",
      "scu_dw_created_time",
      "scu_date_dw_id",
      "scu_lo_id",
      "scu_skill_id",
      "scu_tenant_id",
      "eventdate"
    )

    new Setup {
      val fixtures = List(
        SparkFixture(
          key = NoSkillContentSource,
          value = """
                    |{
                    |  "tenantId": "tenant-id",
                    |  "eventType": "NoMloForSkillsFound",
                    |  "occurredOn": "2018-09-08 02:38:00.0",
                    |  "learningObjectiveId": "learning-objective-id",
                    |  "recommendedSkillsWithNoMlo": [{
                    |               "uuid": "skill-uuid-1",
                    |               "code": "skill-code-1",
                    |               "name": "skill-name-1"
                    |             },
                    |             {
                    |               "uuid": "skill-uuid-2",
                    |               "code": "skill-code-2",
                    |               "name": "skill-name-2"
                    |             }],
                    |  "eventDateDw": "20181029"
                    |}
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>

          assert(sinks.size == 3)
          val df = sinks.filter(_.name == RedshiftNoSkillContentSink).head.output

          assert(df.count === 2)
          assert(df.columns.toSet === expectedColumns)

          assert[String](df, "skill_uuid", "skill-uuid-1")
          assert[String](df, "lo_uuid", "learning-objective-id")
          assert[String](df, "scu_date_dw_id", "20181029")
          assert[String](df, "scu_created_time", "2018-09-08 02:38:00.0")

          val deltaDf = sinks.filter(_.name == "delta-no-skill-content-sink").head.output

          assert(deltaDf.count === 2)
          assert(deltaDf.columns.toSet === expectedDeltaColumns)

          assert[String](deltaDf, "scu_skill_id", "skill-uuid-1")
          assert[String](deltaDf, "scu_lo_id", "learning-objective-id")
          assert[String](deltaDf, "scu_date_dw_id", "20181029")
          assert[String](deltaDf, "scu_created_time", "2018-09-08 02:38:00.0")
        }
      )
    }
  }

}
