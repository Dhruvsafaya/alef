package com.alefeducation.dimensions.team

import com.alefeducation.bigdata.Sink
import com.alefeducation.bigdata.batch.delta.Alias
import com.alefeducation.bigdata.batch.delta.operations.DeltaSCDSink
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.service.DataSink
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, TimestampType}
import org.scalatest.matchers.must.Matchers

class TeamStudentAssociationDimensionSpec extends SparkSuite with Matchers {

  import com.alefeducation.bigdata.commons.testutils.ExpectedFields._

  trait Setup {
    implicit val transformer: TeamStudentAssociationDimension = TeamStudentAssociationDimension(spark)
  }

  val DeltaAssociatedExpectedFields = List(
    ExpectedField(name = "team_student_association_team_id", dataType = StringType),
    ExpectedField(name = "team_student_association_student_id", dataType = StringType),
    ExpectedField(name = "team_student_association_status", dataType = IntegerType),
    ExpectedField(name = "team_student_association_active_until", dataType = TimestampType)
  ) ++ commonExpectedTimestampFields("team_student_association").filterNot(_.name == "team_student_association_deleted_time")

  val DeltaUnassociatedExpectedFields = List(
    ExpectedField(name = "team_student_association_team_id", dataType = StringType),
    ExpectedField(name = "team_student_association_active_until", dataType = TimestampType)
  ) ++ commonExpectedTimestampFields("team_student_association").filterNot(_.name == "team_student_association_deleted_time")

  val RedshiftExpectedFields = List(
    ExpectedField(name = "team_uuid", dataType = StringType),
    ExpectedField(name = "student_uuid", dataType = StringType),
    ExpectedField(name = "team_student_association_status", dataType = IntegerType),
    ExpectedField(name = "team_student_association_active_until", dataType = TimestampType)
  ) ++ commonExpectedTimestampFields("team_student_association").filterNot(_.name == "team_student_association_deleted_time")

  val ParquetExpectedFields = List(
    ExpectedField(name = "eventDateDw", dataType = StringType),
    ExpectedField(name = "eventType", dataType = StringType),
    ExpectedField(name = "loadtime", dataType = StringType),
    ExpectedField(name = "occurredOn", dataType = StringType),
    ExpectedField(name = "students", dataType = ArrayType(StringType)),
    ExpectedField(name = "teamId", dataType = StringType),
    ExpectedField(name = "eventdate", dataType = StringType)
  )

  test("TeamMembersUpdatedEvent :: students associated") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = "parquet-team-members-updated-source",
          value = """
                    |[
                    |  {
                    |    "eventType": "TeamMembersUpdatedEvent",
                    |    "loadtime": "2020-07-08T05:18:02.053Z",
                    |    "teamId": "2ad0d839-8ae9-41c9-b7ac-07ecc341055f",
                    |    "students": [
                    |      "ea60347f-ebd5-44c1-a24f-5ead3aa1681c",
                    |      "94230ddc-2198-4fa4-9cfc-bfe53d6208fa",
                    |      "ea60347f-ebd5-44c1-a24f-5ead3aa1682c"
                    |    ],
                    |    "occurredOn": "2020-07-08 05:18:02.030",
                    |    "eventDateDw": "20200708"
                    |  }
                    |]
                    |""".stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          sinks.collectFirst {
            case s: DeltaSCDSink if s.name == "delta-team-members-unassociated-sink" => s
          } mustBe empty

          val deltaAssociatedSCDSink =
            sinks.collectFirst { case s: DeltaSCDSink if s.name == "delta-team-members-associated-sink" => s }.get
          assertExpectedFields(deltaAssociatedSCDSink.output.schema.fields.toList, DeltaAssociatedExpectedFields)
          assertDWStatus(deltaAssociatedSCDSink)
          deltaAssociatedSCDSink.uniqueIdColumns mustEqual List("team_student_association_team_id", "team_student_association_student_id")
          deltaAssociatedSCDSink.matchConditions must include(
            s"${Alias.Delta}.team_student_association_team_id = ${Alias.Events}.team_student_association_team_id and ${Alias.Delta}.team_student_association_student_id = ${Alias.Events}.team_student_association_student_id")

          val redshiftSCDSink =
            sinks.collectFirst { case s: DataSink if s.name == "redshift-team-members-updated-sink" => s }.get
          assertExpectedFields(redshiftSCDSink.output.schema.fields.toList, RedshiftExpectedFields)
          assertDWStatus(redshiftSCDSink)
          assert(redshiftSCDSink.options("postactions") === postActionQuery)

          val parquetSink = sinks.find(_.name == "parquet-team-members-updated-sink").get
          assertExpectedFields(parquetSink.output.schema.fields.toList, ParquetExpectedFields)
        }
      )
    }
  }

  test("TeamMembersUpdatedEvent :: students unassociated") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = "parquet-team-members-updated-source",
          value = """
                    |[
                    |  {
                    |    "eventType": "TeamMembersUpdatedEvent",
                    |    "loadtime": "2020-07-08T05:18:02.053Z",
                    |    "teamId": "2ad0d839-8ae9-41c9-b7ac-07ecc341055f",
                    |    "students": [],
                    |    "occurredOn": "2020-07-08 05:18:02.030",
                    |    "eventDateDw": "20200708"
                    |  }
                    |]
                    |""".stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          sinks.collectFirst {
            case s: DeltaSCDSink if s.name == "delta-team-members-associated-sink" => s
          } mustBe empty

          val deltaUnassociatedSCDSink =
            sinks.collectFirst { case s: DeltaSCDSink if s.name == "delta-team-members-unassociated-sink" => s }.get
          assertExpectedFields(deltaUnassociatedSCDSink.output.schema.fields.toList, DeltaUnassociatedExpectedFields)
          deltaUnassociatedSCDSink.uniqueIdColumns mustEqual List("team_student_association_team_id")
          deltaUnassociatedSCDSink.matchConditions must include(
            s"${Alias.Delta}.team_student_association_team_id = ${Alias.Events}.team_student_association_team_id")
          deltaUnassociatedSCDSink.create mustBe false

          val redshiftSCDSink = sinks.find(_.name == "redshift-team-members-updated-sink").get
          assertExpectedFields(redshiftSCDSink.output.schema.fields.toList, RedshiftExpectedFields)
          assertDWStatus(redshiftSCDSink)

          val parquetSink = sinks.find(_.name == "parquet-team-members-updated-sink").get
          assertExpectedFields(parquetSink.output.schema.fields.toList, ParquetExpectedFields)
        }
      )
    }
  }

  private def assertDWStatus(sink: Sink): Unit = assert[Int](sink.output, "team_student_association_status", 1)

  val postActionQuery =
    """
      |       begin transaction;
      |       update rs_stage_schema.rel_team_student_association set team_student_association_status = 2
      |       from rs_stage_schema.rel_team_student_association target
      |       join rs_schema.staging_dim_team_student_association staging on staging.team_uuid = target.team_uuid
      |       where target.team_student_association_created_time < staging.team_student_association_created_time
      |       and target.team_student_association_status = 1 and target.team_student_association_active_until is null;
      |
      |       update rs_stage_schema.rel_team_student_association set team_student_association_active_until = staging.team_student_association_created_time
      |       from rs_stage_schema.rel_team_student_association target
      |       join rs_schema.staging_dim_team_student_association staging on staging.team_uuid = target.team_uuid
      |       where target.team_student_association_created_time < staging.team_student_association_created_time
      |       and target.team_student_association_active_until is null;
      |
      |       update rs_schema.dim_team_student_association set team_student_association_status = 2
      |       from rs_schema.dim_team_student_association target
      |       join rs_schema.dim_team team on team.team_dw_id = target.team_student_association_team_dw_id
      |       join rs_schema.staging_dim_team_student_association staging on team.team_id = staging.team_uuid
      |       where target.team_student_association_created_time < staging.team_student_association_created_time and
      |       target.team_student_association_status = 1 and target.team_student_association_active_until is null;
      |
      |       update rs_schema.dim_team_student_association set team_student_association_active_until = staging.team_student_association_created_time
      |       from rs_schema.dim_team_student_association target
      |       join rs_schema.dim_team team on team.team_dw_id = target.team_student_association_team_dw_id
      |       join rs_schema.staging_dim_team_student_association staging on team.team_id = staging.team_uuid
      |       where target.team_student_association_created_time < staging.team_student_association_created_time
      |       and target.team_student_association_active_until is null;
      |
      |       insert into rs_stage_schema.rel_team_student_association (student_uuid, team_student_association_status, team_uuid, team_student_association_created_time, team_student_association_dw_created_time, team_student_association_updated_time, team_student_association_dw_updated_time, team_student_association_active_until)
      |       (select student_uuid, team_student_association_status, team_uuid, team_student_association_created_time, team_student_association_dw_created_time, team_student_association_updated_time, team_student_association_dw_updated_time, team_student_association_active_until from rs_schema.staging_dim_team_student_association WHERE staging_dim_team_student_association.student_uuid IS NOT NULL);
      |       DROP table rs_schema.staging_dim_team_student_association;
      |       end transaction
     """.stripMargin

}
