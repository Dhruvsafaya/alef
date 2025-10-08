package com.alefeducation.dimensions.team

import com.alefeducation.bigdata.commons.testutils.SparkSuite
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.scalatest.matchers.must.Matchers

class TeamDimensionSpec extends SparkSuite with Matchers {

  import com.alefeducation.bigdata.commons.testutils.ExpectedFields._

  trait Setup {
    implicit val transformer: TeamDimension = TeamDimension(spark)
  }

  object Delta {
    val TeamMutatedExpectedFields = List(
      ExpectedField(name = "team_id", dataType = StringType),
      ExpectedField(name = "team_name", dataType = StringType),
      ExpectedField(name = "team_description", dataType = StringType),
      ExpectedField(name = "team_class_id", dataType = StringType),
      ExpectedField(name = "team_teacher_id", dataType = StringType),
      ExpectedField(name = "team_status", dataType = IntegerType)
    ) ++ commonExpectedTimestampFields("team")

    val TeamDeletedExpectedFields = List(
      ExpectedField(name = "team_id", dataType = StringType),
      ExpectedField(name = "team_status", dataType = IntegerType)
    ) ++ commonExpectedTimestampFields("team")
  }

  object Redshift {
    val TeamMutatedExpectedFields = Delta.TeamMutatedExpectedFields.filterNot(_.name == "team_description")
    val TeamDeletedExpectedFields = Delta.TeamDeletedExpectedFields
  }

  object Parquet {
    val TeamMutatedExpectedFields = List(
      ExpectedField(name = "classId", dataType = StringType),
      ExpectedField(name = "description", dataType = StringType),
      ExpectedField(name = "eventDateDw", dataType = StringType),
      ExpectedField(name = "eventType", dataType = StringType),
      ExpectedField(name = "loadtime", dataType = StringType),
      ExpectedField(name = "name", dataType = StringType),
      ExpectedField(name = "occurredOn", dataType = StringType),
      ExpectedField(name = "teacherId", dataType = StringType),
      ExpectedField(name = "teamId", dataType = StringType),
      ExpectedField(name = "eventdate", dataType = StringType)
    )

    val TeamDeletedExpectedFields = List(
      ExpectedField(name = "eventDateDw", dataType = StringType),
      ExpectedField(name = "eventType", dataType = StringType),
      ExpectedField(name = "loadtime", dataType = StringType),
      ExpectedField(name = "occurredOn", dataType = StringType),
      ExpectedField(name = "teamId", dataType = StringType),
      ExpectedField(name = "eventdate", dataType = StringType)
    )
  }

  test("TeamCreatedEvent") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = "parquet-team-mutated-source",
          value = """
              |[
              |  {
              |    "eventType": "TeamCreatedEvent",
              |    "loadtime": "2020-07-08T05:18:02.053Z",
              |    "teamId": "ea60347f-ebd5-44c1-a24f-3ead3aa1681c",
              |    "name": "Team Expo",
              |    "description": "Expo2021",
              |    "classId": "ea60347f-ebd5-44c1-a24f-3ead3aa1681c",
              |    "teacherId": "ea60347f-ebd5-44c1-a24f-3ead3aa1681c",
              |    "occurredOn": "2020-07-08 05:18:02.030",
              |    "eventDateDw": "20200708"
              |  }
              |]
              |""".stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val deltaCreatedSink = sinks.find { s =>
            s.name == "delta-team-sink" && s.eventType == "TeamCreatedEvent"
          }.get
          assertExpectedFields(deltaCreatedSink.output.schema.fields.toList, Delta.TeamMutatedExpectedFields)
          assert[Int](deltaCreatedSink.output, "team_status", 1)
          sinks.find { s =>
            s.name == "delta-team-sink" && s.eventType == "TeamUpdatedEvent"
          } mustBe empty

          val redshiftCreatedSink = sinks.find { s =>
            s.name == "redshift-team-sink" && s.eventType == "TeamCreatedEvent"
          }.get
          assertExpectedFields(redshiftCreatedSink.output.schema.fields.toList, Redshift.TeamMutatedExpectedFields)
          assert[Int](redshiftCreatedSink.output, "team_status", 1)
          val redshiftUpdatedSink = sinks.find { s =>
            s.name == "redshift-team-sink" && s.eventType == "TeamUpdatedEvent"
          }.get
          assert(redshiftUpdatedSink.output.isEmpty)

          val parquetMutatedSinks = sinks.filter(_.name == "parquet-team-mutated-sink")
          val parquetCreatedSink = parquetMutatedSinks.find(_.eventType == "TeamCreatedEvent").get
          assertExpectedFields(parquetCreatedSink.output.schema.fields.toList, Parquet.TeamMutatedExpectedFields)
          val parquetUpdatedSink = parquetMutatedSinks.find(_.eventType == "TeamUpdatedEvent").get
          assert(parquetUpdatedSink.output.isEmpty)
        }
      )
    }
  }

  test("TeamUpdatedEvent") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = "parquet-team-mutated-source",
          value = """
                    |[
                    |  {
                    |    "eventType": "TeamUpdatedEvent",
                    |    "loadtime": "2020-07-08T05:18:02.053Z",
                    |    "teamId": "ea60347f-ebd5-44c1-a24f-3ead3aa1681c",
                    |    "name": "Team Expo",
                    |    "description": "Expo2021",
                    |    "classId": "ea60347f-ebd5-44c1-a24f-3ead3aa1681c",
                    |    "teacherId": "ea60347f-ebd5-44c1-a24f-3ead3aa1681c",
                    |    "occurredOn": "2020-07-08 05:18:02.030",
                    |    "eventDateDw": "20200708"
                    |  }
                    |]
                    |""".stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          sinks.find { s =>
            s.name == "delta-team-sink" && s.eventType == "TeamCreatedEvent"
          } mustBe empty
          val deltaUpdatedSink = sinks.find { s =>
            s.name == "delta-team-sink" && s.eventType == "TeamUpdatedEvent"
          }.get
          assertExpectedFields(deltaUpdatedSink.output.schema.fields.toList, Delta.TeamMutatedExpectedFields)
          assert[Int](deltaUpdatedSink.output, "team_status", 1)

          val redshiftCreatedSink = sinks.find { s =>
            s.name == "redshift-team-sink" && s.eventType == "TeamCreatedEvent"
          }.get
          assert(redshiftCreatedSink.output.isEmpty)
          val redshiftUpdatedSink = sinks.find { s =>
            s.name == "redshift-team-sink" && s.eventType == "TeamUpdatedEvent"
          }.get
          assertExpectedFields(redshiftUpdatedSink.output.schema.fields.toList, Redshift.TeamMutatedExpectedFields)
          assert[Int](redshiftUpdatedSink.output, "team_status", 1)

          val parquetMutatedSinks = sinks.filter(_.name == "parquet-team-mutated-sink")
          val parquetCreatedSink = parquetMutatedSinks.find(_.eventType == "TeamCreatedEvent").get
          assert(parquetCreatedSink.output.isEmpty)
          val parquetUpdatedSink = parquetMutatedSinks.find(_.eventType == "TeamUpdatedEvent").get
          assertExpectedFields(parquetUpdatedSink.output.schema.fields.toList, Parquet.TeamMutatedExpectedFields)
        }
      )
    }
  }

  test("TeamDeletedEvent") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = "parquet-team-deleted-source",
          value = """
                    |[
                    |  {
                    |    "eventType": "TeamDeletedEvent",
                    |    "loadtime": "2020-07-08T05:18:02.053Z",
                    |    "teamId": "ea60347f-ebd5-44c1-a24f-3ead3aa1681c",
                    |    "occurredOn": "2020-07-08 05:18:02.030",
                    |    "eventDateDw": "20200708"
                    |  }
                    |]
                    |""".stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val deltaDeletedSink = sinks.find { s =>
            s.name == "delta-team-sink" && s.eventType == "TeamDeletedEvent"
          }.get
          assertExpectedFields(deltaDeletedSink.output.schema.fields.toList, Delta.TeamDeletedExpectedFields)
          assert[Int](deltaDeletedSink.output, "team_status", 4)

          val redshiftDeletedSink = sinks.find { s =>
            s.name == "redshift-team-sink" && s.eventType == "TeamDeletedEvent"
          }.get
          assertExpectedFields(redshiftDeletedSink.output.schema.fields.toList, Redshift.TeamDeletedExpectedFields)
          assert[Int](redshiftDeletedSink.output, "team_status", 4)

          val parquetDeletedSink = sinks.find(_.name == "parquet-team-deleted-sink").get
          assertExpectedFields(parquetDeletedSink.output.schema.fields.toList, Parquet.TeamDeletedExpectedFields)
        }
      )
    }
  }

}
