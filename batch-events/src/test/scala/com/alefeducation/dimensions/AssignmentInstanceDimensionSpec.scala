package com.alefeducation.dimensions
import com.alefeducation.bigdata.Sink
import com.alefeducation.bigdata.batch.delta.{DeltaCreateSink, DeltaResetSink, DeltaUpdateSink}
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.service.DataSink
import com.alefeducation.util.Constants.AssignmentInstanceUpdatedEvent
import com.alefeducation.util.Helpers._
import com.alefeducation.util.StringUtilities.replaceSpecChars
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class AssignmentInstanceDimensionSpec extends SparkSuite with Matchers {

  trait Setup {
    implicit val transformer: AssignmentInstanceDimension = AssignmentInstanceDimension(spark)
  }

  val rsAISExpectedColumns = Set(
    "ais_created_time",
    "ais_dw_created_time",
    "ais_updated_time",
    "ais_dw_updated_time",
    "ais_deleted_time",
    "ais_instance_id",
    "ais_student_id",
    "ais_status"
  )

  val rsExpectedColumns = Set(
    "assignment_instance_allow_late_submission",
    "assignment_uuid",
    "class_uuid",
    "assignment_instance_created_time",
    "assignment_instance_deleted_time",
    "assignment_instance_due_on",
    "assignment_instance_dw_created_time",
    "assignment_instance_dw_updated_time",
    "grade_uuid",
    "assignment_instance_instructional_plan_id",
    "learning_path_uuid",
    "lo_uuid",
    "section_uuid",
    "assignment_instance_start_on",
    "assignment_instance_status",
    "subject_uuid",
    "teacher_uuid",
    "tenant_uuid",
    "assignment_instance_trimester_id",
    "assignment_instance_type",
    "assignment_instance_updated_time",
    "assignment_instance_id",
    "assignment_instance_teaching_period_id"
  )

  val expectedColumns = Set(
    "assignment_instance_tenant_id",
    "assignment_instance_id",
    "assignment_instance_assignment_id",
    "assignment_instance_due_on",
    "assignment_instance_allow_late_submission",
    "assignment_instance_status",
    "assignment_instance_teacher_id",
    "assignment_instance_type",
    "assignment_instance_grade_id",
    "assignment_instance_class_id",
    "assignment_instance_subject_id",
    "assignment_instance_section_id",
    "assignment_instance_learning_path_id",
    "assignment_instance_lo_id",
    "assignment_instance_start_on",
    "assignment_instance_dw_created_time",
    "assignment_instance_deleted_time",
    "assignment_instance_dw_updated_time",
    "assignment_instance_updated_time",
    "assignment_instance_created_time",
    "assignment_instance_trimester_id",
    "assignment_instance_group_id",
    "assignment_instance_instructional_plan_id",
    "assignment_instance_teaching_period_id"
  )

  val expectedColumnsForUpdate = Set(
    "assignment_instance_id",
    "assignment_instance_due_on",
    "assignment_instance_allow_late_submission",
    "assignment_instance_status",
    "assignment_instance_type",
    "assignment_instance_start_on",
    "assignment_instance_dw_created_time",
    "assignment_instance_deleted_time",
    "assignment_instance_dw_updated_time",
    "assignment_instance_updated_time",
    "assignment_instance_created_time",
    "assignment_instance_teaching_period_id"
  )

  val rsExpectedColumnsForUpdate = Set(
    "assignment_instance_id",
    "assignment_instance_due_on",
    "assignment_instance_allow_late_submission",
    "assignment_instance_status",
    "assignment_instance_type",
    "assignment_instance_start_on",
    "assignment_instance_dw_created_time",
    "assignment_instance_deleted_time",
    "assignment_instance_dw_updated_time",
    "assignment_instance_updated_time",
    "assignment_instance_created_time",
    "assignment_instance_teaching_period_id"
  )

  test("assignment instance created dimension") {
    new Setup {
      val source: String = AssignmentInstanceMutatedParquetSource
      val fixtures = List(
        SparkFixture(
          key = source,
          value = """
                    |[{
                    |  "eventType":"AssignmentInstanceCreatedEvent",
                    |  "tenantId":"tenantId",
                    |  "id": "assignmentInstanceId",
                    |  "assignmentId": "assignmentId",
                    |  "dueOn": 1587458643505,
                    |  "allowLateSubmission": true,
                    |  "teacherId": "teacherId",
                    |  "teachingPeriodId": "teaching-period-id",
                    |  "type": "SECTION",
                    |  "k12Grade": 6,
                    |  "schoolGradeId": "gradeID",
                    |  "classId":"classId",
                    |  "subjectId": "subjectId",
                    |  "sectionId": "sectionId",
                    |  "groupId": "gradeId",
                    |  "levelId": "levelId",
                    |  "mloId": "mloId",
                    |  "trimesterId": "trimesterId",
                    |  "instructionalPlanId":"instructionalPlanId",
                    |  "createdOn": 1587458320360,
                    |  "updatedOn": null,
                    |  "startOn": null,
                    |  "students": [
                    |    {
                    |        "id": "id1",
                    |        "active": true
                    |    },
                    |    {
                    |        "id": "id2",
                    |        "active": true
                    |    },
                    |    {
                    |        "id": "id3",
                    |        "active": true
                    |    }
                    |  ],
                    |  "occurredOn": "2020-02-18 11:48:46"
                    |}]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val s3Sink: Sink = sinks.filter(_.name == AssignmentInstanceMutatedParquetSource).head
          assert(s3Sink.asInstanceOf[DataSink].options.get("partitionBy") === Some("eventdate"))
          assert[String](s3Sink.output, "eventdate", "2020-02-18")

          val deltaCreateSink = sinks.filter(_.name == AssignmentInstanceDeltaSink).head
          val deltaCreateDf = deltaCreateSink.output.cache
          assert(deltaCreateDf.count == 1)
          assert(deltaCreateDf.columns.toSet === expectedColumns)

          assert[String](deltaCreateDf, "assignment_instance_id", "assignmentInstanceId")
          assert[String](deltaCreateDf, "assignment_instance_assignment_id", "assignmentId")
          assert[Boolean](deltaCreateDf, "assignment_instance_allow_late_submission", true)
          assert[String](deltaCreateDf, "assignment_instance_teacher_id", "teacherId")
          assert[String](deltaCreateDf, "assignment_instance_type", "SECTION")
          assert[String](deltaCreateDf, "assignment_instance_grade_id", "gradeID")
          assert[String](deltaCreateDf, "assignment_instance_subject_id", "subjectId")
          assert[String](deltaCreateDf, "assignment_instance_section_id", "sectionId")
          assert[String](deltaCreateDf, "assignment_instance_learning_path_id", "levelId")
          assert[String](deltaCreateDf, "assignment_instance_lo_id", "mloId")
          assert[Int](deltaCreateDf, "assignment_instance_status", 1)
          assert[String](deltaCreateDf, "assignment_instance_trimester_id", "trimesterId")
          assert[String](deltaCreateDf, "assignment_instance_group_id", "gradeId")

          val redshiftCreateSink = sinks.filter(_.name == AssignmentInstanceRedshiftSink).head
          val redshiftCreateDf = redshiftCreateSink.output.cache
          assert(redshiftCreateDf.count == 1)

          assert(redshiftCreateDf.columns.toSet === rsExpectedColumns)

          assert[String](redshiftCreateDf, "assignment_instance_id", "assignmentInstanceId")
          assert[String](redshiftCreateDf, "assignment_uuid", "assignmentId")
          assert[Boolean](redshiftCreateDf, "assignment_instance_allow_late_submission", true)
          assert[String](redshiftCreateDf, "teacher_uuid", "teacherId")
          assert[String](redshiftCreateDf, "assignment_instance_type", "SECTION")
          assert[String](redshiftCreateDf, "subject_uuid", "subjectId")
          assert[String](redshiftCreateDf, "section_uuid", "sectionId")
          assert[String](redshiftCreateDf, "learning_path_uuid", "levelId")
          assert[String](redshiftCreateDf, "lo_uuid", "mloId")
          assert[Int](redshiftCreateDf, "assignment_instance_status", 1)
          assert[String](redshiftCreateDf, "assignment_instance_trimester_id", "trimesterId")
          assert[String](redshiftCreateDf, "assignment_instance_due_on", "2020-04-21 08:44:03.505")
          assert[String](redshiftCreateDf, "assignment_instance_start_on", null)
        }
      )
    }
  }

  test("assignment instance update dimension") {
    new Setup {
      val source: String = AssignmentInstanceMutatedParquetSource
      val fixtures = List(
        SparkFixture(
          key = source,
          value = """
                    |[
                    |{
                    |  "eventType":"AssignmentInstanceUpdatedEvent",
                    |  "tenantId":"tenantId",
                    |  "id": "assignmentInstanceId",
                    |  "assignmentId": "assignmentId",
                    |  "dueOn": 1587458680706,
                    |  "allowLateSubmission": true,
                    |  "teacherId": "teacherId",
                    |  "teachingPeriodId": "teaching-period-id",
                    |  "type": "SECTION",
                    |  "k12Grade": 6,
                    |  "schoolGradeId": "gradeID",
                    |  "classId":"classId",
                    |  "subjectId": "subjectId",
                    |  "sectionId": "sectionId",
                    |  "groupId": "gradeId",
                    |  "levelId": "levelId",
                    |  "mloId": "mloId",
                    |  "trimesterId": "trimesterId",
                    |  "instructionalPlanId": "instructionalPlanId1",
                    |  "createdOn": 1587458358272,
                    |  "updatedOn": 1587458365387,
                    |  "startOn": 1587458371711,
                    |  "students": [
                    |    {
                    |        "id": "id1",
                    |        "active": true
                    |    },
                    |    {
                    |        "id": "id2",
                    |        "active": false
                    |    },
                    |    {
                    |        "id": "id3",
                    |        "active": true
                    |    }
                    |  ],
                    |  "occurredOn": "2020-02-18 11:48:49"
                    |}]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>

          val deltaUpdateSink = sinks.filter(_.name == AssignmentInstanceDeltaSink)
          val deltaUpdateDf = deltaUpdateSink.head.output.cache
          assert(deltaUpdateDf.count == 1)
          assert(deltaUpdateDf.columns.toSet === expectedColumnsForUpdate)
          assert[String](deltaUpdateDf, "assignment_instance_id", "assignmentInstanceId")
          assert[Boolean](deltaUpdateDf, "assignment_instance_allow_late_submission", true)
          assert[String](deltaUpdateDf, "assignment_instance_type", "SECTION")

          val redshiftUpdateSink =
            sinks.filter(s => s.name == AssignmentInstanceRedshiftSink && s.eventType == AssignmentInstanceUpdatedEvent)
          val RsUpdateDf = redshiftUpdateSink.head.output.cache
          assert(RsUpdateDf.count == 1)
          assert(RsUpdateDf.columns.toSet === rsExpectedColumnsForUpdate)
          assert[String](RsUpdateDf, "assignment_instance_id", "assignmentInstanceId")
          assert[Boolean](RsUpdateDf, "assignment_instance_allow_late_submission", true)
          assert[String](RsUpdateDf, "assignment_instance_type", "SECTION")
          assert[Int](RsUpdateDf, "assignment_instance_status", 1)

          assert[String](RsUpdateDf, "assignment_instance_due_on", "2020-04-21 08:44:40.706")
          assert[String](RsUpdateDf, "assignment_instance_start_on", "2020-04-21 08:39:31.711")
        }
      )
    }
  }

  test("assignment instance deleted dimension") {
    new Setup {
      val source: String = AssignmentInstanceDeletedParquetSource
      val fixtures = List(
        SparkFixture(
          key = source,
          value = """
                    |[{
                    |  "eventType":"AssignmentInstanceDeletedEvent",
                    |  "tenantId":"tenantId",
                    |  "id": "assignmentInstanceId",
                    |  "assignmentId": "assignmentId",
                    |  "occurredOn": "2020-02-18 11:48:46"
                    |}]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val expectedColumns = Set(
            "assignment_instance_id",
            "assignment_instance_status",
            "assignment_instance_dw_created_time",
            "assignment_instance_deleted_time",
            "assignment_instance_dw_updated_time",
            "assignment_instance_updated_time",
            "assignment_instance_created_time"
          )

          val deltaDeleteSink = sinks.filter(_.name == AssignmentInstanceDeltaSink).head.output.cache()
          assert(deltaDeleteSink.count === 1)
          assert(deltaDeleteSink.columns.toSet === expectedColumns)
          assert[String](deltaDeleteSink, "assignment_instance_id", "assignmentInstanceId")
          assert[Int](deltaDeleteSink, "assignment_instance_status", 4)

          val redshiftDeleteSink = sinks.filter(_.name == AssignmentInstanceRedshiftSink).head.output.cache()
          assert(redshiftDeleteSink.count === 1)
          assert(redshiftDeleteSink.columns.toSet === expectedColumns)
          assert[String](redshiftDeleteSink, "assignment_instance_id", "assignmentInstanceId")
          assert[Int](redshiftDeleteSink, "assignment_instance_status", 4)
        }
      )
    }
  }

  test("assignment instance no students in all the events") {
    new Setup {
      val source: String = AssignmentInstanceMutatedParquetSource
      val fixtures = List(
        SparkFixture(
          key = source,
          value = """
                    |[{
                    |  "eventType":"AssignmentInstanceCreatedEvent",
                    |  "tenantId":"tenantId",
                    |  "id": "assignmentInstanceId",
                    |  "assignmentId": "assignmentId",
                    |  "teachingPeriodId": "teaching-period-id",
                    |  "dueOn": 1587458643505,
                    |  "allowLateSubmission": true,
                    |  "teacherId": "teacherId",
                    |  "type": "SECTION",
                    |  "k12Grade": 6,
                    |  "classId":"classId",
                    |  "schoolGradeId": "gradeID",
                    |  "subjectId": "subjectId",
                    |  "sectionId": "sectionId",
                    |  "groupId": "gradeId",
                    |  "levelId": "levelId",
                    |  "mloId": "mloId",
                    |  "trimesterId": "trimesterId",
                    |  "createdOn": 1587458320360,
                    |  "updatedOn": null,
                    |  "startOn": null,
                    |  "students": [],
                    |  "occurredOn": "2020-02-18 11:48:46"
                    |},
                    |{
                    |  "eventType":"AssignmentInstanceUpdatedEvent",
                    |  "tenantId":"tenantId",
                    |  "id": "assignmentInstanceId",
                    |  "assignmentId": "assignmentId",
                    |  "dueOn": 1587458680706,
                    |  "allowLateSubmission": true,
                    |  "teacherId": "teacherId",
                    |  "type": "SECTION",
                    |  "k12Grade": 6,
                    |  "classId":"classId",
                    |  "schoolGradeId": "gradeID",
                    |  "subjectId": "subjectId",
                    |  "sectionId": "sectionId",
                    |  "groupId": "gradeId",
                    |  "levelId": "levelId",
                    |  "mloId": "mloId",
                    |  "trimesterId": "trimesterId",
                    |  "instructionalPlanId": "instructionalPlanId1",
                    |  "createdOn": 1587458358272,
                    |  "updatedOn": 1587458365387,
                    |  "startOn": 1587458371711,
                    |  "students": [],
                    |  "occurredOn": "2020-02-18 11:48:49"
                    |}]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val deltaSinks = sinks.filter(_.name == AssignmentInstanceDeltaSink)
          val deltaCreateDf = deltaSinks.head.output.cache()
          assert(deltaCreateDf.count === 1)
          assert(deltaCreateDf.columns.toSet === expectedColumns)
          assert[String](deltaCreateDf, "assignment_instance_id", "assignmentInstanceId")
          assert[String](deltaCreateDf, "assignment_instance_assignment_id", "assignmentId")
          assert[Boolean](deltaCreateDf, "assignment_instance_allow_late_submission", true)
          assert[String](deltaCreateDf, "assignment_instance_teacher_id", "teacherId")
          assert[String](deltaCreateDf, "assignment_instance_type", "SECTION")
          assert[String](deltaCreateDf, "assignment_instance_grade_id", "gradeID")
          assert[String](deltaCreateDf, "assignment_instance_subject_id", "subjectId")
          assert[String](deltaCreateDf, "assignment_instance_section_id", "sectionId")
          assert[String](deltaCreateDf, "assignment_instance_learning_path_id", "levelId")
          assert[String](deltaCreateDf, "assignment_instance_lo_id", "mloId")
          assert[Int](deltaCreateDf, "assignment_instance_status", 1)
          assert[String](deltaCreateDf, "assignment_instance_trimester_id", "trimesterId")
          assert[String](deltaCreateDf, "assignment_instance_group_id", "gradeId")

          val deltaUpdateDf = deltaSinks.tail.head.output.cache()
          assert(deltaUpdateDf.count === 1)
          assert[String](deltaUpdateDf, "assignment_instance_id", "assignmentInstanceId")
          assert[Boolean](deltaUpdateDf, "assignment_instance_allow_late_submission", true)
          assert[String](deltaUpdateDf, "assignment_instance_type", "SECTION")
          assert[Int](deltaUpdateDf, "assignment_instance_status", 1)

          val deltaUpdateStudentsDf = deltaSinks.head.output.cache()
          assert(deltaUpdateStudentsDf.count === 1)
          assert[String](deltaUpdateStudentsDf, "assignment_instance_id", "assignmentInstanceId")
          assert[Int](deltaUpdateStudentsDf, "assignment_instance_status", 1)

          val redshiftSinks = sinks.filter(_.name == AssignmentInstanceRedshiftSink)
          val redshiftCreateDf = redshiftSinks.head.output.cache()
          assert(redshiftCreateDf.count === 1)
          assert[String](redshiftCreateDf, "assignment_instance_id", "assignmentInstanceId")
          assert[String](redshiftCreateDf, "assignment_uuid", "assignmentId")
          assert[Boolean](redshiftCreateDf, "assignment_instance_allow_late_submission", true)
          assert[String](redshiftCreateDf, "teacher_uuid", "teacherId")
          assert[String](redshiftCreateDf, "assignment_instance_type", "SECTION")
          assert[String](redshiftCreateDf, "grade_uuid", "gradeID")
          assert[String](redshiftCreateDf, "subject_uuid", "subjectId")
          assert[String](redshiftCreateDf, "section_uuid", "sectionId")
          assert[String](redshiftCreateDf, "learning_path_uuid", "levelId")
          assert[String](redshiftCreateDf, "lo_uuid", "mloId")
          assert[Int](redshiftCreateDf, "assignment_instance_status", 1)
          assert[String](redshiftCreateDf, "assignment_instance_trimester_id", "trimesterId")

          val redshiftUpdateDf = redshiftSinks.tail.head.output.cache()
          assert(redshiftUpdateDf.count === 1)
          assert(redshiftUpdateDf.columns.toSet === rsExpectedColumnsForUpdate)
          assert[String](redshiftUpdateDf, "assignment_instance_id", "assignmentInstanceId")
          assert[Boolean](redshiftUpdateDf, "assignment_instance_allow_late_submission", true)
          assert[String](redshiftUpdateDf, "assignment_instance_type", "SECTION")
          assert[Int](redshiftUpdateDf, "assignment_instance_status", 1)
          assert[Int](redshiftUpdateDf, "assignment_instance_status", 1)
        }
      )
    }
  }
  //TODO : In future if this comes from app we need to modify code accordingly. This scenario doesn't work now.
  ignore("assignment instance no students in one of the events") {
    new Setup {
      val source: String = AssignmentInstanceMutatedParquetSource
      val fixtures = List(
        SparkFixture(
          key = source,
          value = """
                    |[{
                    |  "eventType":"AssignmentInstanceCreatedEvent",
                    |  "tenantId":"tenantId",
                    |  "id": "assignmentInstanceId",
                    |  "assignmentId": "assignmentId",
                    |  "dueOn": 1587458643505,
                    |  "allowLateSubmission": true,
                    |  "teachingPeriodId": "teaching-period-id",
                    |  "teacherId": "teacherId",
                    |  "type": "SECTION",
                    |  "k12Grade": 6,
                    |  "schoolGradeId": "gradeID",
                    |  "subjectId": "subjectId",
                    |  "sectionId": "sectionId",
                    |  "groupId": "gradeId",
                    |  "levelId": "levelId",
                    |  "mloId": "mloId",
                    |  "trimesterId": "trimesterId",
                    |  "createdOn": 1587458320360,
                    |  "updatedOn": null,
                    |  "startOn": null,
                    |  "students": [],
                    |  "occurredOn": "2020-02-18 11:48:46"
                    |},
                    |{
                    |  "eventType":"AssignmentInstanceUpdatedEvent",
                    |  "tenantId":"tenantId",
                    |  "id": "assignmentInstanceId",
                    |  "assignmentId": "assignmentId",
                    |  "teachingPeriodId": "teaching-period-id",
                    |  "dueOn": 1587458680706,
                    |  "allowLateSubmission": true,
                    |  "teacherId": "teacherId",
                    |  "type": "SECTION",
                    |  "k12Grade": 6,
                    |  "schoolGradeId": "gradeID",
                    |  "subjectId": "subjectId",
                    |  "sectionId": "sectionId",
                    |  "groupId": "gradeId",
                    |  "levelId": "levelId",
                    |  "mloId": "mloId",
                    |  "trimesterId": "trimesterId",
                    |  "instructionalPlanId": "instructionalPlanId1",
                    |  "createdOn": 1587458358272,
                    |  "updatedOn": 1587458365387,
                    |  "startOn": 1587458371711,
                    |  "students": [
                    |    {
                    |        "id": "id1",
                    |        "active": true
                    |    },
                    |    {
                    |        "id": "id2",
                    |        "active": false
                    |    },
                    |    {
                    |        "id": "id3",
                    |        "active": true
                    |    }
                    |  ],
                    |  "occurredOn": "2020-02-18 11:48:49"
                    |}]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          assert(sinks.size == 4)

          val parquetSinks = sinks.collect { case s: DataSink => s }
          val parquetMutatedSink = parquetSinks.head
          assert(parquetMutatedSink.options.get("partitionBy") === Some("eventdate"))
          assert[String](parquetMutatedSink.df, "eventdate", "2020-02-18")

          val deltaCreateSink =
            sinks.collectFirst { case s: DeltaCreateSink => s }.getOrElse(throw new AssertionError(s"delta create sink not found"))
          assert(deltaCreateSink.df.count === 1)
          assert(deltaCreateSink.df.columns.toSet === expectedColumns)
          assert[String](deltaCreateSink.df, "assignment_instance_id", "assignmentInstanceId")
          assert[String](deltaCreateSink.df, "assignment_instance_assignment_id", "assignmentId")
          assert[Boolean](deltaCreateSink.df, "assignment_instance_allow_late_submission", true)
          assert[String](deltaCreateSink.df, "assignment_instance_teacher_id", "teacherId")
          assert[String](deltaCreateSink.df, "assignment_instance_type", "SECTION")
          assert[String](deltaCreateSink.df, "assignment_instance_grade_id", "gradeID")
          assert[String](deltaCreateSink.df, "assignment_instance_subject_id", "subjectId")
          assert[String](deltaCreateSink.df, "assignment_instance_section_id", "sectionId")
          assert[String](deltaCreateSink.df, "assignment_instance_learning_path_id", "levelId")
          assert[String](deltaCreateSink.df, "assignment_instance_lo_id", "mloId")
          //TODO Timestamp comparison to be fixed to remove timezone
          //          assert[String](deltaCreateSink.df, "assignment_instance_due_on", "2020-04-21 08:44:03.505")
          assert[String](deltaCreateSink.df, "assignment_instance_start_on", null)
          assert[String](deltaCreateSink.df, "assignment_instance_student_id", null)
          assert[String](deltaCreateSink.df, "assignment_instance_student_status", null)
          assert[Int](deltaCreateSink.df, "assignment_instance_status", 1)
          assert[String](deltaCreateSink.df, "assignment_instance_trimester_id", "trimesterId")
          assert[String](deltaCreateSink.df, "assignment_instance_group_id", "gradeId")

          val studentColumns = Set("assignment_instance_student_status", "assignment_instance_student_id")
          val deltaUpdateSink =
            sinks.collectFirst { case s: DeltaUpdateSink => s }.getOrElse(throw new AssertionError(s"delta update sink not found"))
          assert(deltaUpdateSink.df.count === 1)
          assert(deltaUpdateSink.df.columns.toSet === expectedColumns.diff(studentColumns))
          assert[String](deltaUpdateSink.df, "assignment_instance_id", "assignmentInstanceId")
          assert[String](deltaUpdateSink.df, "assignment_instance_assignment_id", "assignmentId")
          assert[Boolean](deltaUpdateSink.df, "assignment_instance_allow_late_submission", true)
          assert[String](deltaUpdateSink.df, "assignment_instance_teacher_id", "teacherId")
          assert[String](deltaUpdateSink.df, "assignment_instance_type", "SECTION")
          assert[String](deltaUpdateSink.df, "assignment_instance_grade_id", "gradeID")
          assert[String](deltaUpdateSink.df, "assignment_instance_subject_id", "subjectId")
          assert[String](deltaUpdateSink.df, "assignment_instance_section_id", "sectionId")
          assert[String](deltaUpdateSink.df, "assignment_instance_learning_path_id", "levelId")
          assert[String](deltaUpdateSink.df, "assignment_instance_lo_id", "mloId")
          assert[Int](deltaUpdateSink.df, "assignment_instance_status", 1)
          assert[String](deltaUpdateSink.df, "assignment_instance_trimester_id", "trimesterId")
          assert[String](deltaUpdateSink.df, "assignment_instance_group_id", "gradeId")
          assert[String](deltaUpdateSink.df, "assignment_instance_instructional_plan_id", "instructionalPlanId1")
          deltaUpdateSink.matchConditions must include("assignment_instance_id")
          deltaUpdateSink.updateFields must have size 20
          deltaUpdateSink.updateFields must contain key "assignment_instance_instructional_plan_id"

          val deltaUpdateStudentsSink =
            sinks.last match { case s: DeltaUpdateSink => s }
          assert(deltaUpdateStudentsSink.df.count === 3)
          assert(deltaUpdateStudentsSink.df.columns.toSet === expectedColumns)
          assert[String](deltaUpdateStudentsSink.df, "assignment_instance_id", "assignmentInstanceId")
          assert[String](deltaUpdateStudentsSink.df, "assignment_instance_assignment_id", "assignmentId")
          assert[String](deltaUpdateStudentsSink.df, "assignment_instance_student_id", "id1")
          assert[Boolean](deltaUpdateStudentsSink.df, "assignment_instance_student_status", true)
          assert[Int](deltaUpdateStudentsSink.df, "assignment_instance_status", 1)
          deltaUpdateStudentsSink.matchConditions must include("assignment_instance_student_id")
          deltaUpdateStudentsSink.updateFields must have size 3
          deltaUpdateStudentsSink.updateFields must contain key "assignment_instance_student_status"
        }
      )
    }
  }

  test("assignment instance students from AssignmentInstanceCreatedEvent") {
    new Setup {
      val source: String = AssignmentInstanceMutatedParquetSource
      val fixtures = List(
        SparkFixture(
          key = source,
          value = """
                    |[{
                    |  "eventType":"AssignmentInstanceCreatedEvent",
                    |  "tenantId":"tenantId",
                    |  "id": "assignmentInstanceId",
                    |  "assignmentId": "assignmentId",
                    |  "dueOn": 1587458643505,
                    |  "allowLateSubmission": true,
                    |  "teachingPeriodId": "teaching-period-id",
                    |  "teacherId": "teacherId",
                    |  "type": "SECTION",
                    |  "k12Grade": 6,
                    |  "schoolGradeId": "gradeID",
                    |  "classId":"classId",
                    |  "subjectId": "subjectId",
                    |  "sectionId": "sectionId",
                    |  "groupId": "gradeId",
                    |  "levelId": "levelId",
                    |  "mloId": "mloId",
                    |  "trimesterId": "trimesterId",
                    |  "instructionalPlanId":"instructionalPlanId",
                    |  "createdOn": 1587458320360,
                    |  "updatedOn": null,
                    |  "startOn": null,
                    |  "students": [
                    |    {
                    |        "id": "id1",
                    |        "active": true
                    |    },
                    |    {
                    |        "id": "id2",
                    |        "active": true
                    |    },
                    |    {
                    |        "id": "id3",
                    |        "active": true
                    |    }
                    |  ],
                    |  "occurredOn": "2020-02-18 11:48:46",
                    |  "loadtime": "2020-02-18 11:48:46",
                    |  "eventDateDw": "2020-02-18"
                    |}]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>

          assert(sinks.size === 5)

          val deltaCreateSink = sinks.filter(_.name == AssignmentInstanceStudentDeltaSink).head
          val deltaCreateDf = deltaCreateSink.output.cache
          assert(deltaCreateDf.count == 3)
          assert(deltaCreateDf.columns.toSet === rsAISExpectedColumns)

          assert[String](deltaCreateDf, "ais_instance_id", "assignmentInstanceId")
          assert[String](deltaCreateDf, "ais_student_id", "id1")
          assert[Int](deltaCreateDf, "ais_status", 1)

          val redshiftCreateSink = sinks.filter(_.name == AssignmentInstanceStudentRedshiftSink).head
          val redshiftCreateDf = redshiftCreateSink.output.cache
          assert(redshiftCreateDf.count == 3)

          assert(redshiftCreateDf.columns.toSet === rsAISExpectedColumns)

          assert[String](redshiftCreateDf, "ais_instance_id", "assignmentInstanceId")
          assert[String](redshiftCreateDf, "ais_student_id", "id1")
          assert[Int](redshiftCreateDf, "ais_status", 1)
        }
      )
    }
  }

  test("should not flow records when AssignmentInstanceCreatedEvent contains no students") {
    new Setup {
      val source: String = AssignmentInstanceMutatedParquetSource
      val fixtures = List(
        SparkFixture(
          key = source,
          value = """
                    |[{
                    |  "eventType":"AssignmentInstanceCreatedEvent",
                    |  "tenantId":"tenantId",
                    |  "id": "assignmentInstanceId",
                    |  "assignmentId": "assignmentId",
                    |  "dueOn": 1587458643505,
                    |  "allowLateSubmission": true,
                    |  "teacherId": "teacherId",
                    |  "teachingPeriodId": "teaching-period-id",
                    |  "type": "SECTION",
                    |  "k12Grade": 6,
                    |  "schoolGradeId": "gradeID",
                    |  "classId":"classId",
                    |  "subjectId": "subjectId",
                    |  "sectionId": "sectionId",
                    |  "groupId": "gradeId",
                    |  "levelId": "levelId",
                    |  "mloId": "mloId",
                    |  "trimesterId": "trimesterId",
                    |  "instructionalPlanId":"instructionalPlanId",
                    |  "createdOn": 1587458320360,
                    |  "updatedOn": null,
                    |  "startOn": null,
                    |  "students": [],
                    |  "occurredOn": "2020-02-18 11:48:46",
                    |  "loadtime": "2020-02-18 11:48:46",
                    |  "eventDateDw": "2020-02-18"
                    |}]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>

          assert(sinks.size === 4)
          val deltaCreateSink = sinks.filter(_.name == AssignmentInstanceStudentDeltaSink)
          assert(deltaCreateSink.isEmpty)
          val redshiftDf = sinks.filter(_.name == AssignmentInstanceStudentRedshiftSink).head.output.cache()
          assert(redshiftDf.isEmpty)
        }
      )
    }
  }

  test("should process AssignmentInstanceUpdatedEvent in assignment_instance_student dim") {
    new Setup {
      val source: String = AssignmentInstanceMutatedParquetSource
      val fixtures = List(
        SparkFixture(
          key = source,
          value = """
                    |[{
                    |  "eventType":"AssignmentInstanceUpdatedEvent",
                    |  "tenantId":"tenantId",
                    |  "id": "assignmentInstanceId",
                    |  "assignmentId": "assignmentId",
                    |  "dueOn": 1587458643505,
                    |  "allowLateSubmission": true,
                    |  "teacherId": "teacherId",
                    |  "type": "SECTION",
                    |  "k12Grade": 6,
                    |  "schoolGradeId": "gradeID",
                    |  "teachingPeriodId": "teaching-period-id",
                    |  "classId":"classId",
                    |  "subjectId": "subjectId",
                    |  "sectionId": "sectionId",
                    |  "groupId": "gradeId",
                    |  "levelId": "levelId",
                    |  "mloId": "mloId",
                    |  "trimesterId": "trimesterId",
                    |  "instructionalPlanId":"instructionalPlanId",
                    |  "createdOn": 1587458320360,
                    |  "updatedOn": null,
                    |  "startOn": null,
                    |  "students": [{
                    |        "id": "id1",
                    |        "active": true
                    |    }],
                    |  "occurredOn": "2020-02-18 11:48:46",
                    |  "loadtime": "2020-02-18 11:48:46",
                    |  "eventDateDw": "2020-02-18"
                    |}]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>

          assert(sinks.size === 6)
          val redshiftDf = sinks.filter(_.name == AssignmentInstanceStudentRedshiftSink).head.output.cache()
          assert(redshiftDf.count == 1)

          val deltaResetStudentSink: DeltaResetSink = sinks.filter(_.name == AssignmentInstanceStudentDeltaSink).head.asInstanceOf[DeltaResetSink]

          deltaResetStudentSink.uniqueIdColumns() should be(List("ais_instance_id"))
          replaceSpecChars(deltaResetStudentSink.matchConditions) should be(replaceSpecChars("delta.ais_instance_id = events.ais_instance_id"))
        }
      )
    }
  }

  test("assignment instance students from AssignmentInstanceStudentEvent") {
    new Setup {
      val source: String = AssignmentInstanceStudentParquetSource
      val fixtures = List(
        SparkFixture(
          key = source,
          value = """
                    |[{
                    |  "eventType":"AssignmentInstanceStudentUpdatedEvent",
                    |  "id": "assignmentInstanceId",
                    |  "students": [
                    |    {
                    |        "id": "id1",
                    |        "active": true
                    |    },
                    |    {
                    |        "id": "id2",
                    |        "active": true
                    |    },
                    |    {
                    |        "id": "id3",
                    |        "active": true
                    |    }
                    |  ],
                    |  "occurredOn": "2020-02-18 11:48:46",
                    |  "loadtime": "2020-02-18 11:48:46",
                    |  "eventDateDw": "2020-02-18"
                    |}]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>

          assert(sinks.size === 3 )

          val deltaCreateSink = sinks.filter(_.name == AssignmentInstanceStudentDeltaSink).head
          val deltaCreateDf = deltaCreateSink.output.cache
          assert(deltaCreateDf.count == 3)
          assert(deltaCreateDf.columns.toSet === rsAISExpectedColumns)

          assert[String](deltaCreateDf, "ais_instance_id", "assignmentInstanceId")
          assert[String](deltaCreateDf, "ais_student_id", "id1")
          assert[Int](deltaCreateDf, "ais_status", 1)

          val redshiftCreateSink = sinks.filter(_.name == AssignmentInstanceStudentRedshiftSink).head
          val redshiftCreateDf = redshiftCreateSink.output.cache
          assert(redshiftCreateDf.count == 3)

          assert(redshiftCreateDf.columns.toSet === rsAISExpectedColumns)

          assert[String](redshiftCreateDf, "ais_instance_id", "assignmentInstanceId")
          assert[String](redshiftCreateDf, "ais_student_id", "id1")
          assert[Int](redshiftCreateDf, "ais_status", 1)
        }
      )
    }
  }

  test("assignment instance students from both types of events") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = AssignmentInstanceMutatedParquetSource,
          value = """
                    |[{
                    |  "eventType":"AssignmentInstanceCreatedEvent",
                    |  "tenantId":"tenantId",
                    |  "id": "assignmentInstanceId1",
                    |  "assignmentId": "assignmentId",
                    |  "dueOn": 1587458643505,
                    |  "allowLateSubmission": true,
                    |  "teacherId": "teacherId",
                    |  "teachingPeriodId": "teaching-period-id",
                    |  "type": "SECTION",
                    |  "k12Grade": 6,
                    |  "schoolGradeId": "gradeID",
                    |  "classId":"classId",
                    |  "subjectId": "subjectId",
                    |  "sectionId": "sectionId",
                    |  "groupId": "gradeId",
                    |  "levelId": "levelId",
                    |  "mloId": "mloId",
                    |  "trimesterId": "trimesterId",
                    |  "instructionalPlanId":"instructionalPlanId",
                    |  "createdOn": 1587458320360,
                    |  "updatedOn": null,
                    |  "startOn": null,
                    |  "students": [
                    |    {
                    |        "id": "id1",
                    |        "active": true
                    |    },
                    |    {
                    |        "id": "id2",
                    |        "active": true
                    |    },
                    |    {
                    |        "id": "id3",
                    |        "active": true
                    |    }
                    |  ],
                    |  "occurredOn": "2020-02-18 11:48:46",
                    |  "loadtime": "2020-02-18 11:48:46",
                    |  "eventDateDw": "2020-02-18"
                    |}]
                  """.stripMargin
        ),
        SparkFixture(
          key = AssignmentInstanceStudentParquetSource,
          value = """
                    |[{
                    |  "eventType":"AssignmentInstanceStudentUpdatedEvent",
                    |  "id": "assignmentInstanceId2",
                    |  "students": [
                    |    {
                    |        "id": "studentId1",
                    |        "active": true
                    |    },
                    |    {
                    |        "id": "studentId2",
                    |        "active": true
                    |    },
                    |    {
                    |        "id": "studentId3",
                    |        "active": true
                    |    }
                    |  ],
                    |  "occurredOn": "2020-02-18 11:48:46",
                    |  "loadtime": "2020-02-18 11:48:46",
                    |  "eventDateDw": "2020-02-18"
                    |}]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>

          assert(sinks.size === 6 )

          val deltaCreateSink = sinks.filter(_.name == AssignmentInstanceStudentDeltaSink).head
          val deltaCreateDf = deltaCreateSink.output.cache
          assert(deltaCreateDf.count == 6)
          assert(deltaCreateDf.columns.toSet === rsAISExpectedColumns)

          assert[String](deltaCreateDf, "ais_instance_id", "assignmentInstanceId1")
          assert[String](deltaCreateDf, "ais_student_id", "id1")
          assert[Int](deltaCreateDf, "ais_status", 1)

          val redshiftCreateSink = sinks.filter(_.name == AssignmentInstanceStudentRedshiftSink).head
          val redshiftCreateDf = redshiftCreateSink.output.cache
          assert(redshiftCreateDf.count == 6)

          assert(redshiftCreateDf.columns.toSet === rsAISExpectedColumns)

          assert[String](redshiftCreateDf, "ais_instance_id", "assignmentInstanceId1")
          assert[String](redshiftCreateDf, "ais_student_id", "id1")
          assert[Int](redshiftCreateDf, "ais_status", 1)
        }
      )
    }
  }

  test("assignment instance students no events") {
    new Setup {
      val fixtures = Nil
      withSparkFixtures(
        fixtures, { sinks =>
          assert(sinks.size === 0)
        }
      )
    }
  }

}
