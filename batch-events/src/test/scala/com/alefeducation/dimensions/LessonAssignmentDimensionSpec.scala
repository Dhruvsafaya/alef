package com.alefeducation.dimensions

import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.util.Helpers._

class LessonAssignmentDimensionSpec extends SparkSuite with BaseDimensionSpec {

  test("Test Lesson Assigned Unassigned events to Student/Class") {
    implicit val transformer = new LessonAssignmentDimension(LessonAssignmentDimensionName, spark)

    val fixtures = List(
      SparkFixture(
        key = ParquetLessonStudentSource,
        value =
          """
            |[
            |{"eventType":"LearnerLessonAssignedEvent","loadtime":"2020-05-27T10:28:57.011Z",  "classId":"class1","mloId":"mlo-33336","studentId":"student1","assignedBy":"teacher-16","occurredOn":"2020-05-27 10:28:56.910","eventDateDw":"20200527"},
            |{"eventType":"LearnerLessonAssignedEvent","loadtime":"2020-05-28T07:18:23.749Z",  "classId":"class1","mloId":"mlo-33337","studentId":"student2","assignedBy":"teacher-17","occurredOn":"2020-05-28 07:18:23.735","eventDateDw":"20200528"},
            |{"eventType":"LearnerLessonAssignedEvent","loadtime":"2020-05-28T07:18:23.749Z",  "classId":"class1","mloId":"mlo-33337","studentId":"student3","assignedBy":"teacher-17","occurredOn":"2020-05-28 07:18:23.735","eventDateDw":"20200528"},
            |{"eventType":"LearnerLessonUnAssignedEvent","loadtime":"2020-05-28T07:18:23.749Z","classId":"class1","mloId":"mlo-33337","studentId":"student2","assignedBy":"teacher-17","occurredOn":"2020-05-28 07:18:25.735","eventDateDw":"20200528"},
            |{"eventType":"LearnerLessonUnAssignedEvent","loadtime":"2020-05-28T07:18:41.414Z","classId":"class1","mloId":"mlo-33337","studentId":"student3","assignedBy":"teacher-17","occurredOn":"2020-05-28 07:18:41.408","eventDateDw":"20200528"},
            |{"eventType":"LearnerLessonAssignedEvent","loadtime":"2020-05-28T07:19:28.144Z",  "classId":"class1","mloId":"mlo-33338","studentId":"student1","assignedBy":"teacher-18","occurredOn":"2020-05-28 07:19:28.138","eventDateDw":"20200528"},
            |{"eventType":"LearnerLessonAssignedEvent","loadtime":"2020-05-28T07:19:28.144Z",  "classId":"class1","mloId":"mlo-33339","studentId":"student2","assignedBy":"teacher-19","occurredOn":"2020-05-28 07:19:28.138","eventDateDw":"20200528"},
            |{"eventType":"LearnerLessonAssignedEvent","loadtime":"2020-05-28T07:19:28.145Z",  "classId":"class1","mloId":"mlo-33340","studentId":"student3","assignedBy":"teacher-10","occurredOn":"2020-05-28 07:19:28.138","eventDateDw":"20200528"},
            |{"eventType":"LearnerLessonAssignedEvent","loadtime":"2020-05-28T07:19:28.145Z",  "classId":"class1","mloId":"mlo-33338","studentId":"student3","assignedBy":"teacher-18","occurredOn":"2020-05-28 07:19:28.138","eventDateDw":"20200528"},
            |{"eventType":"LearnerLessonUnAssignedEvent","loadtime":"2020-05-28T07:19:38.456Z","classId":"class1","mloId":"mlo-33338","studentId":"student3","assignedBy":"teacher-18","occurredOn":"2020-05-28 07:19:38.454","eventDateDw":"20200528"}
            |]
            |""".stripMargin
      ),
      SparkFixture(
        key = ParquetLessonClassSource,
        value =
          """
            |[
            |{"eventType":"ClassLessonAssignedEvent","loadtime":"2020-05-27T10:28:57.011Z",  "classId":"class1","mloId":"mlo-33336","assignedBy":"teacher-16","occurredOn":"2020-05-27 10:28:56.910","eventDateDw":"20200527"},
            |{"eventType":"ClassLessonAssignedEvent","loadtime":"2020-05-28T07:18:23.749Z",  "classId":"class1","mloId":"mlo-33337","assignedBy":"teacher-17","occurredOn":"2020-05-28 07:18:23.735","eventDateDw":"20200528"},
            |{"eventType":"ClassLessonAssignedEvent","loadtime":"2020-05-28T07:18:23.749Z",  "classId":"class1","mloId":"mlo-33337","assignedBy":"teacher-17","occurredOn":"2020-05-28 07:18:23.735","eventDateDw":"20200528"},
            |{"eventType":"ClassLessonUnAssignedEvent","loadtime":"2020-05-28T07:18:23.749Z","classId":"class1","mloId":"mlo-33337","assignedBy":"teacher-17","occurredOn":"2020-05-28 07:18:25.735","eventDateDw":"20200528"},
            |{"eventType":"ClassLessonUnAssignedEvent","loadtime":"2020-05-28T07:18:41.414Z","classId":"class1","mloId":"mlo-33337","assignedBy":"teacher-17","occurredOn":"2020-05-28 07:18:41.408","eventDateDw":"20200528"},
            |{"eventType":"ClassLessonAssignedEvent","loadtime":"2020-05-28T07:19:28.144Z",  "classId":"class1","mloId":"mlo-33338","assignedBy":"teacher-18","occurredOn":"2020-05-28 07:19:28.138","eventDateDw":"20200528"},
            |{"eventType":"ClassLessonAssignedEvent","loadtime":"2020-05-28T07:19:28.144Z",  "classId":"class1","mloId":"mlo-33339","assignedBy":"teacher-19","occurredOn":"2020-05-28 07:19:28.138","eventDateDw":"20200528"},
            |{"eventType":"ClassLessonAssignedEvent","loadtime":"2020-05-28T07:19:28.145Z",  "classId":"class1","mloId":"mlo-33340","assignedBy":"teacher-10","occurredOn":"2020-05-28 07:19:28.138","eventDateDw":"20200528"},
            |{"eventType":"ClassLessonAssignedEvent","loadtime":"2020-05-28T07:19:28.145Z",  "classId":"class1","mloId":"mlo-33338","assignedBy":"teacher-18","occurredOn":"2020-05-28 07:19:28.138","eventDateDw":"20200528"},
            |{"eventType":"ClassLessonUnAssignedEvent","loadtime":"2020-05-28T07:19:38.456Z","classId":"class1","mloId":"mlo-33338","assignedBy":"teacher-18","occurredOn":"2020-05-28 07:19:38.454","eventDateDw":"20200528"}
            |]
            |""".stripMargin
      )
    )

    withSparkFixtures(
      fixtures, { sinks =>
        val expectedPqCols = Seq(
          "eventDateDw",
          "eventType",
          "loadtime",
          "occurredOn",
          "studentId",
          "classId",
          "mloId",
          "assignedBy",
          "eventdate"
        )
        val expectedRsCols = Seq(
          "lesson_assignment_created_time",
          "lesson_assignment_updated_time",
          "lesson_assignment_dw_created_time",
          "lesson_assignment_dw_updated_time",
          "lesson_assignment_status",
          "student_uuid",
          "class_uuid",
          "lo_uuid",
          "teacher_uuid",
          "lesson_assignment_assign_status",
          "lesson_assignment_type"
        )
        val expectedDeltaCols = Seq(
          "lesson_assignment_created_time",
          "lesson_assignment_updated_time",
          "lesson_assignment_dw_created_time",
          "lesson_assignment_dw_updated_time",
          "lesson_assignment_status",
          "lesson_assignment_student_id",
          "lesson_assignment_class_id",
          "lesson_assignment_lo_id",
          "lesson_assignment_teacher_id",
          "lesson_assignment_assign_status",
          "lesson_assignment_type"
        )

        testSinkBySinkName(sinks, ParquetLessonStudentSource, expectedPqCols, 10)

        testSinkBySinkName(sinks, RedshiftStudentLessonAssignmentSink, expectedRsCols, 10)
        testSinkBySinkName(sinks, RedshiftClassLessonAssignmentSink, expectedRsCols, 8)

        testSinkBySinkName(sinks, DeltaLessonStudentAssignmentSink, expectedDeltaCols, 10)
        testSinkBySinkName(sinks, DeltaLessonClassAssignmentSink, expectedDeltaCols, 8)
      }
    )

  }
}
