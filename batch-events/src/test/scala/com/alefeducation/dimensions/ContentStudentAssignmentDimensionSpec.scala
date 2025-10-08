package com.alefeducation.dimensions

import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.util.Helpers._

class ContentStudentAssignmentDimensionSpec extends SparkSuite with BaseDimensionSpec {

  test("Test Attached/Detached events") {
    implicit val transformer = new ContentStudentAssignmentDimension(ContentStudentAssignmentDimensionName, spark)

    val fixtures = List(
      SparkFixture(
        key = ParquetStudentContentSource,
        value =
          """
            |[
            |{"eventType":"ContentAssignedEvent","loadtime":"2020-05-27T10:28:57.011Z",  "classId":"class1","contentId":33336,"contentType": "TEQ_1","mloId":"mlo-33336","studentId":"student1","assignedBy":"teacher-16","occurredOn":"2020-05-27 10:28:56.910","eventDateDw":"20200527"},
            |{"eventType":"ContentAssignedEvent","loadtime":"2020-05-27T10:28:57.011Z",  "classId":"class1","contentId":33336,"contentType": "SA","mloId":"mlo-33336","studentId":"student1","assignedBy":"teacher-16","occurredOn":"2020-05-27 10:28:56.910","eventDateDw":"20200527"},
            |{"eventType":"ContentAssignedEvent","loadtime":"2020-05-28T07:18:23.749Z",  "classId":"class1","contentId":33337,"contentType": "TEQ_1","mloId":"mlo-33337","studentId":"student2","assignedBy":"teacher-17","occurredOn":"2020-05-28 07:18:23.735","eventDateDw":"20200528"},
            |{"eventType":"ContentAssignedEvent","loadtime":"2020-05-28T07:18:23.749Z",  "classId":"class1","contentId":33336,"contentType": "TEQ_1","mloId":"mlo-33337","studentId":"student3","assignedBy":"teacher-17","occurredOn":"2020-05-28 07:18:23.735","eventDateDw":"20200528"},
            |{"eventType":"ContentUnAssignedEvent","loadtime":"2020-05-28T07:18:23.749Z","classId":"class1","contentId":33336,"contentType": "TEQ_1","mloId":"mlo-33337","studentId":"student2","assignedBy":"teacher-17","occurredOn":"2020-05-28 07:18:25.735","eventDateDw":"20200528"},
            |{"eventType":"ContentUnAssignedEvent","loadtime":"2020-05-28T07:18:41.414Z","classId":"class1","contentId":33336,"contentType": "TEQ_1","mloId":"mlo-33337","studentId":"student3","assignedBy":"teacher-17","occurredOn":"2020-05-28 07:18:41.408","eventDateDw":"20200528"},
            |{"eventType":"ContentAssignedEvent","loadtime":"2020-05-28T07:19:28.144Z",  "classId":"class1","contentId":33338,"contentType": "TEQ_1","mloId":"mlo-33338","studentId":"student1","assignedBy":"teacher-18","occurredOn":"2020-05-28 07:19:28.138","eventDateDw":"20200528"},
            |{"eventType":"ContentAssignedEvent","loadtime":"2020-05-28T07:19:28.144Z",  "classId":"class1","contentId":33339,"contentType": "TEQ_1","mloId":"mlo-33339","studentId":"student2","assignedBy":"teacher-19","occurredOn":"2020-05-28 07:19:28.138","eventDateDw":"20200528"},
            |{"eventType":"ContentAssignedEvent","loadtime":"2020-05-28T07:19:28.145Z",  "classId":"class1","contentId":33340,"contentType": "TEQ_1","mloId":"mlo-33340","studentId":"student3","assignedBy":"teacher-10","occurredOn":"2020-05-28 07:19:28.138","eventDateDw":"20200528"},
            |{"eventType":"ContentAssignedEvent","loadtime":"2020-05-28T07:19:28.145Z",  "classId":"class1","contentId":33338,"contentType": "TEQ_1","mloId":"mlo-33338","studentId":"student3","assignedBy":"teacher-18","occurredOn":"2020-05-28 07:19:28.138","eventDateDw":"20200528"},
            |{"eventType":"ContentUnAssignedEvent","loadtime":"2020-05-28T07:19:38.456Z","classId":"class1","contentId":33338,"contentType": "TEQ_1","mloId":"mlo-33338","studentId":"student3","assignedBy":"teacher-18","occurredOn":"2020-05-28 07:19:38.454","eventDateDw":"20200528"}
            |]
            |""".stripMargin
      ),
      SparkFixture(
        key = ParquetContentClassSource,
        value =
          """
            |[
            |{"eventType":"ClassContentAssignedEvent","loadtime":"2020-05-27T10:28:57.011Z",  "classId":"class1","contentId":33336,"contentType": "TEQ_1","mloId":"mlo-33336","assignedBy":"teacher-16","occurredOn":"2020-05-27 10:28:56.910","eventDateDw":"20200527"},
            |{"eventType":"ClassContentAssignedEvent","loadtime":"2020-05-28T07:18:23.749Z",  "classId":"class1","contentId":33336,"contentType": "TEQ_1","mloId":"mlo-33337","assignedBy":"teacher-17","occurredOn":"2020-05-28 07:18:23.735","eventDateDw":"20200528"},
            |{"eventType":"ClassContentAssignedEvent","loadtime":"2020-05-28T07:18:23.749Z",  "classId":"class1","contentId":33336,"contentType": "TEQ_1","mloId":"mlo-33337","assignedBy":"teacher-17","occurredOn":"2020-05-28 07:18:23.735","eventDateDw":"20200528"},
            |{"eventType":"ClassContentUnAssignedEvent","loadtime":"2020-05-28T07:18:23.749Z","classId":"class1","contentId":33336,"contentType": "TEQ_1","mloId":"mlo-33337","assignedBy":"teacher-17","occurredOn":"2020-05-28 07:18:25.735","eventDateDw":"20200528"},
            |{"eventType":"ClassContentUnAssignedEvent","loadtime":"2020-05-28T07:18:41.414Z","classId":"class1","contentId":33336,"contentType": "TEQ_1","mloId":"mlo-33337","assignedBy":"teacher-17","occurredOn":"2020-05-28 07:18:41.408","eventDateDw":"20200528"},
            |{"eventType":"ClassContentAssignedEvent","loadtime":"2020-05-28T07:19:28.144Z",  "classId":"class1","contentId":33338,"contentType": "TEQ_1","mloId":"mlo-33338","assignedBy":"teacher-18","occurredOn":"2020-05-28 07:19:28.138","eventDateDw":"20200528"},
            |{"eventType":"ClassContentAssignedEvent","loadtime":"2020-05-28T07:19:28.144Z",  "classId":"class1","contentId":33339,"contentType": "TEQ_1","mloId":"mlo-33339","assignedBy":"teacher-19","occurredOn":"2020-05-28 07:19:28.138","eventDateDw":"20200528"},
            |{"eventType":"ClassContentAssignedEvent","loadtime":"2020-05-28T07:19:28.145Z",  "classId":"class1","contentId":33340,"contentType": "TEQ_1","mloId":"mlo-33340","assignedBy":"teacher-10","occurredOn":"2020-05-28 07:19:28.138","eventDateDw":"20200528"},
            |{"eventType":"ClassContentAssignedEvent","loadtime":"2020-05-28T07:19:28.145Z",  "classId":"class1","contentId":33338,"contentType": "TEQ_1","mloId":"mlo-33338","assignedBy":"teacher-18","occurredOn":"2020-05-28 07:19:28.138","eventDateDw":"20200528"},
            |{"eventType":"ClassContentUnAssignedEvent","loadtime":"2020-05-28T07:19:38.456Z","classId":"class1","contentId":33338,"contentType": "TEQ_1","mloId":"mlo-33338","assignedBy":"teacher-18","occurredOn":"2020-05-28 07:19:38.454","eventDateDw":"20200528"}
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
          "studentId",
          "classId",
          "mloId",
          "assignedBy",
          "contentType",
          "eventdate"
        )
        val expectedRsCols = Seq(
          "content_student_association_created_time",
          "content_student_association_updated_time",
          "content_student_association_dw_created_time",
          "content_student_association_dw_updated_time",
          "content_student_association_status",
          "content_student_association_step_id",
          "content_student_association_student_id",
          "content_student_association_class_id",
          "content_student_association_lo_id",
          "content_student_association_assign_status",
          "content_student_association_assigned_by",
          "content_student_association_content_type",
          "content_student_association_type"
        )

        testSinkBySinkName(sinks, ParquetStudentContentSource, expectedPqCols, 11)

        testSinkBySinkName(sinks, RedshiftStudentContentAssignmentSink, expectedRsCols, 8)
        testSinkBySinkName(sinks, RedshiftStudentContentUnassignmentSink, expectedRsCols, 3)

        testSinkBySinkName(sinks, RedshiftClassContentAssignmentSink, expectedRsCols, 5)
        testSinkBySinkName(sinks, RedshiftClassContentUnAssignmentSink, expectedRsCols, 3)

        testSinkBySinkName(sinks, DeltaContentStudentAssignmentSink, expectedRsCols, 11)
        testSinkBySinkName(sinks, DeltaContentClassAssignmentSink, expectedRsCols, 8)
      }
    )

  }
}
