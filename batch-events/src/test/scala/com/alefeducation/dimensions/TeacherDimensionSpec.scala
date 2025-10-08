package com.alefeducation.dimensions

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.dimensions.teacher._
import com.alefeducation.models.TeacherModel.{SchoolDim, TeacherDim, TeacherRel}
import com.alefeducation.util.Helpers._
import com.alefeducation.util.Resources
import org.mockito.Mockito.when
import org.scalatest.matchers.must.Matchers
import org.scalatestplus.mockito.MockitoSugar.mock

class TeacherDimensionSpec extends SparkSuite with Matchers {

  val service = mock[SparkBatchService]

  val teacherDimExpectedColumns: Set[String] = dimDateCols("teacher").toSet ++ Set("teacher_id", "school_id")

  test("teacher created event when nothing is in database") {
    val sprk = spark
    import sprk.implicits._

    val teacherCreatedEvent =
      """
        |{
        |  "tenantId": "tenant-id",
        |  "eventType": "TeacherCreatedEvent",
        |  "createdOn": 1538048350311,
        |  "uuid": "teacher-id2",
        |  "enabled": true,
        |  "schoolId": "school-id",
        |  "eventDateDw": "20180928",
        |  "occurredOn": "2018-09-28 15:40:10.0",
        |  "loadtime": "2018-09-28 15:40:11.0"
        |}
      """.stripMargin

    val teacherCreated = spark.read.json(Seq(teacherCreatedEvent).toDS())
    when(service.readOptional(ParquetTeacherSource, sprk)).thenReturn(Some(teacherCreated))
    when(service.readOptional(ParquetTeacherSchoolMovedSource, sprk)).thenReturn(None)
    when(service.readFromRedshiftQuery[TeacherRel](s"select * from ${Resources.redshiftStageSchema()}.rel_teacher where teacher_id in ('teacher-id2') and teacher_active_until is null")).thenReturn(Seq.empty[TeacherRel].toDF())
    when(service.readFromRedshiftQuery[TeacherDim](s"select * from ${Resources.redshiftSchema()}.dim_teacher where teacher_id in ('teacher-id2') and teacher_active_until is null")).thenReturn(Seq.empty[TeacherDim].toDF())

    val transformer = new TeacherTransform(sprk, service)
    val result = transformer.transform().get.output

    assert(result.columns.toSet === teacherDimExpectedColumns)
    assert[String](result, "teacher_id", "teacher-id2")
    assert[String](result, "school_id", "school-id")
    assert[String](result, "teacher_created_time", "2018-09-28 15:40:10.0")
    assert[Int](result, "teacher_status", ActiveEnabled)
  }

  test("teacher enabled disabled event") {
    val sprk = spark
    import sprk.implicits._
    val teacherEnabledDisabledEvent =
      """
        |[{
        |  "tenantId": "tenant-id",
        |  "eventType": "TeacherDisabledEvent",
        |  "createdOn": 1538048350311,
        |  "uuid": "teacher-id2",
        |  "enabled": false,
        |  "schoolId": "school-id",
        |  "eventDateDw": "20190302",
        |  "occurredOn": "2019-03-02 15:40:10.0",
        |  "loadtime": "2019-03-02 15:40:10.1"
        |},
        |{
        |  "tenantId": "tenant-id",
        |  "eventType": "TeacherEnabledEvent",
        |  "createdOn": 1538048350311,
        |  "uuid": "teacher-id1",
        |  "enabled": false,
        |  "schoolId": "school-id",
        |  "eventDateDw": "20190302",
        |  "occurredOn": "2019-03-02 15:41:10.0",
        |  "loadtime": "2019-03-02 15:41:10.2"
        |}]
      """.stripMargin
    val teacherEnabledDisabled = spark.read.json(Seq(teacherEnabledDisabledEvent).toDS())
    when(service.readOptional(ParquetTeacherSource, sprk)).thenReturn(Some(teacherEnabledDisabled))

    val schoolDim =
      """
        |[{
        |   "school_status":1,
        |   "school_dw_id" : 1,
        |   "school_id":"school-id",
        |   "school_created_time": "2019-01-09 02:30:00.0"
        |}
        |]
      """.stripMargin
    val schoolDimDf = spark.read.json(Seq(schoolDim).toDS())
    when(service.readFromRedshiftQuery[SchoolDim](s"select * from ${Resources.redshiftSchema()}.dim_school where school_dw_id in (1)")).thenReturn(schoolDimDf)

    val teacherDim =
      """
        |[{
        |   "teacher_id":"teacher-id2",
        |   "teacher_created_time": "2019-02-09 02:30:00.0",
        |   "teacher_school_dw_id" : 1,
        |   "teacher_subject_dw_id": null,
        |   "teacher_active_until": null,
        |   "teacher_status": 1
        |},
        |{
        |   "teacher_id":"teacher-id1",
        |   "teacher_created_time": "2019-02-09 02:30:00.0",
        |   "teacher_school_dw_id" : 1,
        |   "teacher_subject_dw_id": null,
        |   "teacher_active_until": null,
        |   "teacher_status": 3
        |}]
      """.stripMargin

    val teacherDimDf = spark.read.json(Seq(teacherDim).toDS())
    when(service.readFromRedshiftQuery[TeacherDim](s"select * from ${Resources.redshiftSchema()}.dim_teacher where teacher_id in ('teacher-id2','teacher-id1') and teacher_active_until is null")).thenReturn(teacherDimDf)

    when(service.readFromRedshiftQuery[TeacherRel](s"select * from ${Resources.redshiftStageSchema()}.rel_teacher where teacher_id in ('teacher-id2','teacher-id1') and teacher_active_until is null")).thenReturn(Seq.empty[TeacherRel].toDF())
    when(service.readOptional(ParquetTeacherSchoolMovedSource, sprk)).thenReturn(None)

    val transformer = new TeacherTransform(sprk, service)
    val result = transformer.transform().get.output

    assert(result.columns.toSet === teacherDimExpectedColumns)
    assert(result.count() == 2)

    val teacherDisabled = result.filter("teacher_id = 'teacher-id2'")
    assert[Int](teacherDisabled, "teacher_status", Disabled)
    assert[String](teacherDisabled, "teacher_created_time", "2019-03-02 15:40:10.0")

    val teacherEnabled = result.filter("teacher_id = 'teacher-id1'")
    assert[Int](teacherEnabled, "teacher_status", ActiveEnabled)
    assert[String](teacherEnabled, "teacher_created_time", "2019-03-02 15:41:10.0")
  }

  test("teacher enabled disabled event for same teacher") {
    val sprk = spark
    import sprk.implicits._
    val teacherEnabledDisabledEvent =
      """
        |[{
        |  "tenantId": "tenant-id",
        |  "eventType": "TeacherDisabledEvent",
        |  "createdOn": 1538048350311,
        |  "uuid": "teacher-id2",
        |  "enabled": false,
        |  "schoolId": "school-id",
        |  "eventDateDw": "20190302",
        |  "occurredOn": "2019-03-02 15:40:10.0",
        |  "loadtime": "2019-03-02 15:40:10.1"
        |},{
        |  "tenantId": "tenant-id",
        |  "eventType": "TeacherEnabledEvent",
        |  "createdOn": 1538048350311,
        |  "uuid": "teacher-id2",
        |  "enabled": false,
        |  "schoolId": "school-id",
        |  "eventDateDw": "20190302",
        |  "occurredOn": "2019-03-02 15:41:10.0",
        |  "loadtime": "2019-03-02 15:41:10.1"
        |}]
        """.stripMargin
    val teacherEnabledDisabled = spark.read.json(Seq(teacherEnabledDisabledEvent).toDS())
    when(service.readOptional(ParquetTeacherSource, sprk)).thenReturn(Some(teacherEnabledDisabled))

    when(service.readOptional(ParquetTeacherSchoolMovedSource, sprk)).thenReturn(None)

    when(service.readFromRedshiftQuery[TeacherRel](s"select * from ${Resources.redshiftStageSchema()}.rel_teacher where teacher_id in ('teacher-id2') and teacher_active_until is null")).thenReturn(Seq.empty[TeacherRel].toDF())

    val teacherDim =
      """
        |[{
        |   "teacher_id":"teacher-id2",
        |   "teacher_created_time": "2019-02-09 02:30:00.0",
        |   "teacher_school_dw_id" : 1,
        |   "teacher_subject_dw_id":1,
        |   "teacher_active_until": null,
        |   "teacher_status": 1
        |},
        |{
        |   "teacher_id":"teacher-id1",
        |   "teacher_created_time": "2019-02-09 02:30:00.0",
        |   "teacher_school_dw_id" : 1,
        |   "teacher_subject_dw_id": null,
        |   "teacher_active_until": null,
        |   "teacher_status": 3
        |}]
        """.stripMargin
    val teacherDimDf = spark.read.json(Seq(teacherDim).toDS())
    when(service.readFromRedshiftQuery[TeacherDim](s"select * from ${Resources.redshiftSchema()}.dim_teacher where teacher_id in ('teacher-id2') and teacher_active_until is null")).thenReturn(teacherDimDf)


    val schoolDim =
      """
        |[{
        |   "school_status":1,
        |   "school_dw_id" : 1,
        |   "school_id":"school-id",
        |   "school_created_time": "2019-01-09 02:30:00.0"
        |}
        |]
        """.stripMargin
    val schoolDimDf = spark.read.json(Seq(schoolDim).toDS())
    when(service.readFromRedshiftQuery[SchoolDim](s"select * from ${Resources.redshiftSchema()}.dim_school where school_dw_id in (1)")).thenReturn(schoolDimDf)

    val transformer = new TeacherTransform(sprk, service)
    val teacher = transformer.transform().get.output

    assert(teacher.count() == 1)

    assert[Int](teacher, "teacher_status", ActiveEnabled)
    assert[String](teacher, "teacher_created_time", "2019-03-02 15:41:10.0")
  }

  test("teacher enabled disabled event for same teacher when teacher is in rel") {
    val sprk = spark
    import sprk.implicits._
    val teacherEnabledDisabledEvent =
      """
        |[{
        |  "tenantId": "tenant-id",
        |  "eventType": "TeacherDisabledEvent",
        |  "createdOn": 1538048350311,
        |  "uuid": "teacher-id2",
        |  "enabled": false,
        |  "schoolId": "school-id",
        |  "eventDateDw": "20190302",
        |  "occurredOn": "2019-03-02 15:40:10.0",
        |  "loadtime": "2019-03-02 15:40:10.1"
        |},{
        |  "tenantId": "tenant-id",
        |  "eventType": "TeacherEnabledEvent",
        |  "createdOn": 1538048350311,
        |  "uuid": "teacher-id2",
        |  "enabled": false,
        |  "schoolId": "school-id",
        |  "eventDateDw": "20190302",
        |  "occurredOn": "2019-03-02 15:41:10.0",
        |  "loadtime": "2019-03-02 15:41:10.1"
        |}]
        """.stripMargin
    val teacherEnabledDisabled = spark.read.json(Seq(teacherEnabledDisabledEvent).toDS())
    when(service.readOptional(ParquetTeacherSource, sprk)).thenReturn(Some(teacherEnabledDisabled))

    when(service.readOptional(ParquetTeacherSchoolMovedSource, sprk)).thenReturn(None)

    val teacherRel =
      """
        |[{
        |   "teacher_id":"teacher-id2",
        |   "teacher_created_time": "2019-02-09 02:30:00.0",
        |   "school_id" : "school-id",
        |   "subject_id": null,
        |   "teacher_active_until": null,
        |   "teacher_status": 1
        |},
        |{
        |   "teacher_id":"teacher-id1",
        |   "teacher_created_time": "2019-02-09 02:30:00.0",
        |   "school_id" : "school-id",
        |   "subject_id":  null,
        |   "teacher_active_until": null,
        |   "teacher_status": 3
        |}]
        """.stripMargin
    val teacherRelDf = spark.read.json(Seq(teacherRel).toDS())
    when(service.readFromRedshiftQuery[TeacherRel](s"select * from ${Resources.redshiftStageSchema()}.rel_teacher where teacher_id in ('teacher-id2') and teacher_active_until is null")).thenReturn(teacherRelDf)

    when(service.readFromRedshiftQuery[TeacherDim](s"select * from ${Resources.redshiftSchema()}.dim_teacher where teacher_id in ('teacher-id2') and teacher_active_until is null")).thenReturn(Seq.empty[TeacherDim].toDF())

    val transformer = new TeacherTransform(sprk, service)
    val teacher = transformer.transform().get.output

    assert(teacher.count() == 1)

    assert[Int](teacher, "teacher_status", ActiveEnabled)
    assert[String](teacher, "teacher_created_time", "2019-03-02 15:41:10.0")
  }

  test("teacher move schools event") {
    val sprk = spark
    import sprk.implicits._

    when(service.readOptional(ParquetTeacherSource, sprk)).thenReturn(None)

    val schoolMovedEvent =
      """
        |[{
        |  "tenantId": "tenant-id",
        |  "eventType": "TeacherMovedBetweenSchools",
        |  "createdOn": 1538048350311,
        |  "teacherId": "teacher-id1",
        |  "sourceSchoolId": "old-school_id",
        |  "targetSchoolId": "new_school",
        |  "eventDateDw": "20190210",
        |  "occurredOn": "2019-02-10 02:30:00.0"
        |}
        |]
        """.stripMargin
    val schoolMovedDf = spark.read.json(Seq(schoolMovedEvent).toDS())
    when(service.readOptional(ParquetTeacherSchoolMovedSource, sprk)).thenReturn(Some(schoolMovedDf))

    when(service.readFromRedshiftQuery[TeacherRel](s"select * from ${Resources.redshiftStageSchema()}.rel_teacher where teacher_id in ('teacher-id1') and teacher_active_until is null")).thenReturn(Seq.empty[TeacherRel].toDF())

    val teacherDim =
      """
        |[{
        |   "teacher_status":1,
        |   "teacher_id":"teacher-id1",
        |   "teacher_created_time": "2019-02-01 02:30:00.0",
        |   "teacher_school_dw_id" :1,
        |   "teacher_subject_dw_id": null,
        |   "teacher_enabled":true,
        |   "teacher_active_until": null
        |}
        |]
        """.stripMargin
    val teacherDimDf = spark.read.json(Seq(teacherDim).toDS())
    when(service.readFromRedshiftQuery[TeacherDim](s"select * from ${Resources.redshiftSchema()}.dim_teacher where teacher_id in ('teacher-id1') and teacher_active_until is null")).thenReturn(teacherDimDf)

    val schoolDim =
      """
        |[{
        |   "school_status":1,
        |   "school_dw_id" : 1,
        |   "school_id":"old-school_id",
        |   "school_created_time": "2019-01-01 02:30:00.0"
        |}
        |]
        """.stripMargin
    val schoolDimDf = spark.read.json(Seq(schoolDim).toDS())
    when(service.readFromRedshiftQuery[SchoolDim](s"select * from ${Resources.redshiftSchema()}.dim_school where school_dw_id in (1)")).thenReturn(schoolDimDf)


    val transformer = new TeacherTransform(sprk, service)
    val teacherRSDf = transformer.transform().get.output

    assert(teacherRSDf.columns.toSet === teacherDimExpectedColumns)

    assert[String](teacherRSDf, "teacher_id", "teacher-id1")
    assert[String](teacherRSDf, "school_id", "new_school")
    assert[Int](teacherRSDf, "teacher_status", ActiveEnabled)
  }


  test("teacher created and enabled/disabled event") {
    val sprk = spark
    import sprk.implicits._
    val teacherEvents =
      """
        |[{
        |  "tenantId": "tenant-id",
        |  "eventType": "TeacherCreatedEvent",
        |  "createdOn": 1538048350311,
        |  "uuid": "teacher-id2",
        |  "enabled": true,
        |  "schoolId": "school-id",
        |  "eventDateDw": "20180928",
        |  "occurredOn": "2018-09-28 15:40:10.0",
        |  "loadtime": "2018-09-28 15:40:10.1"
        |},
        |{
        |  "tenantId": "tenant-id",
        |  "eventType": "TeacherDisabledEvent",
        |  "createdOn": 1538048350311,
        |  "uuid": "teacher-id2",
        |  "enabled": false,
        |  "schoolId": "school-id",
        |  "eventDateDw": "20190302",
        |  "occurredOn": "2019-03-02 15:40:10.0",
        |  "loadtime": "2019-03-02 15:40:10.2"
        |},
        |{
        |  "tenantId": "tenant-id",
        |  "eventType": "TeacherEnabledEvent",
        |  "createdOn": 1538048350311,
        |  "uuid": "teacher-id1",
        |  "enabled": false,
        |  "schoolId": "school-id",
        |  "eventDateDw": "20190302",
        |  "occurredOn": "2019-03-02 15:41:10.0",
        |  "loadtime": "2019-03-02 15:41:10.3"
        |}]
        """.stripMargin

    val teacherEventDf = spark.read.json(Seq(teacherEvents).toDS())
    when(service.readOptional(ParquetTeacherSource, sprk)).thenReturn(Some(teacherEventDf))

    when(service.readOptional(ParquetTeacherSchoolMovedSource, sprk)).thenReturn(None)
    when(service.readFromRedshiftQuery[TeacherRel](s"select * from ${Resources.redshiftStageSchema()}.rel_teacher where teacher_id in ('teacher-id2','teacher-id1') and teacher_active_until is null")).thenReturn(Seq.empty[TeacherRel].toDF())

    val teacherDim =
      """
        |[
        |{
        |   "teacher_id":"teacher-id1",
        |   "teacher_created_time": "2019-02-09 02:30:00.0",
        |   "teacher_active_until": null,
        |   "teacher_school_dw_id" : 1,
        |   "teacher_subject_dw_id": null,
        |   "teacher_status": 3
        |}]
        """.stripMargin
    val teacherDimDf = spark.read.json(Seq(teacherDim).toDS())
    when(service.readFromRedshiftQuery[TeacherDim](s"select * from ${Resources.redshiftSchema()}.dim_teacher where teacher_id in ('teacher-id2','teacher-id1') and teacher_active_until is null")).thenReturn(teacherDimDf)

    val schoolDim =
      """
        |[{
        |   "school_status":1,
        |   "school_dw_id" : 1,
        |   "school_id":"school-id",
        |   "school_created_time": "2019-01-09 02:30:00.0"
        |}
        |]
        """.stripMargin
    val schoolDimDf = spark.read.json(Seq(schoolDim).toDS())
    when(service.readFromRedshiftQuery[SchoolDim](s"select * from ${Resources.redshiftSchema()}.dim_school where school_dw_id in (1)")).thenReturn(schoolDimDf)

    val transformer = new TeacherTransform(sprk, service)
    val teacherRSDf = transformer.transform().get.output

    assert(teacherRSDf.count == 2)

    val teacherDisabled = teacherRSDf.filter("teacher_id = 'teacher-id2'")
    assert[Int](teacherDisabled, "teacher_status", Disabled)
    assert[String](teacherDisabled, "teacher_created_time", "2019-03-02 15:40:10.0")

    val teacherEnabled = teacherRSDf.filter("teacher_id = 'teacher-id1'")
    assert[Int](teacherEnabled, "teacher_status", ActiveEnabled)
    assert[String](teacherEnabled, "teacher_created_time", "2019-03-02 15:41:10.0")
  }


  test("teacher created and school moved event") {
    val sprk = spark
    import sprk.implicits._

    val teacherMutatedEvents =
      """
        |[{
        |  "tenantId": "tenant-id",
        |  "eventType": "TeacherCreatedEvent",
        |  "createdOn": 1538048350311,
        |  "uuid": "teacher-id1",
        |  "enabled": true,
        |  "schoolId": "old-school_id",
        |  "subjectIds": ["subject-1"],
        |  "eventDateDw": "20180928",
        |  "occurredOn": "2018-09-28 15:40:10.0",
        |  "loadtime": "2018-09-28 15:40:10.1"
        |}]
        """.stripMargin
    val teacherMutated = spark.read.json(Seq(teacherMutatedEvents).toDS())
    when(service.readOptional(ParquetTeacherSource, sprk)).thenReturn(Some(teacherMutated))

    val teacherSchoolMovedEvent =
      """
        |[{
        |  "tenantId": "tenant-id",
        |  "eventType": "TeacherMovedBetweenSchools",
        |  "createdOn": 1538048350311,
        |  "teacherId": "teacher-id1",
        |  "sourceSchoolId": "old-school_id",
        |  "targetSchoolId": "new_school",
        |  "eventDateDw": "20190303",
        |  "occurredOn": "2019-03-03 15:41:10.0",
        |  "loadtime": "2019-03-03 15:41:10.1"
        |}
        |]
        """.stripMargin
    val teacherSchoolMoved = spark.read.json(Seq(teacherSchoolMovedEvent).toDS())
    when(service.readOptional(ParquetTeacherSchoolMovedSource, sprk)).thenReturn(Some(teacherSchoolMoved))

    when(service.readFromRedshiftQuery[TeacherRel](s"select * from ${Resources.redshiftStageSchema()}.rel_teacher where teacher_id in ('teacher-id1') and teacher_active_until is null")).thenReturn(Seq.empty[TeacherRel].toDF())
    when(service.readFromRedshiftQuery[TeacherDim](s"select * from ${Resources.redshiftSchema()}.dim_teacher where teacher_id in ('teacher-id1') and teacher_active_until is null")).thenReturn(Seq.empty[TeacherDim].toDF())

    val transformer = new TeacherTransform(sprk, service)
    val teacherRSDf = transformer.transform().get.output

    assert(teacherRSDf.columns.toSet === teacherDimExpectedColumns)

    val teacherDisabled = teacherRSDf.filter("teacher_id = 'teacher-id1'")
    assert(teacherRSDf.count === 1)
    assert[String](teacherDisabled, "school_id", "new_school")
    assert[Int](teacherDisabled, "teacher_status", ActiveEnabled)
    assert[String](teacherDisabled, "teacher_created_time", "2019-03-03 15:41:10.0")
  }

  test("teacher disabled and school moved events") {
    val sprk = spark
    import sprk.implicits._

    val teacherMutatedEvents =
      """
        |[{
        |  "tenantId": "tenant-id",
        |  "eventType": "TeacherDisabledEvent",
        |  "createdOn": 1538048350311,
        |  "uuid": "teacher-id1",
        |  "enabled": false,
        |  "schoolId": "old-school_id",
        |  "eventDateDw": "20180928",
        |  "occurredOn": "2019-02-28 15:40:10.0",
        |  "loadtime": "2019-02-28 15:40:10.1"
        |}]
        """.stripMargin

    val teacherMutatedDf = spark.read.json(Seq(teacherMutatedEvents).toDS())
    when(service.readOptional(ParquetTeacherSource, sprk)).thenReturn(Some(teacherMutatedDf))

    val teacherSchoolMovedEvent =
      """
        |[{
        |  "tenantId": "tenant-id",
        |  "eventType": "TeacherMovedBetweenSchools",
        |  "createdOn": 1538048350311,
        |  "teacherId": "teacher-id1",
        |  "sourceSchoolId": "old-school_id",
        |  "targetSchoolId": "new_school",
        |  "eventDateDw": "20180928",
        |  "occurredOn": "2019-03-03 15:43:00.0",
        |  "loadtime": "2019-03-03 15:43:00.1"
        |}
        |]
        """.stripMargin
    val teacherSchoolMoved = spark.read.json(Seq(teacherSchoolMovedEvent).toDS())
    when(service.readOptional(ParquetTeacherSchoolMovedSource, sprk)).thenReturn(Some(teacherSchoolMoved))

    when(service.readFromRedshiftQuery[TeacherRel](s"select * from ${Resources.redshiftStageSchema()}.rel_teacher where teacher_id in ('teacher-id1') and teacher_active_until is null")).thenReturn(Seq.empty[TeacherRel].toDF())

    val teacherDim =
      """
        |[{
        |   "teacher_status":1,
        |   "teacher_id":"teacher-id1",
        |   "teacher_created_time": "2019-01-09 02:30:00.0",
        |   "teacher_school_dw_id" : 1,
        |   "teacher_subject_dw_id": null,
        |   "teacher_enabled":true,
        |   "teacher_active_until": null
        |}
        |]
        """.stripMargin
    val teacherDimDf = spark.read.json(Seq(teacherDim).toDS())
    when(service.readFromRedshiftQuery[TeacherDim](s"select * from ${Resources.redshiftSchema()}.dim_teacher where teacher_id in ('teacher-id1') and teacher_active_until is null")).thenReturn(teacherDimDf)

    val schoolDim =
      """
        |[{
        |   "school_status":1,
        |   "school_dw_id" : 1,
        |   "school_id":"old-school_id",
        |   "school_created_time": "2019-01-09 02:30:00.0"
        |}
        |]
        """.stripMargin
    val schoolDimDf = spark.read.json(Seq(schoolDim).toDS())
    when(service.readFromRedshiftQuery[SchoolDim](s"select * from ${Resources.redshiftSchema()}.dim_school where school_dw_id in (1)")).thenReturn(schoolDimDf)


    val transformer = new TeacherTransform(sprk, service)
    val teacherRSDf = transformer.transform().get.output
    assert(teacherRSDf.columns.toSet === teacherDimExpectedColumns)

    val teacherDisabled = teacherRSDf.filter("teacher_id = 'teacher-id1'")
    assert[String](teacherDisabled, "school_id", "new_school")
    assert[Int](teacherDisabled, "teacher_status", Disabled)
    assert[String](teacherDisabled, "teacher_created_time", "2019-03-03 15:43:00.0")

  }

  test("teacher created, enable/disable and school moved events") {
    val sprk = spark
    import sprk.implicits._
    val teacherMutatedEvent =
      """
        |[{
        |  "tenantId": "tenant-id",
        |  "eventType": "TeacherCreatedEvent",
        |  "createdOn": 1538048350311,
        |  "uuid": "teacher-id1",
        |  "enabled": true,
        |  "schoolId": "school-id",
        |  "eventDateDw": "20180928",
        |  "occurredOn": "2018-09-28 15:40:10.0",
        |  "loadtime": "2018-09-28 15:40:10.1"
        |},{
        |  "tenantId": "tenant-id",
        |  "eventType": "TeacherDisabledEvent",
        |  "createdOn": 1538048350311,
        |  "uuid": "teacher-id1",
        |  "enabled": false,
        |  "schoolId": "school-id",
        |  "eventDateDw": "20190302",
        |  "occurredOn": "2019-03-02 15:40:10.0",
        |  "loadtime": "2019-03-02 15:40:10.2"
        |}]
      """.stripMargin
    val teacherMutatedDf = spark.read.json(Seq(teacherMutatedEvent).toDS())
    when(service.readOptional(ParquetTeacherSource, sprk)).thenReturn(Some(teacherMutatedDf))

    val teacherSchoolMovedEvent =
      """
        |[{
        |  "tenantId": "tenant-id",
        |  "eventType": "TeacherMovedBetweenSchools",
        |  "createdOn": 1538048350311,
        |  "teacherId": "teacher-id1",
        |  "sourceSchoolId": "school-id",
        |  "targetSchoolId": "new-school-id",
        |  "eventDateDw": "20190210",
        |  "occurredOn": "2019-02-10 02:30:00.0",
        |  "loadtime": "2019-02-10 02:30:00.1"
        |}
        |]
      """.stripMargin
    val teacherSchoolMovedDf = spark.read.json(Seq(teacherSchoolMovedEvent).toDS())
    when(service.readOptional(ParquetTeacherSchoolMovedSource, sprk)).thenReturn(Some(teacherSchoolMovedDf))

    when(service.readFromRedshiftQuery[TeacherRel](s"select * from ${Resources.redshiftStageSchema()}.rel_teacher where teacher_id in ('teacher-id1') and teacher_active_until is null")).thenReturn(Seq.empty[TeacherRel].toDF())
    when(service.readFromRedshiftQuery[TeacherDim](s"select * from ${Resources.redshiftSchema()}.dim_teacher where teacher_id in ('teacher-id1') and teacher_active_until is null")).thenReturn(Seq.empty[TeacherDim].toDF())

    val transformer = new TeacherTransform(sprk, service)
    val teacherRSDf = transformer.transform().get.output
    assert(teacherRSDf.columns.toSet === teacherDimExpectedColumns)
    assert[String](teacherRSDf, "school_id", "new-school-id")
    assert[Int](teacherRSDf, "teacher_status", Disabled)
    assert[String](teacherRSDf, "teacher_created_time", "2019-03-02 15:40:10.0")
  }

  test("all teacher Events") {
    val sprk = spark
    import sprk.implicits._

    val teacherMutatedEvents =
      """
        |[{
        |  "tenantId": "tenant-id",
        |  "eventType": "TeacherCreatedEvent",
        |  "createdOn": 1538048350311,
        |  "uuid": "teacher-created-id1",
        |  "enabled": true,
        |  "schoolId": "school-id",
        |  "eventDateDw": "20180928",
        |  "occurredOn": "2018-09-28 15:40:10.0",
        |  "loadtime": "2018-09-28 15:40:10.1"
        |},
        |{
        |  "tenantId": "tenant-id",
        |  "eventType": "TeacherCreatedEvent",
        |  "createdOn": 1538048350311,
        |  "uuid": "teacher-created-id2",
        |  "enabled": true,
        |  "schoolId": "school-id",
        |  "eventDateDw": "20180928",
        |  "occurredOn": "2018-09-29 15:40:10.0",
        |  "loadtime": "2018-09-29 15:40:10.1"
        |},
        |{
        |  "tenantId": "tenant-id",
        |  "eventType": "TeacherDisabledEvent",
        |  "createdOn": 1538048350312,
        |  "uuid": "teacher-created-id2",
        |  "enabled": false,
        |  "schoolId": "school-id",
        |  "eventDateDw": "20180930",
        |  "occurredOn": "2018-09-30 15:40:10.0",
        |  "loadtime": "2018-09-30 15:40:10.1"
        |},
        |{
        |  "tenantId": "tenant-id",
        |  "eventType": "TeacherEnabledEvent",
        |  "createdOn": 1538048350322,
        |  "uuid": "teacher-created-id2",
        |  "enabled": true,
        |  "schoolId": "school-id",
        |  "eventDateDw": "20180930",
        |  "occurredOn": "2018-09-30 21:40:10.0",
        |  "loadtime": "2018-09-30 21:40:10.1"
        |},
        |{
        |  "tenantId": "tenant-id",
        |  "eventType": "TeacherEnabledEvent",
        |  "createdOn": 1538048350322,
        |  "uuid": "teacher-id-4",
        |  "enabled": true,
        |  "schoolId": "school-id",
        |  "eventDateDw": "20180930",
        |  "occurredOn": "2018-09-30 22:40:10.0",
        |  "loadtime": "2018-09-30 22:40:10.1"
        |},
        |{
        |  "tenantId": "tenant-id",
        |  "eventType": "TeacherDisabledEvent",
        |  "createdOn": 1538048350322,
        |  "uuid": "teacher-id-3",
        |  "enabled": false,
        |  "schoolId": "school-id",
        |  "eventDateDw": "20180930",
        |  "occurredOn": "2018-09-30 22:45:10.0",
        |  "loadtime": "2018-09-30 22:45:10.1"
        |}
        |]
        """.stripMargin
    val teacherMutatedDf = spark.read.json(Seq(teacherMutatedEvents).toDS())
    when(service.readOptional(ParquetTeacherSource, sprk)).thenReturn(Some(teacherMutatedDf))

    val teacherMovedEvent =
      """
        |[{
        |  "tenantId": "tenant-id",
        |  "eventType": "TeacherMovedBetweenSchools",
        |  "createdOn": 1538048350311,
        |  "teacherId": "teacher-created-id1",
        |  "sourceSchoolId": "school_id",
        |  "targetSchoolId": "new_school",
        |  "eventDateDw": "20181010",
        |  "occurredOn": "2018-10-10 02:30:00.0",
        |  "loadtime": "2018-10-10 02:30:00.1"
        |}
        |]
        """.stripMargin

    val teacherMoveDf = spark.read.json(Seq(teacherMovedEvent).toDS())
    when(service.readOptional(ParquetTeacherSchoolMovedSource, sprk)).thenReturn(Some(teacherMoveDf))

    when(service.readFromRedshiftQuery[TeacherRel](s"select * from ${Resources.redshiftStageSchema()}.rel_teacher where teacher_id in ('teacher-created-id1','teacher-created-id2','teacher-id-4','teacher-id-3') and teacher_active_until is null")).thenReturn(Seq.empty[TeacherRel].toDF())

    val teacherDim =
      """
        |[
        |{
        |   "teacher_status":1,
        |   "teacher_id":"teacher-id-3",
        |   "teacher_created_time": "2018-08-09 02:30:00.0",
        |   "teacher_school_dw_id" : 1,
        |   "teacher_subject_dw_id":5,
        |   "teacher_enabled":true,
        |   "teacher_active_until": null
        |},
        |{
        |   "teacher_status":3,
        |   "teacher_id":"teacher-id-4",
        |   "teacher_created_time": "2018-08-09 02:30:00.0",
        |   "teacher_school_dw_id" : 1,
        |   "teacher_subject_dw_id":5,
        |   "teacher_enabled":true,
        |   "teacher_active_until": null
        |}]
        """.stripMargin
    val teacherDimDf = spark.read.json(Seq(teacherDim).toDS())
    when(service.readFromRedshiftQuery[TeacherDim](s"select * from ${Resources.redshiftSchema()}.dim_teacher where teacher_id in ('teacher-created-id1','teacher-created-id2','teacher-id-4','teacher-id-3') and teacher_active_until is null")).thenReturn(teacherDimDf)

    val schoolDim =
      """
        |[{
        |   "school_status":1,
        |   "school_dw_id" : 1,
        |   "school_id":"school-id",
        |   "school_created_time": "2018-08-09 02:30:00.0"
        |},{
        |   "school_status":1,
        |   "school_dw_id" : 2,
        |   "school_id":"new_school",
        |   "school_created_time": "2018-08-09 02:31:00.0"
        |}
        |]
        """.stripMargin
    val schoolDimDf = spark.read.json(Seq(schoolDim).toDS())
    when(service.readFromRedshiftQuery[SchoolDim](s"select * from ${Resources.redshiftSchema()}.dim_school where school_dw_id in (1)")).thenReturn(schoolDimDf)

    val transformer = new TeacherTransform(sprk, service)
    val teacherCreated = transformer.transform().get.output
    assert(teacherCreated.count == 4)
    assert(teacherCreated.columns.toSet === teacherDimExpectedColumns)

    val teacher1 = teacherCreated.filter("teacher_id = 'teacher-created-id1'")
    assert[Int](teacher1, "teacher_status", ActiveEnabled)
    assert[String](teacher1, "teacher_created_time", "2018-10-10 02:30:00.0")
    assert[String](teacher1, "school_id", "new_school")

    val teacher2 = teacherCreated.filter("teacher_id = 'teacher-created-id2'")
    assert[Int](teacher2, "teacher_status", ActiveEnabled)
    assert[String](teacher2, "teacher_created_time", "2018-09-30 21:40:10.0")
    assert[String](teacher2, "school_id", "school-id")

    val teacher4 = teacherCreated.filter("teacher_id = 'teacher-id-4'")
    assert[Int](teacher4, "teacher_status", ActiveEnabled)
    assert[String](teacher4, "teacher_created_time", "2018-09-30 22:40:10.0")
    assert[String](teacher4, "school_id", "school-id")

    val teacher3 = teacherCreated.filter("teacher_id = 'teacher-id-3'")
    assert[Int](teacher3, "teacher_status", Disabled)
    assert[String](teacher3, "teacher_created_time", "2018-09-30 22:45:10.0")
    assert[String](teacher3, "school_id", "school-id")

  }

  test("teacher created and disabled events with same occurredOn") {
    val sprk = spark
    import sprk.implicits._

    val teacherMutatedEvent =
      """
        |[
        |{"eventType":"TeacherCreatedEvent","occurredOn":"2021-08-30 07:02:14.877","loadtime":"2021-08-30 15:37:53.56","tenantId":"93e4949d-7eff-4707-9201-dac917a5e013","createdOn":1630306934876,"uuid":"teacher-id-1","enabled":false,"schoolId":"61eda87b-98b3-4e37-a8a2-8c30f82a68b1"},
        |{"eventType":"TeacherDisabledEvent","occurredOn":"2021-08-30 07:02:14.877","loadtime":"2021-08-30 15:38:05.101","tenantId":"93e4949d-7eff-4707-9201-dac917a5e013","createdOn":1630306934877,"uuid":"teacher-id-1","enabled":false,"schoolId":"61eda87b-98b3-4e37-a8a2-8c30f82a68b1"},
        |{"eventType":"TeacherCreatedEvent","occurredOn":"2021-08-30 07:02:15.487","loadtime":"2021-08-30 15:38:15.211","tenantId":"93e4949d-7eff-4707-9201-dac917a5e013","createdOn":1630306935487,"uuid":"teacher-id-2","enabled":false,"schoolId":"61eda87b-98b3-4e37-a8a2-8c30f82a68b1"},
        |{"eventType":"TeacherDisabledEvent","occurredOn":"2021-08-30 07:02:15.487","loadtime":"2021-08-30 15:38:24.572","tenantId":"93e4949d-7eff-4707-9201-dac917a5e013","createdOn":1630306935487,"uuid":"teacher-id-2","enabled":false,"schoolId":"61eda87b-98b3-4e37-a8a2-8c30f82a68b1"}
        |]
        """.stripMargin
    val teacherMutatedDf = spark.read.json(Seq(teacherMutatedEvent).toDS())
    when(service.readOptional(ParquetTeacherSource, sprk)).thenReturn(Some(teacherMutatedDf))

    when(service.readOptional(ParquetTeacherSchoolMovedSource, sprk)).thenReturn(None)

    when(service.readFromRedshiftQuery[TeacherRel](s"select * from ${Resources.redshiftStageSchema()}.rel_teacher where teacher_id in ('teacher-id-1','teacher-id-2') and teacher_active_until is null")).thenReturn(Seq.empty[TeacherRel].toDF())
    when(service.readFromRedshiftQuery[TeacherDim](s"select * from ${Resources.redshiftSchema()}.dim_teacher where teacher_id in ('teacher-id-1','teacher-id-2') and teacher_active_until is null")).thenReturn(Seq.empty[TeacherDim].toDF())

    val schoolDim =
      """
        |[{
        |   "school_status":1,
        |   "school_dw_id" : 1,
        |   "school_id":"61eda87b-98b3-4e37-a8a2-8c30f82a68b1",
        |   "school_created_time": "2019-01-09 02:30:00.0"
        |}
        |]
        """.stripMargin
    val schoolDimDf = spark.read.json(Seq(schoolDim).toDS())
    when(service.readFromRedshiftQuery[SchoolDim](s"select * from ${Resources.redshiftSchema()}.dim_school where school_dw_id in (1)")).thenReturn(schoolDimDf)

    val transformer = new TeacherTransform(sprk, service)
    val teacherDf = transformer.transform().get.output

    val fstTeacherDisabled = teacherDf.filter("teacher_id = 'teacher-id-1'")
    assert[Int](fstTeacherDisabled, "teacher_status", Disabled)
    assert[String](fstTeacherDisabled, "teacher_created_time", "2021-08-30 07:02:14.877")

    val sndTeacherDisabled = teacherDf.filter("teacher_id = 'teacher-id-2'")
    assert[Int](sndTeacherDisabled, "teacher_status", Disabled)
    assert[String](sndTeacherDisabled, "teacher_created_time", "2021-08-30 07:02:15.487")
  }
}
