package com.alefeducation.dimensions

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.ExpectedFields.assertUserSink
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.dimensions.student.StudentDelta.StudentTransformedSource
import com.alefeducation.dimensions.student.{StudentDelta, StudentRedshift, StudentRelUserTransform, StudentTransform}
import com.alefeducation.models.StudentModel.{GradeDim, SchoolDim, SectionDim, StudentDim, StudentRel}
import com.alefeducation.util.Constants.{StudentUserType, TeacherUserType}
import com.alefeducation.util.Helpers._
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.functions._
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

class StudentDimensionSpec extends SparkSuite {

  val service = mock[SparkBatchService]

  val studentDimExpectedColumns: Set[String] = dimDateCols("student").toSet ++ Set(
    "student_uuid",
    "student_username",
    "school_uuid",
    "grade_uuid",
    "section_uuid",
    "student_tags",
    "student_special_needs"
  )

  val studentDeltaDimExpectedColumns: Set[String] = dimDateCols("student").toSet ++ Set(
    "student_id",
    "student_username",
    "school_id",
    "grade_id",
    "section_id",
    "student_tags",
    "student_special_needs"
  )

  test("grade movement , section movement and school movement event in disabled student with username changed old and new") {

    val sprk = spark
    import sprk.implicits._
    val studentMovedEvent =
      """{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentMovedBetweenGrades",
        |   "occurredOn":"2019-02-10 02:39:00.0",
        |   "studentId" : "student-id",
        |   "loadtime":"wMC3yz1AAAAggyUA",
        |   "oldSchoolId":"old-school-id",
        |   "oldClassId":"old-class-id",
        |   "oldSectionId":"old-class-id",
        |   "oldGradeId": "old-grade-id",
        |   "oldK12Grade":6,
        |   "targetSchoolId":"new-school-id",
        |   "targetClassId":"new-class-id",
        |   "targetSectionId":"new-class-id",
        |   "targetGradeId":"new-grade-id",
        |   "targetK12Grade": 7
        |},
        |{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentMovedBetweenSchools",
        |   "occurredOn":"2019-02-11 02:41:00.0",
        |   "studentId" : "student-id1",
        |   "loadtime":"wMC3yz1AAAAggyUA",
        |   "oldSchoolId":"old-school-id",
        |   "oldClassId":"old-class-id",
        |   "oldSectionId":"old-class-id",
        |   "oldGradeId": "old-grade-id",
        |   "oldK12Grade":6,
        |   "targetSchoolId":"new-school-id1",
        |   "targetClassId":"new-class-id1",
        |   "targetSectionId":"new-class-id1",
        |   "targetGradeId":"new-grade-id1",
        |   "targetK12Grade": 7
        |}""".stripMargin
    val duplicateStudentMovedEvent = studentMovedEvent
    val studentMoved = spark.read.json(Seq(s"""[$studentMovedEvent,$duplicateStudentMovedEvent]""".stripMargin).toDS())
    when(service.readOptional(ParquetStudentSchoolOrGradeMoveSource, sprk)).thenReturn(Some(studentMoved))

    val studentPromotedEvent =
      """
        |[{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentPromotedEvent",
        |   "occurredOn":"2019-02-10 02:40:00.0",
        |   "studentId" : "student-id",
        |   "loadtime":"wMC3yz1AAAAggyUA",
        |   "rolledOverClassId":"new-class-id1",
        |   "rolledOverSectionId":"new-class-id1",
        |   "rolledOverGradeId": "new-grade-id1"
        |}]
            """.stripMargin
    val studentPromoted = spark.read.json(Seq(studentPromotedEvent).toDS())
    when(service.readOptional(ParquetStudentPromotedSource, sprk)).thenReturn(Some(studentPromoted))

    val studentStudentSectionUpdatedEvent =
      """
        |[{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentSectionUpdatedEvent",
        |   "occurredOn":"2019-02-12 02:40:00.0",
        |   "id" : "student-id",
        |   "grade":"6",
        |   "gradeId": "new-grade-id1",
        |   "schoolId":"new-school-id1",
        |   "oldSectionId":"new-class-id1",
        |   "newSectionId":"new-class-id2"
        |}
        |]
            """.stripMargin
    val studentStudentSectionUpdated = spark.read.json(Seq(studentStudentSectionUpdatedEvent).toDS())
    when(service.readOptional(ParquetStudentSectionUpdatedSource, sprk)).thenReturn(Some(studentStudentSectionUpdated))

    val redshiftSchoolData =
      """
        |[{
        |   "school_status":1,
        |   "school_dw_id" : 1,
        |   "school_id":"school-id",
        |   "school_created_time": "1970-01-15 02:40:00.0"
        |}
        |]
            """.stripMargin
    val redshiftSchool = spark.read.json(Seq(redshiftSchoolData).toDS())
    when(service.readFromRedshift[SchoolDim](RedshiftSchoolSink, selectedCols = List("school_dw_id", "school_id"))).thenReturn(redshiftSchool)

    val redshiftGradeData =
      """
        |[{
        |   "grade_status":1,
        |   "grade_dw_id" : 1,
        |   "grade_id":"grade-id",
        |   "grade_created_time": "1970-01-15 02:40:00.0"
        |}
        |]
            """.stripMargin
    val redshiftGrade = spark.read.json(Seq(redshiftGradeData).toDS())
    when(service.readFromRedshift[GradeDim](RedshiftGradeSink, selectedCols = List("grade_dw_id", "grade_id"))).thenReturn(redshiftGrade)

    val redshiftSectionData =
      """
        |[{
        |   "section_status":1,
        |   "section_dw_id" : 1,
        |   "section_id":"class-id",
        |   "section_created_time": "1970-01-15 02:40:00.0"
        |}
        |]
            """.stripMargin
    val redshiftSection = spark.read.json(Seq(redshiftSectionData).toDS())
    when(service.readFromRedshift[SectionDim](RedshiftSectionSink, selectedCols = List("section_dw_id", "section_id"))).thenReturn(redshiftSection)

    val redshiftDimStudentData =
      """
        |[{
        |   "student_status":1,
        |   "student_id":"student-id",
        |   "student_created_time": "1970-01-15 02:40:00.0",
        |   "student_username":"student-username",
        |   "student_school_dw_id" : "1",
        |   "student_grade_dw_id":"1",
        |   "student_section_dw_id":"1",
        |   "student_tags":"ELITE",
        |   "student_active_until": null,
        |   "student_special_needs":"n/a"
        |},
        |{
        |   "student_status":1,
        |   "student_id":"student-id1",
        |   "student_created_time": "1970-01-15 02:40:00.0",
        |   "student_username":"student-username1",
        |   "student_school_dw_id" : "1",
        |   "student_grade_dw_id":"1",
        |   "student_section_dw_id":"1",
        |   "student_tags":null,
        |   "student_active_until": null,
        |   "student_special_needs":"n/a"
        |}
        |]
            """.stripMargin
    val redshiftDimStudent = spark.read.json(Seq(redshiftDimStudentData).toDS())
    when(service.readFromRedshift[StudentDim](RedshiftDimStudentSink)).thenReturn(redshiftDimStudent)

    when(service.readFromRedshift[StudentRel](RedshiftStudentSink)).thenReturn(Seq.empty[StudentRel].toDF)

    when(service.readOptional(ParquetStudentsDeletedSource, sprk)).thenReturn(None)

    val redshiftStudentEnableDisableData =
      """
        |[{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentEnabledEvent",
        |   "createdOn":12345,
        |   "schoolId" : "school-id",
        |   "loadtime":"wMC3yz1AAAAggyUA",
        |   "eventDateDw":"20181008",
        |   "uuid": "student-id",
        |   "k12Grade":6,
        |   "classId":"class-id",
        |   "sectionId":"class-id",
        |   "gradeId":"grade-id",
        |   "username":"student-u1",
        |   "occurredOn": "1970-08-20 03:40:00.0"
        |},
        |{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentDisabledEvent",
        |   "createdOn":12346,
        |   "schoolId" : "school-id",
        |   "loadtime":"wMC3yz1AAAAggyUA",
        |   "eventDateDw":"20181008",
        |   "uuid": "student-id",
        |   "k12Grade":6,
        |   "classId":"class-id",
        |   "sectionId":"class-id",
        |   "gradeId":"grade-id",
        |   "username":"student-u1",
        |   "occurredOn": "1970-08-21 02:40:00.0"
        |}]
            """.stripMargin
    val redshiftStudentEnableDisable = spark.read.json(Seq(redshiftStudentEnableDisableData).toDS())
    when(service.readOptional(ParquetStudentEnableDisableSource, sprk)).thenReturn(Some(redshiftStudentEnableDisable))

    val studentUpdatedEvent =
      """
        |[{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentUpdatedEvent",
        |   "createdOn":12345,
        |   "uuid":"student-id",
        |   "username":"good-student",
        |   "grade": "6",
        |   "gradeId" : "grade-id",
        |   "classId": "class-id",
        |   "sectionId": "class-id",
        |   "schoolId" : "school-id",
        |   "tags":["Test"],
        |   "specialNeeds":null,
        |   "loadtime":"wMC3yz1AAAAggyUA",
        |   "eventDateDw":"20181008",
        |   "occurredOn": "2019-03-11 02:30:00.0"
        |},
        |{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentUpdatedEvent",
        |   "createdOn":12345,
        |   "uuid":"student-id1",
        |   "username":"good-student1",
        |   "grade": "6",
        |   "gradeId" : "grade-id",
        |   "classId": "class-id",
        |   "sectionId": "class-id",
        |   "schoolId" : "school-id",
        |   "tags":["Test"],
        |   "specialNeeds":["Hearing Impairment - Deafness"],
        |   "loadtime":"wMC3yz1AAAAggyUA",
        |   "eventDateDw":"20181008",
        |   "occurredOn": "2019-03-10 02:30:00.0"
        |}]
            """.stripMargin
    val studentUpdated = spark.read.json(Seq(studentUpdatedEvent).toDS())
    when(service.readOptional(ParquetStudentSource, sprk)).thenReturn(Some(studentUpdated))

    val studentTagUpdatedEvent =
      """
        |[{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentTagUpdatedEvent",
        |   "occurredOn":"2019-03-10 02:30:00.0",
        |   "studentId" : "student-id",
        |   "tags":["Elite"]
        |}
        |]
            """.stripMargin
    val studentTagUpdated = spark.read.json(Seq(studentTagUpdatedEvent).toDS())
    when(service.readOptional(ParquetStudentTagUpdatedSource, sprk)).thenReturn(Some(studentTagUpdated))

    val transformer = new StudentTransform(sprk, service)
    val result = transformer.transform().get.output

    assert(result.count == 2)
    val oldStudent = result.filter(col("student_uuid") === "student-id1")
    assert(result.columns.toSet === studentDimExpectedColumns)
    assert[String](oldStudent, "student_uuid", "student-id1")
    assert[String](oldStudent, "student_tags", "Test")
    assert[String](oldStudent, "student_created_time", "2019-03-10 02:30:00.0")
    assert[Int](oldStudent, "student_status", ActiveEnabled)
    assert[String](oldStudent, "student_special_needs", "Hearing Impairment - Deafness")
    val newStudent = result.filter(col("student_uuid") === "student-id")
    assert[String](newStudent, "section_uuid", "new-class-id2")
    assert[String](newStudent, "student_username", "good-student")
    assert[String](newStudent, "student_tags", "Elite")
    assert[Int](newStudent, "student_status", Disabled)
    assert[String](newStudent, "student_created_time", "2019-03-11 02:30:00.0")
    assert[String](newStudent, "student_special_needs", "n/a")
  }

  test("student created event rel user transformer") {

    val sprk = spark
    import sprk.implicits._
    val studentCreatedEvent =
      """
        |[{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentCreatedEvent",
        |   "createdOn":12345,
        |   "uuid":"student-id",
        |   "username":"student-username",
        |   "grade": "6",
        |   "gradeId" : "grade-id",
        |   "classId": "class-id",
        |   "sectionId": "class-id",
        |   "schoolId" : "school-id",
        |   "tags":null,
        |   "specialNeeds":null,
        |   "loadtime":"wMC3yz1AAAAggyUA",
        |   "eventDateDw":"20181008",
        |   "occurredOn": "1970-07-14 02:40:00.0"
        |}]
        |""".stripMargin

    val studentCreated = spark.read.json(Seq(studentCreatedEvent).toDS())
    when(service.readOptional(ParquetStudentSource, sprk)).thenReturn(Some(studentCreated))

    val transformer = new StudentRelUserTransform(spark, service);
    assertUserSink(transformer.transform().toList, List(("student-id", "1970-07-14 02:40:00.0", StudentUserType)))
  }

  test("student created event old and new") {

    val sprk = spark
    import sprk.implicits._
    val studentCreatedEvent =
      """
        |[{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentCreatedEvent",
        |   "createdOn":12345,
        |   "uuid":"student-id",
        |   "username":"student-username",
        |   "grade": "6",
        |   "gradeId" : "grade-id",
        |   "classId": "class-id",
        |   "sectionId": "class-id",
        |   "schoolId" : "school-id",
        |   "tags":null,
        |   "specialNeeds":null,
        |   "loadtime":"wMC3yz1AAAAggyUA",
        |   "eventDateDw":"20181008",
        |   "occurredOn": "1970-07-14 02:40:00.0"
        |},
        |{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentCreatedEvent",
        |   "createdOn":12345,
        |   "uuid":"student-id1",
        |   "username":"student-username1",
        |   "grade": "6",
        |   "gradeId" : "grade-id",
        |   "classId": "class-id",
        |   "sectionId": "class-id",
        |   "schoolId" : "school-id",
        |   "tags":["Test"],
        |   "specialNeeds":["Hearing Impairment - Deafness"],
        |   "loadtime":"wMC3yz1AAAAggyUA",
        |   "eventDateDw":"20181008",
        |   "occurredOn": "1970-07-14 02:40:00.0"
        |}]
            """.stripMargin
    val studentCreated = spark.read.json(Seq(studentCreatedEvent).toDS())
    when(service.readOptional(ParquetStudentSource, sprk)).thenReturn(Some(studentCreated))

    val studentTagEvent =
      """
        |[{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentTagUpdatedEvent",
        |   "occurredOn":"2019-02-12 02:40:00.0",
        |   "studentId" : "student-id",
        |   "tags":["Elite"]
        |}
        |]
            """.stripMargin
    val studentTag = spark.read.json(Seq(studentTagEvent).toDS())
    when(service.readOptional(ParquetStudentTagUpdatedSource, sprk)).thenReturn(Some(studentTag))

    when(service.readOptional(ParquetStudentSchoolOrGradeMoveSource, sprk)).thenReturn(None)
    when(service.readOptional(ParquetStudentPromotedSource, sprk)).thenReturn(None)
    when(service.readOptional(ParquetStudentSectionUpdatedSource, sprk)).thenReturn(None)
    when(service.readFromRedshift[SchoolDim](RedshiftSchoolSink, selectedCols = List("school_dw_id", "school_id"))).thenReturn(Seq.empty[(Int, String)].toDF("school_dw_id", "school_id"))
    when(service.readFromRedshift[GradeDim](RedshiftGradeSink, selectedCols = List("grade_dw_id", "grade_id"))).thenReturn(Seq.empty[(Int, String)].toDF("grade_dw_id", "grade_id"))
    when(service.readFromRedshift[SectionDim](RedshiftSectionSink, selectedCols = List("section_dw_id", "section_id"))).thenReturn(Seq.empty[(Int, String)].toDF("section_dw_id", "section_id"))
    when(service.readFromRedshift[StudentDim](RedshiftDimStudentSink)).thenReturn(Seq.empty[StudentDim].toDF)
    when(service.readOptional(ParquetStudentEnableDisableSource, sprk)).thenReturn(None)
    when(service.readFromRedshift[StudentRel](RedshiftStudentSink)).thenReturn(Seq.empty[StudentRel].toDF)
    when(service.readOptional(ParquetStudentsDeletedSource, sprk)).thenReturn(None)

    val transformer = new StudentTransform(sprk, service)
    val studentCreatedDf = transformer.transform().get.output

    assert(studentCreatedDf.count == 2)
    assert(studentCreatedDf.columns.toSet === studentDimExpectedColumns)
    val newStudent = studentCreatedDf.filter("student_uuid == 'student-id1'")
    assert[String](newStudent, "student_uuid", "student-id1")
    assert[String](newStudent, "student_username", "student-username1")
    assert[String](newStudent, "student_special_needs", "Hearing Impairment - Deafness")
    assert[String](newStudent, "student_tags", "Test")
    assert[Int](newStudent, "student_status", ActiveEnabled)
    assert[String](newStudent, "student_created_time", "1970-07-14 02:40:00.0")
  }

  test("student created duplicate events old only") {
    val sprk = spark
    import sprk.implicits._

    val studentCreatedEvent =
      """{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentCreatedEvent",
        |   "createdOn":12345,
        |   "uuid":"student-id1",
        |   "username":"student-username1",
        |   "grade": "6",
        |   "gradeId" : "grade-id",
        |   "classId": "class-id",
        |   "sectionId": "class-id",
        |   "schoolId" : "school-id",
        |   "tags":["Test"],
        |   "specialNeeds":[],
        |   "loadtime":"wMC3yz1AAAAggyUA",
        |   "eventDateDw":"20181008",
        |   "occurredOn": "1970-07-14 02:40:00.0"
        |}""".stripMargin
    val studentCreateDuplicateEvent = studentCreatedEvent
    val studentCreatedDup = spark.read.json(Seq(s"""[$studentCreatedEvent,$studentCreateDuplicateEvent]""".stripMargin).toDS())
    when(service.readOptional(ParquetStudentSource, sprk)).thenReturn(Some(studentCreatedDup))
    when(service.readOptional(ParquetStudentSchoolOrGradeMoveSource, sprk)).thenReturn(None)
    when(service.readOptional(ParquetStudentTagUpdatedSource, sprk)).thenReturn(None)
    when(service.readOptional(ParquetStudentPromotedSource, sprk)).thenReturn(None)
    when(service.readOptional(ParquetStudentSectionUpdatedSource, sprk)).thenReturn(None)
    when(service.readFromRedshift[SchoolDim](RedshiftSchoolSink, selectedCols = List("school_dw_id", "school_id"))).thenReturn(Seq.empty[(Int, String)].toDF("school_dw_id", "school_id"))
    when(service.readFromRedshift[GradeDim](RedshiftGradeSink, selectedCols = List("grade_dw_id", "grade_id"))).thenReturn(Seq.empty[(Int, String)].toDF("grade_dw_id", "grade_id"))
    when(service.readFromRedshift[SectionDim](RedshiftSectionSink, selectedCols = List("section_dw_id", "section_id"))).thenReturn(Seq.empty[(Int, String)].toDF("section_dw_id", "section_id"))
    when(service.readFromRedshift[StudentDim](RedshiftDimStudentSink)).thenReturn(Seq.empty[StudentDim].toDF)
    when(service.readOptional(ParquetStudentEnableDisableSource, sprk)).thenReturn(None)
    when(service.readFromRedshift[StudentRel](RedshiftStudentSink)).thenReturn(Seq.empty[StudentRel].toDF)
    when(service.readOptional(ParquetStudentsDeletedSource, sprk)).thenReturn(None)

    val transformer = new StudentTransform(sprk, service)
    val studentCreatedDf = transformer.transform().get.output
    assert(studentCreatedDf.count == 1)
    assert(studentCreatedDf.columns.toSet === studentDimExpectedColumns)
    assert[String](studentCreatedDf, "student_uuid", "student-id1")
    assert[String](studentCreatedDf, "student_username", "student-username1")
    assert[String](studentCreatedDf, "student_tags", "Test")
    assert[String](studentCreatedDf, "student_special_needs", "n/a")
    assert[Int](studentCreatedDf, "student_status", ActiveEnabled)
    assert[String](studentCreatedDf, "student_created_time", "1970-07-14 02:40:00.0")
  }

  test("student updated event old only") {

    val sprk = spark
    import sprk.implicits._

    val studentUpdatedEvent =
      """
        |[
        |{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentUpdatedEvent",
        |   "createdOn":12345,
        |   "uuid":"student-id",
        |   "username":"student-username1",
        |   "grade": "6",
        |   "gradeId" : "grade-id",
        |   "classId": "class-id",
        |   "sectionId": "class-id",
        |   "schoolId" : "school-id",
        |   "tags":["Test"],
        |   "specialNeeds":[],
        |   "loadtime":"wMC3yz1AAAAggyUA",
        |   "eventDateDw":"20181008",
        |   "occurredOn": "2019-03-10 02:30:00.0"
        |}]
            """.stripMargin
    val studentUpdated = spark.read.json(Seq(studentUpdatedEvent).toDS())
    when(service.readOptional(ParquetStudentSource, sprk)).thenReturn(Some(studentUpdated))

    val studentRedshiftData =
      """
        |[{
        |   "student_status":1,
        |   "student_uuid":"student-id",
        |   "student_created_time": "2019-03-09 02:30:00.0",
        |   "student_username":"student-username",
        |   "school_uuid" : "old-school-id",
        |   "grade_uuid":"old-grade-id",
        |   "section_uuid":"old-class-id",
        |   "student_tags":"ELITE",
        |   "student_active_until": null,
        |   "student_special_needs":"Disablity"
        |}
        |]
            """.stripMargin
    val studentRedshiftDataDup = spark.read.json(Seq(studentRedshiftData).toDS())
    when(service.readFromRedshift[StudentRel](RedshiftStudentSink)).thenReturn(studentRedshiftDataDup)
    when(service.readOptional(ParquetStudentSchoolOrGradeMoveSource, sprk)).thenReturn(None)
    when(service.readOptional(ParquetStudentTagUpdatedSource, sprk)).thenReturn(None)
    when(service.readOptional(ParquetStudentPromotedSource, sprk)).thenReturn(None)
    when(service.readOptional(ParquetStudentSectionUpdatedSource, sprk)).thenReturn(None)
    when(service.readFromRedshift[SchoolDim](RedshiftSchoolSink, selectedCols = List("school_dw_id", "school_id"))).thenReturn(Seq.empty[(Int, String)].toDF("school_dw_id", "school_id"))
    when(service.readFromRedshift[GradeDim](RedshiftGradeSink, selectedCols = List("grade_dw_id", "grade_id"))).thenReturn(Seq.empty[(Int, String)].toDF("grade_dw_id", "grade_id"))
    when(service.readFromRedshift[SectionDim](RedshiftSectionSink, selectedCols = List("section_dw_id", "section_id"))).thenReturn(Seq.empty[(Int, String)].toDF("section_dw_id", "section_id"))
    when(service.readFromRedshift[StudentDim](RedshiftDimStudentSink)).thenReturn(Seq.empty[StudentDim].toDF)
    when(service.readOptional(ParquetStudentEnableDisableSource, sprk)).thenReturn(None)
    when(service.readOptional(ParquetStudentsDeletedSource, sprk)).thenReturn(None)

    val transformer = new StudentTransform(sprk, service)
    val studentUpdatedDf = transformer.transform().get.output

    assert(studentUpdatedDf.count == 1)
    assert(studentUpdatedDf.columns.toSet === studentDimExpectedColumns)
    assert[String](studentUpdatedDf, "student_uuid", "student-id")
    assert[String](studentUpdatedDf, "student_username", "student-username1")
    assert[String](studentUpdatedDf, "student_tags", "Test")
    assert[String](studentUpdatedDf, "student_special_needs", "n/a")
    assert[Int](studentUpdatedDf, "student_status", ActiveEnabled)
    assert[String](studentUpdatedDf, "student_created_time", "2019-03-10 02:30:00.0")
  }

  test("student created event with username present in event") {
    val sprk = spark
    import sprk.implicits._

    val studentUpdatedEvent =
      """
        |[{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentCreatedEvent",
        |   "createdOn":12345,
        |   "uuid":"student-id",
        |   "username":"student-username",
        |   "grade": "6",
        |   "gradeId" : "grade-id",
        |   "classId": "class-id",
        |   "sectionId": "class-id",
        |   "schoolId" : "school-id",
        |   "tags":["Test"],
        |   "specialNeeds":["Hearing Impairment"],
        |   "loadtime":"wMC3yz1AAAAggyUA",
        |   "eventDateDw":"20181008",
        |   "occurredOn": "1970-07-14 02:40:00.0"
        |}]
            """.stripMargin
    val studentUpdated = spark.read.json(Seq(studentUpdatedEvent).toDS())
    when(service.readOptional(ParquetStudentSource, sprk)).thenReturn(Some(studentUpdated))

    val studentTagUpdatedSourceEvent =
      """
        |[{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentTagUpdatedEvent",
        |   "occurredOn":"2019-02-12 02:40:00.0",
        |   "studentId" : "student-id",
        |   "tags":["Elite"]
        |}
        |]
            """.stripMargin
    val studentTagUpdatedSource = spark.read.json(Seq(studentTagUpdatedSourceEvent).toDS())
    when(service.readOptional(ParquetStudentTagUpdatedSource, sprk)).thenReturn(Some(studentTagUpdatedSource))
    when(service.readFromRedshift[StudentRel](RedshiftStudentSink)).thenReturn(Seq.empty[StudentRel].toDF)
    when(service.readOptional(ParquetStudentSchoolOrGradeMoveSource, sprk)).thenReturn(None)
    when(service.readOptional(ParquetStudentPromotedSource, sprk)).thenReturn(None)
    when(service.readOptional(ParquetStudentSectionUpdatedSource, sprk)).thenReturn(None)
    when(service.readFromRedshift[SchoolDim](RedshiftSchoolSink, selectedCols = List("school_dw_id", "school_id"))).thenReturn(Seq.empty[(Int, String)].toDF("school_dw_id", "school_id"))
    when(service.readFromRedshift[GradeDim](RedshiftGradeSink, selectedCols = List("grade_dw_id", "grade_id"))).thenReturn(Seq.empty[(Int, String)].toDF("grade_dw_id", "grade_id"))
    when(service.readFromRedshift[SectionDim](RedshiftSectionSink, selectedCols = List("section_dw_id", "section_id"))).thenReturn(Seq.empty[(Int, String)].toDF("section_dw_id", "section_id"))
    when(service.readFromRedshift[StudentDim](RedshiftDimStudentSink)).thenReturn(Seq.empty[StudentDim].toDF)
    when(service.readOptional(ParquetStudentEnableDisableSource, sprk)).thenReturn(None)
    when(service.readOptional(ParquetStudentsDeletedSource, sprk)).thenReturn(None)

    val transformer = new StudentTransform(sprk, service)
    val studentCreatedDf = transformer.transform().get.output

    assert[String](studentCreatedDf, "student_uuid", "student-id")
    assert[String](studentCreatedDf, "student_username", "student-username")
    assert[String](studentCreatedDf, "student_special_needs", "Hearing Impairment")
    assert[String](studentCreatedDf, "student_tags", "Elite")
    assert[Int](studentCreatedDf, "student_status", ActiveEnabled)
    assert[String](studentCreatedDf, "student_created_time", "2019-02-12 02:40:00.0")
  }

  test("student created event with existing students in dim") {
    val sprk = spark
    import sprk.implicits._

    val studentUpdatedEvent =
      """
        |[{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentCreatedEvent",
        |   "createdOn":12345,
        |   "uuid":"student-id",
        |   "username":null,
        |   "grade": "6",
        |   "gradeId" : "grade-id",
        |   "classId": "class-id",
        |   "sectionId": "class-id",
        |   "schoolId" : "school-id",
        |   "tags":["Test"],
        |   "specialNeeds":[],
        |   "loadtime":"wMC3yz1AAAAggyUA",
        |   "eventDateDw":"20181008",
        |   "occurredOn": "1970-07-14 02:40:00.0"
        |}]
            """.stripMargin
    val studentUpdated = spark.read.json(Seq(studentUpdatedEvent).toDS())
    when(service.readOptional(ParquetStudentSource, sprk)).thenReturn(Some(studentUpdated))

    val studentTagUpdatedSourceEvent =
      """
        |[{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentTagUpdatedEvent",
        |   "occurredOn":"1970-07-14 02:40:00.0",
        |   "studentId" : "student-id",
        |   "tags":[]
        |}
        |]
            """.stripMargin
    val studentTagUpdatedSource = spark.read.json(Seq(studentTagUpdatedSourceEvent).toDS())
    when(service.readOptional(ParquetStudentTagUpdatedSource, sprk)).thenReturn(Some(studentTagUpdatedSource))

    when(service.readFromRedshift[StudentRel](RedshiftStudentSink)).thenReturn(Seq.empty[StudentRel].toDF)
    when(service.readOptional(ParquetStudentSchoolOrGradeMoveSource, sprk)).thenReturn(None)
    when(service.readOptional(ParquetStudentPromotedSource, sprk)).thenReturn(None)
    when(service.readOptional(ParquetStudentSectionUpdatedSource, sprk)).thenReturn(None)

    val redshiftSchoolData =
      """
        |[{
        |   "school_status":1,
        |   "school_dw_id" : 1,
        |   "school_id":"school-id",
        |   "school_created_time": "1970-01-15 02:40:00.0"
        |}
        |]
            """.stripMargin
    val redshiftSchool = spark.read.json(Seq(redshiftSchoolData).toDS())
    when(service.readFromRedshift[SchoolDim](RedshiftSchoolSink, selectedCols = List("school_dw_id", "school_id"))).thenReturn(redshiftSchool)

    val redshiftGradeData =
      """
        |[{
        |   "grade_status":1,
        |   "grade_dw_id" : 1,
        |   "grade_id":"grade-id",
        |   "grade_created_time": "1970-01-15 02:40:00.0"
        |}
        |]
            """.stripMargin
    val redshiftGrade = spark.read.json(Seq(redshiftGradeData).toDS())
    when(service.readFromRedshift[GradeDim](RedshiftGradeSink, selectedCols = List("grade_dw_id", "grade_id"))).thenReturn(redshiftGrade)

    val redshiftSectionData =
      """
        |[{
        |   "section_status":1,
        |   "section_dw_id" : 1,
        |   "section_id":"class-id",
        |   "section_created_time": "1970-01-15 02:40:00.0"
        |}
        |]
            """.stripMargin
    val redshiftSection = spark.read.json(Seq(redshiftSectionData).toDS())
    when(service.readFromRedshift[SectionDim](RedshiftSectionSink, selectedCols = List("section_dw_id", "section_id"))).thenReturn(redshiftSection)

    val redshiftStudentData =
      """
        |[{
        |   "student_status":1,
        |   "student_id":"student-id2",
        |   "student_created_time": "1970-01-15 02:40:00.0",
        |   "student_username":"student-username",
        |   "student_school_dw_id" : "1",
        |   "student_grade_dw_id":"1",
        |   "student_section_dw_id":"1",
        |   "student_tags":"ELITE",
        |   "student_active_until": null,
        |   "student_special_needs":"n/a"
        |},
        |{
        |   "student_status":1,
        |   "student_id":"student-id3",
        |   "student_created_time": "1970-01-15 02:40:00.0",
        |   "student_username":"student-username1",
        |   "student_school_dw_id" : "1",
        |   "student_grade_dw_id":"1",
        |   "student_section_dw_id":"1",
        |   "student_tags":null,
        |   "student_active_until": null,
        |   "student_special_needs":"n/a"
        |}
        |]
            """.stripMargin
    val redshiftStudent = spark.read.json(Seq(redshiftStudentData).toDS())
    when(service.readFromRedshift[StudentDim](RedshiftDimStudentSink)).thenReturn(redshiftStudent)

    when(service.readOptional(ParquetStudentEnableDisableSource, sprk)).thenReturn(None)
    when(service.readOptional(ParquetStudentsDeletedSource, sprk)).thenReturn(None)

    val transformer = new StudentTransform(sprk, service)
    val studentCreatedDf = transformer.transform().get.output
    assert(studentCreatedDf.count == 1)
  }

  test("student created event without username in event") {
    val sprk = spark
    import sprk.implicits._

    val studentCreatedEvent =
      """
        |[{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentCreatedEvent",
        |   "createdOn":12345,
        |   "uuid":"student-id",
        |   "username":null,
        |   "grade": "6",
        |   "gradeId" : "grade-id",
        |   "classId": "class-id",
        |   "sectionId": "class-id",
        |   "schoolId" : "school-id",
        |   "tags":["Test"],
        |   "specialNeeds":[],
        |   "loadtime":"wMC3yz1AAAAggyUA",
        |   "eventDateDw":"20181008",
        |   "occurredOn": "1970-07-14 02:40:00.0"
        |}]
            """.stripMargin
    val studentCreated = spark.read.json(Seq(studentCreatedEvent).toDS())
    when(service.readOptional(ParquetStudentSource, sprk)).thenReturn(Some(studentCreated))

    val studentTagUpdatedSourceEvent =
      """
        |[{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentTagUpdatedEvent",
        |   "occurredOn":"1970-07-14 02:40:00.0",
        |   "studentId" : "student-id",
        |   "tags":[]
        |}
        |]
            """.stripMargin
    val studentTagUpdatedSource = spark.read.json(Seq(studentTagUpdatedSourceEvent).toDS())
    when(service.readOptional(ParquetStudentTagUpdatedSource, sprk)).thenReturn(Some(studentTagUpdatedSource))

    when(service.readFromRedshift[StudentRel](RedshiftStudentSink)).thenReturn(Seq.empty[StudentRel].toDF)
    when(service.readOptional(ParquetStudentSchoolOrGradeMoveSource, sprk)).thenReturn(None)
    when(service.readOptional(ParquetStudentPromotedSource, sprk)).thenReturn(None)
    when(service.readOptional(ParquetStudentSectionUpdatedSource, sprk)).thenReturn(None)
    when(service.readFromRedshift[SchoolDim](RedshiftSchoolSink, selectedCols = List("school_dw_id", "school_id"))).thenReturn(Seq.empty[(Int, String)].toDF("school_dw_id", "school_id"))
    when(service.readFromRedshift[GradeDim](RedshiftGradeSink, selectedCols = List("grade_dw_id", "grade_id"))).thenReturn(Seq.empty[(Int, String)].toDF("grade_dw_id", "grade_id"))
    when(service.readFromRedshift[SectionDim](RedshiftSectionSink, selectedCols = List("section_dw_id", "section_id"))).thenReturn(Seq.empty[(Int, String)].toDF("section_dw_id", "section_id"))
    when(service.readFromRedshift[StudentDim](RedshiftDimStudentSink)).thenReturn(Seq.empty[StudentDim].toDF)
    when(service.readOptional(ParquetStudentEnableDisableSource, sprk)).thenReturn(None)
    when(service.readOptional(ParquetStudentsDeletedSource, sprk)).thenReturn(None)

    val transformer = new StudentTransform(sprk, service)
    val studentCreatedDf = transformer.transform().get.output

    assert(studentCreatedDf.count == 1)
    assert(studentCreatedDf.columns.toSet === studentDimExpectedColumns)
    assert[String](studentCreatedDf, "student_uuid", "student-id")
    assert[String](studentCreatedDf, "student_username", null)
    assert[String](studentCreatedDf, "student_tags", DefaultStringValue)
    assert[Int](studentCreatedDf, "student_status", ActiveEnabled)
    assert[String](studentCreatedDf, "student_created_time", "1970-07-14 02:40:00.0")
  }

  test("student updated event") {

    val sprk = spark
    import sprk.implicits._

    val studentUpdatedEvent =
      """
        |[{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentUpdatedEvent",
        |   "createdOn":12345,
        |   "uuid":"student-id",
        |   "username":"student-username1",
        |   "grade": "6",
        |   "gradeId" : "grade-id",
        |   "classId": "class-id",
        |   "sectionId": "class-id",
        |   "schoolId" : "school-id",
        |   "tags":["Test"],
        |   "specialNeeds":[],
        |   "loadtime":"wMC3yz1AAAAggyUA",
        |   "eventDateDw":"20181008",
        |   "occurredOn": "2019-03-10 02:30:00.0"
        |},
        |{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentUpdatedEvent",
        |   "createdOn":12345,
        |   "uuid":"student-id2",
        |   "username":"student-username2",
        |   "grade": "6",
        |   "gradeId" : "grade-id",
        |   "classId": "class-id",
        |   "sectionId": "class-id",
        |   "schoolId" : "school-id",
        |   "tags":["Test"],
        |   "specialNeeds":[],
        |   "loadtime":"wMC3yz1AAAAggyUA",
        |   "eventDateDw":"20181008",
        |   "occurredOn": "2019-03-10 02:31:00.0"
        |}]
              """.stripMargin
    val studentUpdated = spark.read.json(Seq(studentUpdatedEvent).toDS())
    when(service.readOptional(ParquetStudentSource, sprk)).thenReturn(Some(studentUpdated))

    val studentTagUpdatedSourceEvent =
      """
        |[{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentTagUpdatedEvent",
        |   "occurredOn":"2019-03-10 02:31:00.0",
        |   "studentId" : "student-id2",
        |   "tags":[]
        |},
        |{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentTagUpdatedEvent",
        |   "occurredOn":"2019-03-10 02:30:00.0",
        |   "studentId" : "student-id",
        |   "tags":["ELITE"]
        |}
        |]
              """.stripMargin
    val studentTagUpdatedSource = spark.read.json(Seq(studentTagUpdatedSourceEvent).toDS())
    when(service.readOptional(ParquetStudentTagUpdatedSource, sprk)).thenReturn(Some(studentTagUpdatedSource))

    val redshiftRelStudentData =
      """
        |[{
        |   "student_status":1,
        |   "student_uuid":"student-id",
        |   "student_created_time": "2019-03-09 02:30:00.0",
        |   "student_username":"student-username",
        |   "school_uuid" : "old-school-id",
        |   "grade_uuid":"old-grade-id",
        |   "section_uuid":"old-class-id",
        |   "student_tags":"ELITE",
        |   "student_special_needs":"n/a",
        |   "student_active_until": null
        |},
        |{
        |   "student_status":1,
        |   "student_uuid":"student-id2",
        |   "student_created_time": "2019-03-09 02:31:00.0",
        |   "student_username":"student-username2",
        |   "school_uuid" : "old-school-id",
        |   "grade_uuid":"old-grade-id",
        |   "section_uuid":"old-class-id",
        |   "student_tags":"ELITE",
        |   "student_special_needs":"n/a",
        |   "student_active_until": null
        |}
        |]""".stripMargin
    val redshiftRelStudent = spark.read.json(Seq(redshiftRelStudentData).toDS())
    when(service.readFromRedshift[StudentRel](RedshiftStudentSink)).thenReturn(redshiftRelStudent)

    when(service.readOptional(ParquetStudentSchoolOrGradeMoveSource, sprk)).thenReturn(None)
    when(service.readOptional(ParquetStudentPromotedSource, sprk)).thenReturn(None)
    when(service.readOptional(ParquetStudentSectionUpdatedSource, sprk)).thenReturn(None)

    val redshiftSchoolData =
      """
        |[{
        |   "school_status":1,
        |   "school_dw_id" : 1,
        |   "school_id":"school-id",
        |   "school_created_time": "2019-03-09 02:30:00.0"
        |}
        |]""".stripMargin
    val redshiftSchool = spark.read.json(Seq(redshiftSchoolData).toDS())
    when(service.readFromRedshift[SchoolDim](RedshiftSchoolSink, selectedCols = List("school_dw_id", "school_id"))).thenReturn(redshiftSchool)

    val redshiftGradeData =
      """
        |[{
        |   "grade_status":1,
        |   "grade_dw_id" : 1,
        |   "grade_id":"grade-id",
        |   "grade_created_time": "2019-03-09 02:30:00.0"
        |}
        |]""".stripMargin
    val redshiftGrade = spark.read.json(Seq(redshiftGradeData).toDS())
    when(service.readFromRedshift[GradeDim](RedshiftGradeSink, selectedCols = List("grade_dw_id", "grade_id"))).thenReturn(redshiftGrade)

    val redshiftSectionData =
      """
        |[{
        |   "section_status":1,
        |   "section_dw_id" : 1,
        |   "section_id":"class-id",
        |   "section_created_time": "2019-03-09 02:30:00.0"
        |}
        |]
        |""".stripMargin
    val redshiftSection = spark.read.json(Seq(redshiftSectionData).toDS())
    when(service.readFromRedshift[SectionDim](RedshiftSectionSink, selectedCols = List("section_dw_id", "section_id"))).thenReturn(redshiftSection)

    val redshiftStudentData =
      """
        |[{
        |   "student_status":3,
        |   "student_id":"student-id3",
        |   "student_created_time": "2019-02-09 02:30:00.0",
        |   "student_username":"student-username",
        |   "student_school_dw_id" : "1",
        |   "student_grade_dw_id":"1",
        |   "student_section_dw_id":"1",
        |   "student_tags":"ELITE",
        |   "student_special_needs":"n/a",
        |   "student_active_until": "2019-01-09 02:30:00.0"
        |}
        |]
        |""".stripMargin
    val redshiftStudent = spark.read.json(Seq(redshiftStudentData).toDS())
    when(service.readFromRedshift[StudentDim](RedshiftDimStudentSink)).thenReturn(redshiftStudent)

    when(service.readOptional(ParquetStudentEnableDisableSource, sprk)).thenReturn(None)
    when(service.readOptional(ParquetStudentsDeletedSource, sprk)).thenReturn(None)

    val transformer = new StudentTransform(sprk, service)
    val result = transformer.transform().get.output
    val studentUpdatedDf = result.filter(col("student_uuid") === "student-id2")

    assert(studentUpdatedDf.count == 1)
    assert(studentUpdatedDf.columns.toSet === studentDimExpectedColumns)
    assert[String](studentUpdatedDf, "student_uuid", "student-id2")
    assert[String](studentUpdatedDf, "student_created_time", "2019-03-10 02:31:00.0")
    assert[Int](studentUpdatedDf, "student_status", ActiveEnabled)
    assert[String](studentUpdatedDf, "student_username", "student-username2")
    assert[String](result.filter(col("student_uuid") === "student-id"), "student_username", "student-username1")
    assert[String](result.filter(col("student_uuid") === "student-id"),
      "student_created_time",
      "2019-03-10 02:30:00.0")
    assert[String](result.filter(col("student_uuid") === "student-id2"), "student_tags", DefaultStringValue)
    assert[String](result.filter(col("student_uuid") === "student-id"), "student_tags", "ELITE")

  }

  test("students deleted event") {
    val sprk = spark
    import sprk.implicits._

    val studentUpdatedEvent =
      """
        |[{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentUpdatedEvent",
        |   "createdOn":12345,
        |   "uuid":"student-id",
        |   "username":"student-username1",
        |   "grade": "6",
        |   "gradeId" : "grade-id",
        |   "classId": "class-id",
        |   "sectionId": "class-id",
        |   "schoolId" : "school-id",
        |   "tags":["Test"],
        |   "specialNeeds":[],
        |   "loadtime":"wMC3yz1AAAAggyUA",
        |   "eventDateDw":"20181008",
        |   "occurredOn": "2019-03-10 02:30:00.0"
        |},
        |{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentUpdatedEvent",
        |   "createdOn":12345,
        |   "uuid":"student-id2",
        |   "username":"student-username2",
        |   "grade": "6",
        |   "gradeId" : "grade-id",
        |   "classId": "class-id",
        |   "sectionId": "class-id",
        |   "schoolId" : "school-id",
        |   "tags":["Test"],
        |   "specialNeeds":[],
        |   "loadtime":"wMC3yz1AAAAggyUA",
        |   "eventDateDw":"20181008",
        |   "occurredOn": "2019-03-10 02:31:00.0"
        |}]
              """.stripMargin
    val studentUpdated = spark.read.json(Seq(studentUpdatedEvent).toDS())
    when(service.readOptional(ParquetStudentSource, sprk)).thenReturn(Some(studentUpdated))

    when(service.readOptional(ParquetStudentTagUpdatedSource, sprk)).thenReturn(None)
    when(service.readFromRedshift[StudentRel](RedshiftStudentSink)).thenReturn(Seq.empty[StudentRel].toDF)
    when(service.readOptional(ParquetStudentSchoolOrGradeMoveSource, sprk)).thenReturn(None)
    when(service.readOptional(ParquetStudentPromotedSource, sprk)).thenReturn(None)
    when(service.readOptional(ParquetStudentSectionUpdatedSource, sprk)).thenReturn(None)

    val redshiftSchoolData =
      """
        |[{
        |   "school_status":1,
        |   "school_dw_id" : 1,
        |   "school_id":"school-id",
        |   "school_created_time": "2019-01-09 02:30:00.0"
        |}
        |]""".stripMargin
    val redshiftSchool = spark.read.json(Seq(redshiftSchoolData).toDS())
    when(service.readFromRedshift[SchoolDim](RedshiftSchoolSink, selectedCols = List("school_dw_id", "school_id"))).thenReturn(redshiftSchool)

    val redshiftGradeData =
      """
        |[{
        |   "grade_status":1,
        |   "grade_dw_id" : 1,
        |   "grade_id":"grade-id",
        |   "grade_created_time": "2019-01-09 02:30:00.0"
        |}
        |]""".stripMargin
    val redshiftGrade = spark.read.json(Seq(redshiftGradeData).toDS())
    when(service.readFromRedshift[GradeDim](RedshiftGradeSink, selectedCols = List("grade_dw_id", "grade_id"))).thenReturn(redshiftGrade)

    val redshiftSectionData =
      """
        |[{
        |   "section_status":1,
        |   "section_dw_id" : 1,
        |   "section_id":"class-id",
        |   "section_created_time": "2019-01-09 02:30:00.0"
        |}
        |]""".stripMargin
    val redshiftSection = spark.read.json(Seq(redshiftSectionData).toDS())
    when(service.readFromRedshift[SectionDim](RedshiftSectionSink, selectedCols = List("section_dw_id", "section_id"))).thenReturn(redshiftSection)

    val redshiftStudentData =
      """
        |[{
        |   "student_status":1,
        |   "student_id":"deleted-student-1",
        |   "student_created_time": "2019-02-09 02:30:00.0",
        |   "student_username":"student-username",
        |   "student_school_dw_id" : "1",
        |   "student_grade_dw_id":"1",
        |   "student_section_dw_id":"1",
        |   "student_tags":"Test",
        |   "student_special_needs":"n/a",
        |   "student_active_until": null
        |}
        |,
        |{
        |   "student_status":1,
        |   "student_id":"deleted-student-2",
        |   "student_created_time": "2019-02-09 02:30:00.0",
        |   "student_username":"student-username",
        |   "student_school_dw_id" : "1",
        |   "student_grade_dw_id":"1",
        |   "student_section_dw_id":"1",
        |   "student_tags":"ELITE",
        |   "student_special_needs":"n/a",
        |   "student_active_until": null
        |}
        |]
              """.stripMargin
    val redshiftStudent = spark.read.json(Seq(redshiftStudentData).toDS())
    when(service.readFromRedshift[StudentDim](RedshiftDimStudentSink)).thenReturn(redshiftStudent)

    when(service.readOptional(ParquetStudentEnableDisableSource, sprk)).thenReturn(None)

    val studentDeletedEvent =
      """
        |[{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentsDeletedEvent",
        |   "createdOn":12345,
        |   "schoolId" : "school-id",
        |   "events" : [{"studentId": "deleted-student-1", "classId": "class-id-1", "sectionId": "class-id-1", "gradeId" : "grade-id", "userName" : "uname-1"}, {"studentId": "deleted-student-2", "classId": "class-id-1", "sectionId": "class-id-1", "gradeId" : "grade-id",  "userName" : "uname-2"}],
        |   "loadtime":"wMC3yz1AAAAggyUA",
        |   "eventDateDw":"20181008",
        |   "occurredOn": "2019-03-10 02:30:00.0"
        |}]""".stripMargin
    val studentDeleted = spark.read.json(Seq(studentDeletedEvent).toDS())
    when(service.readOptional(ParquetStudentsDeletedSource, sprk)).thenReturn(Some(studentDeleted))

    val transformer = new StudentTransform(sprk, service)
    val studentDeletedDf = transformer.transform().get.output

    assert(studentDeletedDf.count == 2)
    assert[Int](studentDeletedDf, "student_status", Deleted)
    assert[String](studentDeletedDf, "student_created_time", "2019-03-10 02:30:00.0")

    val deletedStudents = studentDeletedDf.select(col("student_uuid")).as(Encoders.STRING).collectAsList()
    assert(deletedStudents.size == 2)
    assert(deletedStudents.toArray.toSet == Set("deleted-student-1", "deleted-student-2"))
    assert(studentDeletedDf.columns.toSet == studentDimExpectedColumns)

    val deletedStudentsTags = studentDeletedDf.select(col("student_tags")).as(Encoders.STRING).collectAsList()
    assert(deletedStudentsTags.toArray.toSet == Set("Test", "ELITE"))

  }

  test("student enabled event") {

    val sprk = spark
    import sprk.implicits._

    val studentUpdatedEvent =
      """
        |[{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentUpdatedEvent",
        |   "createdOn":12345,
        |   "uuid":"student-id",
        |   "username":"student-username1",
        |   "grade": "6",
        |   "gradeId" : "grade-id",
        |   "classId": "class-id",
        |   "sectionId": "class-id",
        |   "schoolId" : "school-id",
        |   "tags":["Test"],
        |   "specialNeeds":[],
        |   "loadtime":"wMC3yz1AAAAggyUA",
        |   "eventDateDw":"20181008",
        |   "occurredOn": "2019-03-10 02:30:00.0"
        |},
        |{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentUpdatedEvent",
        |   "createdOn":12345,
        |   "uuid":"student-id2",
        |   "username":"student-username2",
        |   "grade": "6",
        |   "gradeId" : "grade-id",
        |   "classId": "class-id",
        |   "sectionId": "class-id",
        |   "schoolId" : "school-id",
        |   "tags":["Test"],
        |   "specialNeeds":[],
        |   "loadtime":"wMC3yz1AAAAggyUA",
        |   "eventDateDw":"20181008",
        |   "occurredOn": "2019-03-10 02:31:00.0"
        |}]
              """.stripMargin
    val studentUpdated = spark.read.json(Seq(studentUpdatedEvent).toDS())
    when(service.readOptional(ParquetStudentSource, sprk)).thenReturn(Some(studentUpdated))

    when(service.readOptional(ParquetStudentTagUpdatedSource, sprk)).thenReturn(None)
    when(service.readFromRedshift[StudentRel](RedshiftStudentSink)).thenReturn(Seq.empty[StudentRel].toDF)
    when(service.readOptional(ParquetStudentSchoolOrGradeMoveSource, sprk)).thenReturn(None)
    when(service.readOptional(ParquetStudentPromotedSource, sprk)).thenReturn(None)
    when(service.readOptional(ParquetStudentSectionUpdatedSource, sprk)).thenReturn(None)

    val redshiftSchoolData =
      """
        |[{
        |   "school_status":1,
        |   "school_dw_id" : 1,
        |   "school_id":"school-id",
        |   "school_created_time": "2019-01-09 02:30:00.0"
        |}
        |]""".stripMargin
    val redshiftSchool = spark.read.json(Seq(redshiftSchoolData).toDS())
    when(service.readFromRedshift[SchoolDim](RedshiftSchoolSink, selectedCols = List("school_dw_id", "school_id"))).thenReturn(redshiftSchool)

    val redshiftGradeData =
      """
        |[{
        |   "grade_status":1,
        |   "grade_dw_id" : 1,
        |   "grade_id":"grade-id",
        |   "grade_created_time": "2019-01-09 02:30:00.0"
        |}
        |]""".stripMargin
    val redshiftGrade = spark.read.json(Seq(redshiftGradeData).toDS())
    when(service.readFromRedshift[GradeDim](RedshiftGradeSink, selectedCols = List("grade_dw_id", "grade_id"))).thenReturn(redshiftGrade)

    val redshiftSectionData =
      """
        |[{
        |   "section_status":1,
        |   "section_dw_id" : 1,
        |   "section_id":"class-id",
        |   "section_created_time": "2019-01-09 02:30:00.0"
        |}
        |]""".stripMargin
    val redshiftSection = spark.read.json(Seq(redshiftSectionData).toDS())
    when(service.readFromRedshift[SectionDim](RedshiftSectionSink, selectedCols = List("section_dw_id", "section_id"))).thenReturn(redshiftSection)

    val redshiftStudentData =
      """
        |[{
        |   "student_status":1,
        |   "student_id":"student-uuid",
        |   "student_created_time": "2019-02-09 02:30:00.0",
        |   "student_username":"student-username",
        |   "student_school_dw_id" : "1",
        |   "student_grade_dw_id":"1",
        |   "student_section_dw_id":"1",
        |   "student_tags":"ELITE",
        |   "student_special_needs":"n/a",
        |   "student_active_until": null
        |}
        |]""".stripMargin
    val redshiftStudent = spark.read.json(Seq(redshiftStudentData).toDS())
    when(service.readFromRedshift[StudentDim](RedshiftDimStudentSink)).thenReturn(redshiftStudent)

    val studentEnableDisableEvent =
      """
        |[{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentEnabledEvent",
        |   "createdOn":12345,
        |   "schoolId" : "school-id",
        |   "loadtime":"wMC3yz1AAAAggyUA",
        |   "eventDateDw":"20181008",
        |   "uuid": "student-uuid",
        |   "k12Grade":6,
        |   "classId":"class-id",
        |   "sectionId":"class-id",
        |   "gradeId":"grade-id",
        |   "username":"student-username",
        |   "occurredOn": "1970-07-14 02:40:00.0"
        |}]
              """.stripMargin
    val studentEnableDisable = spark.read.json(Seq(studentEnableDisableEvent).toDS())
    when(service.readOptional(ParquetStudentEnableDisableSource, sprk)).thenReturn(Some(studentEnableDisable))
    when(service.readOptional(ParquetStudentsDeletedSource, sprk)).thenReturn(None)

    val transformer = new StudentTransform(sprk, service)
    val studentEnabledDf = transformer.transform().get.output

    assert(studentEnabledDf.count == 1)
    assert(studentEnabledDf.columns.toSet === studentDimExpectedColumns)
    assert[String](studentEnabledDf, "student_uuid", "student-uuid")
    assert[String](studentEnabledDf, "student_username", "student-username")
    assert[Int](studentEnabledDf, "student_status", ActiveEnabled)
    assert[String](studentEnabledDf, "student_tags", "ELITE")
    assert[String](studentEnabledDf, "student_created_time", "2019-02-09 02:30:00.0")

  }

  test("student disabled event") {

    val sprk = spark
    import sprk.implicits._

    val studentUpdatedEvent =
      """
        |[{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentUpdatedEvent",
        |   "createdOn":12345,
        |   "uuid":"student-id",
        |   "username":"student-username1",
        |   "grade": "6",
        |   "gradeId" : "grade-id",
        |   "classId": "class-id",
        |   "sectionId": "class-id",
        |   "schoolId" : "school-id",
        |   "tags":["Test"],
        |   "specialNeeds":[],
        |   "loadtime":"wMC3yz1AAAAggyUA",
        |   "eventDateDw":"20181008",
        |   "occurredOn": "2019-03-10 02:30:00.0"
        |},
        |{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentUpdatedEvent",
        |   "createdOn":12345,
        |   "uuid":"student-id2",
        |   "username":"student-username2",
        |   "grade": "6",
        |   "gradeId" : "grade-id",
        |   "classId": "class-id",
        |   "sectionId": "class-id",
        |   "schoolId" : "school-id",
        |   "tags":["Test"],
        |   "specialNeeds":[],
        |   "loadtime":"wMC3yz1AAAAggyUA",
        |   "eventDateDw":"20181008",
        |   "occurredOn": "2019-03-10 02:31:00.0"
        |}]
              """.stripMargin
    val studentUpdated = spark.read.json(Seq(studentUpdatedEvent).toDS())
    when(service.readOptional(ParquetStudentSource, sprk)).thenReturn(Some(studentUpdated))

    when(service.readOptional(ParquetStudentTagUpdatedSource, sprk)).thenReturn(None)
    when(service.readFromRedshift[StudentRel](RedshiftStudentSink)).thenReturn(Seq.empty[StudentRel].toDF)
    when(service.readOptional(ParquetStudentSchoolOrGradeMoveSource, sprk)).thenReturn(None)
    when(service.readOptional(ParquetStudentPromotedSource, sprk)).thenReturn(None)
    when(service.readOptional(ParquetStudentSectionUpdatedSource, sprk)).thenReturn(None)

    val redshiftSchoolData =
      """
        |[{
        |   "school_status":1,
        |   "school_dw_id" : 1,
        |   "school_id":"school-id",
        |   "school_created_time": "2019-01-09 02:30:00.0"
        |}
        |]=""".stripMargin
    val redshiftSchool = spark.read.json(Seq(redshiftSchoolData).toDS())
    when(service.readFromRedshift[SchoolDim](RedshiftSchoolSink, selectedCols = List("school_dw_id", "school_id"))).thenReturn(redshiftSchool)

    val redshiftGradeData =
      """
        |[{
        |   "grade_status":1,
        |   "grade_dw_id" : 1,
        |   "grade_id":"grade-id",
        |   "grade_created_time": "2019-01-09 02:30:00.0"
        |}
        |]""".stripMargin
    val redshiftGrade = spark.read.json(Seq(redshiftGradeData).toDS())
    when(service.readFromRedshift[GradeDim](RedshiftGradeSink, selectedCols = List("grade_dw_id", "grade_id"))).thenReturn(redshiftGrade)

    val redshiftSectionData =
      """
        |[{
        |   "section_status":1,
        |   "section_dw_id" : 1,
        |   "section_id":"class-id",
        |   "section_created_time": "2019-01-09 02:30:00.0"
        |}
        |]
              """.stripMargin
    val redshiftSection = spark.read.json(Seq(redshiftSectionData).toDS())
    when(service.readFromRedshift[SectionDim](RedshiftSectionSink, selectedCols = List("section_dw_id", "section_id"))).thenReturn(redshiftSection)

    val redshiftStudentData =
      """
        |[{
        |   "student_status":1,
        |   "student_id":"student-uuid",
        |   "student_created_time": "2019-02-09 02:30:00.0",
        |   "student_username":"student-username",
        |   "student_school_dw_id" : "1",
        |   "student_grade_dw_id":"1",
        |   "student_section_dw_id":"1",
        |   "student_tags":"ELITE",
        |   "student_special_needs":"n/a",
        |   "student_active_until": null
        |}
        |]
              """.stripMargin
    val redshiftStudent = spark.read.json(Seq(redshiftStudentData).toDS())
    when(service.readFromRedshift[StudentDim](RedshiftDimStudentSink)).thenReturn(redshiftStudent)

    val studentEnableDisableEvent =
      """
        |[{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentDisabledEvent",
        |   "createdOn":12345,
        |   "schoolId" : "school-id",
        |   "loadtime":"wMC3yz1AAAAggyUA",
        |   "eventDateDw":"20181008",
        |   "uuid": "student-uuid",
        |   "k12Grade":6,
        |   "classId":"class-id",
        |   "sectionId":"class-id",
        |   "gradeId":"grade-id",
        |   "username":"student-username",
        |   "occurredOn": "2019-03-09 02:30:00.0"
        |}]
              """.stripMargin
    val studentEnableDisable = spark.read.json(Seq(studentEnableDisableEvent).toDS())
    when(service.readOptional(ParquetStudentEnableDisableSource, sprk)).thenReturn(Some(studentEnableDisable))
    when(service.readOptional(ParquetStudentsDeletedSource, sprk)).thenReturn(None)

    val transformer = new StudentTransform(sprk, service)
    val studentDisabledDf = transformer.transform().get.output

    assert(studentDisabledDf.count == 1)
    assert(studentDisabledDf.columns.toSet === studentDimExpectedColumns)
    assert[String](studentDisabledDf, "student_uuid", "student-uuid")
    assert[String](studentDisabledDf, "student_username", "student-username")
    assert[Int](studentDisabledDf, "student_status", Disabled)
    assert[String](studentDisabledDf, "student_created_time", "2019-03-09 02:30:00.0")
  }

  test("student school movement event") {

    val sprk = spark
    import sprk.implicits._

    when(service.readOptional(ParquetStudentSource, sprk)).thenReturn(None)
    when(service.readOptional(ParquetStudentTagUpdatedSource, sprk)).thenReturn(None)

    val studentRelData =
      """
        |[{
        |   "student_status":1,
        |   "student_uuid":"student-id",
        |   "student_created_time": "2019-03-09 02:30:00.0",
        |   "student_username":"student-username",
        |   "school_uuid" : "old-school-id",
        |   "grade_uuid":"old-grade-id",
        |   "section_uuid":"old-class-id",
        |   "student_tags":"ELITE",
        |   "student_special_needs":"n/a",
        |   "student_active_until": null
        |},
        |{
        |   "student_status":2,
        |   "student_uuid":"student-id",
        |   "student_created_time": "2019-03-09 01:40:00.0",
        |   "student_username":"student-username1",
        |   "school_uuid" : "old-school-id1",
        |   "grade_uuid":"old-grade-id",
        |   "section_uuid":"old-class-id",
        |   "student_tags":"",
        |   "student_special_needs":"n/a",
        |   "student_active_until": null
        |}
        |]
              """.stripMargin
    val studentRel = spark.read.json(Seq(studentRelData).toDS())
    when(service.readFromRedshift[StudentRel](RedshiftStudentSink)).thenReturn(studentRel)

    val schoolGradeMovedEvent =
      """
        |[{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentMovedBetweenSchools",
        |   "occurredOn":"2019-03-10 02:40:00.0",
        |   "studentId" : "student-id",
        |   "loadtime":"wMC3yz1AAAAggyUA",
        |   "oldSchoolId":"old-school-id",
        |   "oldClassId":"old-class-id",
        |   "oldSectionId":"old-class-id",
        |   "oldGradeId": "old-grade-id",
        |   "oldK12Grade":6,
        |   "targetSchoolId":"new-school-id",
        |   "targetClassId":"new-class-id",
        |   "targetSectionId":"new-class-id",
        |   "targetGradeId":"new-grade-id",
        |   "targetK12Grade": 7
        |}]
              """.stripMargin
    val schoolGradeMoved = spark.read.json(Seq(schoolGradeMovedEvent).toDS())
    when(service.readOptional(ParquetStudentSchoolOrGradeMoveSource, sprk)).thenReturn(Some(schoolGradeMoved))

    when(service.readOptional(ParquetStudentPromotedSource, sprk)).thenReturn(None)
    when(service.readOptional(ParquetStudentSectionUpdatedSource, sprk)).thenReturn(None)

    val redshiftSchoolData =
      """
        |[{
        |   "school_status":1,
        |   "school_dw_id" : 1,
        |   "school_id":"school-id",
        |   "school_created_time": "2019-01-09 02:30:00.0"
        |}
        |]""".stripMargin
    val redshiftSchool = spark.read.json(Seq(redshiftSchoolData).toDS())
    when(service.readFromRedshift[SchoolDim](RedshiftSchoolSink, selectedCols = List("school_dw_id", "school_id"))).thenReturn(redshiftSchool)

    val redshiftGradeData =
      """
        |[{
        |   "grade_status":1,
        |   "grade_dw_id" : 1,
        |   "grade_id":"grade-id",
        |   "grade_created_time": "2019-01-09 02:30:00.0"
        |}
        |]""".stripMargin
    val redshiftGrade = spark.read.json(Seq(redshiftGradeData).toDS())
    when(service.readFromRedshift[GradeDim](RedshiftGradeSink, selectedCols = List("grade_dw_id", "grade_id"))).thenReturn(redshiftGrade)

    val redshiftSectionData =
      """
        |[{
        |   "section_status":1,
        |   "section_dw_id" : 1,
        |   "section_id":"class-id",
        |   "section_created_time": "2019-01-09 02:30:00.0"
        |}
        |]""".stripMargin
    val redshiftSection = spark.read.json(Seq(redshiftSectionData).toDS())
    when(service.readFromRedshift[SectionDim](RedshiftSectionSink, selectedCols = List("section_dw_id", "section_id"))).thenReturn(redshiftSection)

    val redshiftStudentData =
      """
        |[{
        |   "student_status":1,
        |   "student_id":"student-uuid2",
        |   "student_created_time": "2019-02-09 02:30:00.0",
        |   "student_username":"student-username",
        |   "student_school_dw_id" : "1",
        |   "student_grade_dw_id":"1",
        |   "student_section_dw_id":"1",
        |   "student_tags":"ELITE",
        |   "student_special_needs":"n/a",
        |   "student_active_until": null
        |}
        |]
              """.stripMargin
    val redshiftStudent = spark.read.json(Seq(redshiftStudentData).toDS())
    when(service.readFromRedshift[StudentDim](RedshiftDimStudentSink)).thenReturn(redshiftStudent)
    when(service.readOptional(ParquetStudentEnableDisableSource, sprk)).thenReturn(None)
    when(service.readOptional(ParquetStudentsDeletedSource, sprk)).thenReturn(None)

    val transformer = new StudentTransform(sprk, service)
    val df = transformer.transform().get.output

    assert(df.count == 1)
    assert(df.columns.toSet === studentDimExpectedColumns)
    assert[String](df, "student_uuid", "student-id")
    assert[String](df, "grade_uuid", "new-grade-id")
    assert[String](df, "section_uuid", "new-class-id")
    assert[String](df, "grade_uuid", "new-grade-id")
    assert[String](df, "student_username", "student-username")
    assert[String](df, "student_tags", "ELITE")
    assert[Int](df, "student_status", ActiveEnabled)
    assert[String](df, "student_created_time", "2019-03-10 02:40:00.0")
  }

  test("student grade movement event") {

    val sprk = spark
    import sprk.implicits._

    when(service.readOptional(ParquetStudentSource, sprk)).thenReturn(None)
    when(service.readOptional(ParquetStudentTagUpdatedSource, sprk)).thenReturn(None)

    val studentRelData =
      """
        |[{
        |   "student_status":1,
        |   "student_uuid":"student-id",
        |   "student_created_time": "2019-03-09 02:30:00.0",
        |   "student_username":"student-username",
        |   "school_uuid" : "old-school-id",
        |   "grade_uuid":"old-grade-id",
        |   "section_uuid":"old-class-id",
        |   "student_tags":"ELITE",
        |   "student_special_needs":"n/a",
        |   "student_active_until": null
        |},
        |{
        |   "student_status":2,
        |   "student_uuid":"student-id",
        |   "student_created_time": "2019-03-09 01:40:00.0",
        |   "student_username":"student-username1",
        |   "school_uuid" : "old-school-id1",
        |   "grade_uuid":"old-grade-id",
        |   "section_uuid":"old-class-id",
        |   "student_tags":"",
        |   "student_special_needs":"n/a",
        |   "student_active_until": null
        |}
        |]
              """.stripMargin
    val studentRel = spark.read.json(Seq(studentRelData).toDS())
    when(service.readFromRedshift[StudentRel](RedshiftStudentSink)).thenReturn(studentRel)

    val schoolGradeMovedEvent =
      """
        |[{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentMovedBetweenGrades",
        |   "occurredOn":"2019-03-10 02:40:00.0",
        |   "studentId" : "student-id",
        |   "loadtime":"wMC3yz1AAAAggyUA",
        |   "oldSchoolId":"old-school-id",
        |   "oldClassId":"old-class-id",
        |   "oldSectionId":"old-class-id",
        |   "oldGradeId": "old-grade-id",
        |   "oldK12Grade":6,
        |   "targetSchoolId":"old-school-id",
        |   "targetClassId":"new-class-id",
        |   "targetSectionId":"new-class-id",
        |   "targetGradeId":"new-grade-id",
        |   "targetK12Grade": 7
        |}]
              """.stripMargin
    val schoolGradeMoved = spark.read.json(Seq(schoolGradeMovedEvent).toDS())
    when(service.readOptional(ParquetStudentSchoolOrGradeMoveSource, sprk)).thenReturn(Some(schoolGradeMoved))

    when(service.readOptional(ParquetStudentPromotedSource, sprk)).thenReturn(None)
    when(service.readOptional(ParquetStudentSectionUpdatedSource, sprk)).thenReturn(None)

    val redshiftSchoolData =
      """
        |[{
        |   "school_status":1,
        |   "school_dw_id" : 1,
        |   "school_id":"school-id",
        |   "school_created_time": "2019-01-09 02:30:00.0"
        |}
        |]""".stripMargin
    val redshiftSchool = spark.read.json(Seq(redshiftSchoolData).toDS())
    when(service.readFromRedshift[SchoolDim](RedshiftSchoolSink, selectedCols = List("school_dw_id", "school_id"))).thenReturn(redshiftSchool)

    val redshiftGradeData =
      """
        |[{
        |   "grade_status":1,
        |   "grade_dw_id" : 1,
        |   "grade_id":"grade-id",
        |   "grade_created_time": "2019-01-09 02:30:00.0"
        |}
        |]""".stripMargin
    val redshiftGrade = spark.read.json(Seq(redshiftGradeData).toDS())
    when(service.readFromRedshift[GradeDim](RedshiftGradeSink, selectedCols = List("grade_dw_id", "grade_id"))).thenReturn(redshiftGrade)

    val redshiftSectionData =
      """
        |[{
        |   "section_status":1,
        |   "section_dw_id" : 1,
        |   "section_id":"class-id",
        |   "section_created_time": "2019-01-09 02:30:00.0"
        |}
        |]""".stripMargin
    val redshiftSection = spark.read.json(Seq(redshiftSectionData).toDS())
    when(service.readFromRedshift[SectionDim](RedshiftSectionSink, selectedCols = List("section_dw_id", "section_id"))).thenReturn(redshiftSection)

    val redshiftStudentData =
      """
        |[{
        |   "student_status":1,
        |   "student_id":"student-uuid2",
        |   "student_created_time": "2019-02-09 02:30:00.0",
        |   "student_username":"student-username",
        |   "student_school_dw_id" : "1",
        |   "student_grade_dw_id":"1",
        |   "student_section_dw_id":"1",
        |   "student_tags":"ELITE",
        |   "student_special_needs":"n/a",
        |   "student_active_until": null
        |}
        |]
              """.stripMargin
    val redshiftStudent = spark.read.json(Seq(redshiftStudentData).toDS())
    when(service.readFromRedshift[StudentDim](RedshiftDimStudentSink)).thenReturn(redshiftStudent)
    when(service.readOptional(ParquetStudentEnableDisableSource, sprk)).thenReturn(None)
    when(service.readOptional(ParquetStudentsDeletedSource, sprk)).thenReturn(None)

    val transformer = new StudentTransform(sprk, service)
    val df = transformer.transform().get.output

    assert(df.count == 1)
    assert(df.columns.toSet === studentDimExpectedColumns)
    assert[String](df, "student_uuid", "student-id")
    assert[String](df, "school_uuid", "old-school-id")
    assert[String](df, "section_uuid", "new-class-id")
    assert[String](df, "grade_uuid", "new-grade-id")
    assert[String](df, "student_username", "student-username")
    assert[String](df, "student_tags", "ELITE")
    assert[Int](df, "student_status", ActiveEnabled)
    assert[String](df, "student_created_time", "2019-03-10 02:40:00.0")
  }

  test("student grade and school movement together") {

    val sprk = spark
    import sprk.implicits._

    when(service.readOptional(ParquetStudentSource, sprk)).thenReturn(None)
    when(service.readOptional(ParquetStudentTagUpdatedSource, sprk)).thenReturn(None)

    val studentRelData =
      """
        |[{
        |   "student_status":1,
        |   "student_uuid":"student-id",
        |   "student_created_time": "2019-03-09 02:30:00.0",
        |   "student_username":"student-username",
        |   "school_uuid" : "old-school-id",
        |   "grade_uuid":"old-grade-id",
        |   "section_uuid":"old-class-id",
        |   "student_tags":"ELITE",
        |   "student_special_needs":"n/a",
        |   "student_active_until": null
        |},
        |{
        |   "student_status":2,
        |   "student_uuid":"student-id",
        |   "student_created_time": "2019-03-09 01:40:00.0",
        |   "student_username":"student-username1",
        |   "school_uuid" : "old-school-id1",
        |   "grade_uuid":"old-grade-id",
        |   "section_uuid":"old-class-id",
        |   "student_tags":"",
        |   "student_special_needs":"n/a",
        |   "student_active_until": null
        |}
        |]
              """.stripMargin
    val studentRel = spark.read.json(Seq(studentRelData).toDS())
    when(service.readFromRedshift[StudentRel](RedshiftStudentSink)).thenReturn(studentRel)

    val schoolGradeMovedEvent =
      """
        |[{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentMovedBetweenGrades",
        |   "occurredOn":"2019-03-10 02:40:00.0",
        |   "studentId" : "student-id",
        |   "loadtime":"wMC3yz1AAAAggyUA",
        |   "oldSchoolId":"old-school-id",
        |   "oldClassId":"old-class-id",
        |   "oldSectionId":"old-class-id",
        |   "oldGradeId": "old-grade-id",
        |   "oldK12Grade":6,
        |   "targetSchoolId":"new-school-id-0",
        |   "targetClassId":"new-class-id-0",
        |   "targetSectionId":"new-class-id-0",
        |   "targetGradeId":"new-grade-id-0",
        |   "targetK12Grade": 7
        |},
        |{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentMovedBetweenSchools",
        |   "occurredOn":"2019-03-12 02:40:00.0",
        |   "studentId" : "student-id",
        |   "loadtime":"wMC3yz1AAAAggyUA",
        |   "oldSchoolId":"new-school-id-0",
        |   "oldClassId":"new-class-id-0",
        |   "oldSectionId":"new-class-id-0",
        |   "oldGradeId": "new-grade-id-0",
        |   "oldK12Grade":6,
        |   "targetSchoolId":"new-school-id-0",
        |   "targetClassId":"new-class-id",
        |   "targetSectionId":"new-class-id",
        |   "targetGradeId":"new-grade-id",
        |   "targetK12Grade": 7
        |}]
              """.stripMargin
    val schoolGradeMoved = spark.read.json(Seq(schoolGradeMovedEvent).toDS())
    when(service.readOptional(ParquetStudentSchoolOrGradeMoveSource, sprk)).thenReturn(Some(schoolGradeMoved))

    when(service.readOptional(ParquetStudentPromotedSource, sprk)).thenReturn(None)
    when(service.readOptional(ParquetStudentSectionUpdatedSource, sprk)).thenReturn(None)

    val redshiftSchoolData =
      """
        |[{
        |   "school_status":1,
        |   "school_dw_id" : 1,
        |   "school_id":"school-id",
        |   "school_created_time": "2019-01-09 02:30:00.0"
        |}
        |] """.stripMargin
    val redshiftSchool = spark.read.json(Seq(redshiftSchoolData).toDS())
    when(service.readFromRedshift[SchoolDim](RedshiftSchoolSink, selectedCols = List("school_dw_id", "school_id"))).thenReturn(redshiftSchool)

    val redshiftGradeData =
      """
        |[{
        |   "grade_status":1,
        |   "grade_dw_id" : 1,
        |   "grade_id":"grade-id",
        |   "grade_created_time": "2019-01-09 02:30:00.0"
        |}
        |]""".stripMargin
    val redshiftGrade = spark.read.json(Seq(redshiftGradeData).toDS())
    when(service.readFromRedshift[GradeDim](RedshiftGradeSink, selectedCols = List("grade_dw_id", "grade_id"))).thenReturn(redshiftGrade)

    val redshiftSectionData =
      """
        |[{
        |   "section_status":1,
        |   "section_dw_id" : 1,
        |   "section_id":"class-id",
        |   "section_created_time": "2019-01-09 02:30:00.0"
        |}
        |]""".stripMargin
    val redshiftSection = spark.read.json(Seq(redshiftSectionData).toDS())
    when(service.readFromRedshift[SectionDim](RedshiftSectionSink, selectedCols = List("section_dw_id", "section_id"))).thenReturn(redshiftSection)

    val redshiftStudentData =
      """
        |[{
        |   "student_status":1,
        |   "student_id":"student-uuid2",
        |   "student_created_time": "2019-02-09 02:30:00.0",
        |   "student_username":"student-username",
        |   "student_school_dw_id" : "1",
        |   "student_grade_dw_id":"1",
        |   "student_section_dw_id":"1",
        |   "student_tags":"ELITE",
        |   "student_special_needs":"n/a",
        |   "student_active_until": null
        |}
        |]
              """.stripMargin
    val redshiftStudent = spark.read.json(Seq(redshiftStudentData).toDS())
    when(service.readFromRedshift[StudentDim](RedshiftDimStudentSink)).thenReturn(redshiftStudent)
    when(service.readOptional(ParquetStudentEnableDisableSource, sprk)).thenReturn(None)
    when(service.readOptional(ParquetStudentsDeletedSource, sprk)).thenReturn(None)

    val transformer = new StudentTransform(sprk, service)
    val df = transformer.transform().get.output

    assert(df.count == 1)
    assert(df.columns.toSet === studentDimExpectedColumns)
    assert[String](df, "student_uuid", "student-id")
    assert[String](df, "school_uuid", "new-school-id-0")
    assert[String](df, "section_uuid", "new-class-id")
    assert[String](df, "grade_uuid", "new-grade-id")
    assert[String](df, "student_username", "student-username")
    assert[String](df, "student_tags", "ELITE")
    assert[Int](df, "student_status", ActiveEnabled)
    assert[String](df, "student_created_time", "2019-03-12 02:40:00.0")

  }

  test("student grade, school movement and section updated events together") {
    val sprk = spark
    import sprk.implicits._

    when(service.readOptional(ParquetStudentSource, sprk)).thenReturn(None)
    when(service.readOptional(ParquetStudentTagUpdatedSource, sprk)).thenReturn(None)

    val studentRelData =
      """
        |[{
        |   "student_status":1,
        |   "student_uuid":"student-id",
        |   "student_created_time": "2019-03-09 02:30:00.0",
        |   "student_username":"student-username",
        |   "school_uuid" : "old-school-id",
        |   "grade_uuid":"old-grade-id",
        |   "section_uuid":"old-class-id",
        |   "student_tags":"ELITE",
        |   "student_special_needs":"n/a",
        |   "student_active_until": null
        |},
        |{
        |   "student_status":2,
        |   "student_uuid":"student-id",
        |   "student_created_time": "2019-03-09 01:40:00.0",
        |   "student_username":"student-username1",
        |   "school_uuid" : "old-school-id1",
        |   "grade_uuid":"old-grade-id",
        |   "section_uuid":"old-class-id",
        |   "student_tags":"",
        |   "student_special_needs":"n/a",
        |   "student_active_until": null
        |}
        |]
              """.stripMargin
    val studentRel = spark.read.json(Seq(studentRelData).toDS())
    when(service.readFromRedshift[StudentRel](RedshiftStudentSink)).thenReturn(studentRel)

    val schoolGradeMovedEvent =
      """
        |[{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentMovedBetweenGrades",
        |   "occurredOn":"2019-03-10 02:40:00.0",
        |   "studentId" : "student-id",
        |   "loadtime":"wMC3yz1AAAAggyUA",
        |   "oldSchoolId":"old-school-id",
        |   "oldClassId":"old-class-id",
        |   "oldSectionId":"old-class-id",
        |   "oldGradeId": "old-grade-id",
        |   "oldK12Grade":6,
        |   "targetSchoolId":"new-school-id-0",
        |   "targetClassId":"new-class-id-0",
        |   "targetSectionId":"new-class-id-0",
        |   "targetGradeId":"new-grade-id-0",
        |   "targetK12Grade": 7
        |},
        |{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentMovedBetweenSchools",
        |   "occurredOn":"2019-03-12 02:40:00.0",
        |   "studentId" : "student-id",
        |   "loadtime":"wMC3yz1AAAAggyUA",
        |   "oldSchoolId":"old-school-id",
        |   "oldClassId":"old-class-id",
        |   "oldSectionId":"old-class-id",
        |   "oldGradeId": "old-grade-id",
        |   "oldK12Grade":6,
        |   "targetSchoolId":"new-school-id",
        |   "targetClassId":"new-class-id",
        |   "targetSectionId":"new-class-id",
        |   "targetGradeId":"new-grade-id",
        |   "targetK12Grade": 7
        |}]
              """.stripMargin
    val schoolGradeMoved = spark.read.json(Seq(schoolGradeMovedEvent).toDS())
    when(service.readOptional(ParquetStudentSchoolOrGradeMoveSource, sprk)).thenReturn(Some(schoolGradeMoved))
    when(service.readOptional(ParquetStudentPromotedSource, sprk)).thenReturn(None)

    val sectionUpdateEvent =
      """
        |[{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentSectionUpdatedEvent",
        |   "createdOn":12345,
        |   "occurredOn":"2019-03-13 02:40:00.0",
        |   "id" : "student-id",
        |   "grade":"6",
        |   "gradeId": "latest-grade-id",
        |   "schoolId":"new-school-id",
        |   "oldSectionId":"old-class-id1",
        |   "newSectionId":"latest-class-id"
        |}
        |]
              """.stripMargin
    val sectionUpdated = spark.read.json(Seq(sectionUpdateEvent).toDS())
    when(service.readOptional(ParquetStudentSectionUpdatedSource, sprk)).thenReturn(Some(sectionUpdated))

    val redshiftSchoolData =
      """
        |[{
        |   "school_status":1,
        |   "school_dw_id" : 1,
        |   "school_id":"school-id",
        |   "school_created_time": "2019-01-09 02:30:00.0"
        |}
        |] """.stripMargin
    val redshiftSchool = spark.read.json(Seq(redshiftSchoolData).toDS())
    when(service.readFromRedshift[SchoolDim](RedshiftSchoolSink, selectedCols = List("school_dw_id", "school_id"))).thenReturn(redshiftSchool)

    val redshiftGradeData =
      """
        |[{
        |   "grade_status":1,
        |   "grade_dw_id" : 1,
        |   "grade_id":"grade-id",
        |   "grade_created_time": "2019-01-09 02:30:00.0"
        |}
        |]""".stripMargin
    val redshiftGrade = spark.read.json(Seq(redshiftGradeData).toDS())
    when(service.readFromRedshift[GradeDim](RedshiftGradeSink, selectedCols = List("grade_dw_id", "grade_id"))).thenReturn(redshiftGrade)

    val redshiftSectionData =
      """
        |[{
        |   "section_status":1,
        |   "section_dw_id" : 1,
        |   "section_id":"class-id",
        |   "section_created_time": "2019-01-09 02:30:00.0"
        |}
        |]""".stripMargin
    val redshiftSection = spark.read.json(Seq(redshiftSectionData).toDS())
    when(service.readFromRedshift[SectionDim](RedshiftSectionSink, selectedCols = List("section_dw_id", "section_id"))).thenReturn(redshiftSection)

    val redshiftStudentData =
      """
        |[{
        |   "student_status":1,
        |   "student_id":"student-uuid2",
        |   "student_created_time": "2019-02-09 02:30:00.0",
        |   "student_username":"student-username",
        |   "student_school_dw_id" : "1",
        |   "student_grade_dw_id":"1",
        |   "student_section_dw_id":"1",
        |   "student_tags":"ELITE",
        |   "student_special_needs":"n/a",
        |   "student_active_until": null
        |}
        |]
              """.stripMargin
    val redshiftStudent = spark.read.json(Seq(redshiftStudentData).toDS())
    when(service.readFromRedshift[StudentDim](RedshiftDimStudentSink)).thenReturn(redshiftStudent)
    when(service.readOptional(ParquetStudentEnableDisableSource, sprk)).thenReturn(None)
    when(service.readOptional(ParquetStudentsDeletedSource, sprk)).thenReturn(None)

    val transformer = new StudentTransform(sprk, service)
    val df = transformer.transform().get.output
    assert(df.count == 1)
    assert(df.columns.toSet === studentDimExpectedColumns)
    assert[String](df, "student_uuid", "student-id")
    assert[String](df, "grade_uuid", "latest-grade-id")
    assert[String](df, "section_uuid", "latest-class-id")
    assert[String](df, "school_uuid", "new-school-id")
    assert[String](df, "student_username", "student-username")
    assert[String](df, "student_tags", "ELITE")
    assert[Int](df, "student_status", ActiveEnabled)
    assert[String](df, "student_created_time", "2019-03-13 02:40:00.0")
  }

  test("student created, deleted, updated, enabled, disabled events") {
    val sprk = spark
    import sprk.implicits._

    val studentEvent =
      """
        |[{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentCreatedEvent",
        |   "createdOn":12345,
        |   "uuid":"student-created-id",
        |   "username":"student-created-username",
        |   "grade": "6",
        |   "gradeId" : "grade-id",
        |   "classId": "class-id",
        |   "sectionId": "class-id",
        |   "schoolId" : "school-id",
        |   "tags":["Test"],
        |   "specialNeeds":[],
        |   "loadtime":"wMC3yz1AAAAggyUA",
        |   "eventDateDw":"20181008",
        |   "occurredOn": "1970-07-14 02:40:00.0"
        |},
        |{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentUpdatedEvent",
        |   "createdOn":122,
        |   "uuid":"student-id1",
        |   "username":"student-updated-username",
        |   "grade": "6",
        |   "gradeId" : "grade-id",
        |   "classId": "class-id",
        |   "sectionId": "class-id",
        |   "schoolId" : "school-id",
        |   "tags":["Test"],
        |   "specialNeeds":[],
        |   "loadtime":"wMC3yz1AAAAggyUA",
        |   "eventDateDw":"20181008",
        |   "occurredOn": "1970-07-15 02:40:00.0"
        |},
        |{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentUpdatedEvent",
        |   "createdOn":123,
        |   "uuid":"student-id1",
        |   "username":"student-updated-username",
        |   "grade": "6",
        |   "gradeId" : "grade-id",
        |   "classId": "class-id",
        |   "schoolId" : "school-id",
        |   "tags":["Test"],
        |   "specialNeeds":[],
        |   "loadtime":"wMC3yz1AAAAggyUA",
        |   "eventDateDw":"20181008",
        |   "occurredOn": "1970-07-16 19:50:00.0"
        |}]""".stripMargin
    val student = spark.read.json(Seq(studentEvent).toDS())
    when(service.readOptional(ParquetStudentSource, sprk)).thenReturn(Some(student))

    val studentTagUpdatedEvent =
      """
        |[{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentTagUpdatedEvent",
        |   "occurredOn":"1970-07-14 02:40:00.0",
        |   "studentId" : "student-created-id",
        |   "tags":["ELITE"]
        |},
        |{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentTagUpdatedEvent",
        |   "occurredOn":"1970-07-15 02:40:00.0",
        |   "studentId" : "student-id1",
        |   "tags":["Elite"]
        |}
        |]
              """.stripMargin
    val studentTagUpdated = spark.read.json(Seq(studentTagUpdatedEvent).toDS())
    when(service.readOptional(ParquetStudentTagUpdatedSource, sprk)).thenReturn(Some(studentTagUpdated))

    val studentRelData =
      """
        |[{
        |   "student_status":1,
        |   "student_uuid":"deleted-student-1",
        |   "student_created_time": "1970-01-16 02:40:00.0",
        |   "student_username":"student-username",
        |   "school_uuid" : "old-school-id",
        |   "grade_uuid":"old-grade-id",
        |   "section_uuid":"old-class-id",
        |   "student_tags":"ELITE",
        |   "student_special_needs":"n/a",
        |   "student_active_until": null
        |},
        |{
        |   "student_status":1,
        |   "student_uuid":"deleted-student-2",
        |   "student_created_time": "1970-01-16 02:40:00.0",
        |   "student_username":"student-username1",
        |   "school_uuid" : "old-school-id1",
        |   "grade_uuid":"old-grade-id",
        |   "section_uuid":"old-class-id",
        |   "student_tags":"ELITE",
        |   "student_special_needs":"n/a",
        |   "student_active_until": null
        |},
        |{
        |   "student_status":1,
        |   "student_uuid":"student-uuid-3",
        |   "student_created_time": "1970-01-16 02:40:00.0",
        |   "student_username":"student-username",
        |   "school_uuid" : "old-school-id",
        |   "grade_uuid":"old-grade-id",
        |   "section_uuid":"old-class-id",
        |   "student_tags":"ELITE",
        |   "student_special_needs":"n/a",
        |   "student_active_until": null
        |}
        |]
              """.stripMargin
    val studentRel = spark.read.json(Seq(studentRelData).toDS())
    when(service.readFromRedshift[StudentRel](RedshiftStudentSink)).thenReturn(studentRel)

    when(service.readOptional(ParquetStudentSchoolOrGradeMoveSource, sprk)).thenReturn(None)
    when(service.readOptional(ParquetStudentPromotedSource, sprk)).thenReturn(None)

    val sectionUpdateEvent =
      """
        |[{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentSectionUpdatedEvent",
        |   "occurredOn":"1970-07-17 19:50:00.0",
        |   "id" : "student-id1",
        |   "grade":"6",
        |   "gradeId": "grade-id",
        |   "schoolId":"school-id",
        |   "oldSectionId":"old-class-id",
        |   "newSectionId":"class-id1"
        |}
        |]
              """.stripMargin
    val sectionUpdated = spark.read.json(Seq(sectionUpdateEvent).toDS())
    when(service.readOptional(ParquetStudentSectionUpdatedSource, sprk)).thenReturn(Some(sectionUpdated))

    val redshiftSchoolData =
      """
        |[{
        |   "school_status":1,
        |   "school_dw_id" : 1,
        |   "school_id":"school-id",
        |   "school_created_time": "1970-01-15 02:40:00.0"
        |}
        |]""".stripMargin
    val redshiftSchool = spark.read.json(Seq(redshiftSchoolData).toDS())
    when(service.readFromRedshift[SchoolDim](RedshiftSchoolSink, selectedCols = List("school_dw_id", "school_id"))).thenReturn(redshiftSchool)

    val redshiftGradeData =
      """
        |[{
        |   "grade_status":1,
        |   "grade_dw_id" : 1,
        |   "grade_id":"grade-id",
        |   "grade_created_time": "1970-01-15 02:40:00.0"
        |}
        |]""".stripMargin
    val redshiftGrade = spark.read.json(Seq(redshiftGradeData).toDS())
    when(service.readFromRedshift[GradeDim](RedshiftGradeSink, selectedCols = List("grade_dw_id", "grade_id"))).thenReturn(redshiftGrade)

    val redshiftSectionData =
      """
        |[{
        |   "section_status":1,
        |   "section_dw_id" : 1,
        |   "section_id":"class-id",
        |   "section_created_time": "1970-01-15 02:40:00.0"
        |}
        |]""".stripMargin
    val redshiftSection = spark.read.json(Seq(redshiftSectionData).toDS())
    when(service.readFromRedshift[SectionDim](RedshiftSectionSink, selectedCols = List("section_dw_id", "section_id"))).thenReturn(redshiftSection)

    val redshiftStudentData =
      """
        |[{
        |   "student_status":1,
        |   "student_id":"student-id1",
        |   "student_created_time": "1970-01-15 02:40:00.0",
        |   "student_username":"student-username",
        |   "student_school_dw_id" : "1",
        |   "student_grade_dw_id":"1",
        |   "student_section_dw_id":"1",
        |   "student_tags":"ELITE",
        |   "student_special_needs":"n/a",
        |   "student_active_until": null
        |}
        |]""".stripMargin
    val redshiftStudent = spark.read.json(Seq(redshiftStudentData).toDS())
    when(service.readFromRedshift[StudentDim](RedshiftDimStudentSink)).thenReturn(redshiftStudent)

    val studentEnabledDisabledEvent =
      """
        |[{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentEnabledEvent",
        |   "createdOn":12345,
        |   "schoolId" : "school-id",
        |   "loadtime":"wMC3yz1AAAAggyUA",
        |   "eventDateDw":"20181008",
        |   "uuid": "student-uuid-3",
        |   "k12Grade":6,
        |   "classId":"class-id",
        |   "sectionId":"class-id",
        |   "gradeId":"grade-id",
        |   "username":"student-u1",
        |   "occurredOn": "1970-08-20 03:40:00.0"
        |},
        |{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentDisabledEvent",
        |   "createdOn":12346,
        |   "schoolId" : "school-id",
        |   "loadtime":"wMC3yz1AAAAggyUA",
        |   "eventDateDw":"20181008",
        |   "uuid": "student-uuid-3",
        |   "k12Grade":6,
        |   "classId":"class-id",
        |   "sectionId":"class-id",
        |   "gradeId":"grade-id",
        |   "username":"student-u1",
        |   "occurredOn": "1970-08-21 02:40:00.0"
        |}]
              """.stripMargin
    val studentEnabledDisabled = spark.read.json(Seq(studentEnabledDisabledEvent).toDS())
    when(service.readOptional(ParquetStudentEnableDisableSource, sprk)).thenReturn(Some(studentEnabledDisabled))

    val studentDeletedEvent =
      """
        |[{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentsDeletedEvent",
        |   "createdOn":12345,
        |   "schoolId" : "school-id",
        |   "events" : [{"studentId": "deleted-student-1", "classId": "class-id-1","sectionId": "class-id-1", "gradeId" : "grade-id", "userName" : "del-uname-1"}, {"studentId": "deleted-student-2", "classId": "class-id-1","sectionId": "class-id-1", "gradeId" : "grade-id", "userName" : "del-uname-2"}],
        |   "loadtime":"wMC3yz1AAAAggyUA",
        |   "eventDateDw":"20181008",
        |   "occurredOn": "1970-07-14 08:40:00.0"
        |}]
              """.stripMargin
    val studentDeleted = spark.read.json(Seq(studentDeletedEvent).toDS())
    when(service.readOptional(ParquetStudentsDeletedSource, sprk)).thenReturn(Some(studentDeleted))

    val transformer = new StudentTransform(sprk, service)
    val df = transformer.transform().get.output

    assert(df.count() == 5)
    assert(df.columns.toSet === studentDimExpectedColumns)
    val stuCreateDf = df.filter(col("student_uuid") === "student-created-id")
    assert(stuCreateDf.count == 1)
    assert[String](stuCreateDf, "student_uuid", "student-created-id")
    assert[String](stuCreateDf, "student_username", "student-created-username")

    val stuLatestUpdateDf = df.filter(col("student_uuid") === "student-id1")
    assert(stuLatestUpdateDf.count == 1)
    assert[String](stuLatestUpdateDf, "student_uuid", "student-id1")
    assert[String](stuLatestUpdateDf, "student_username", "student-updated-username")
    assert[String](stuLatestUpdateDf, "section_uuid", "class-id1")
    assert[String](stuLatestUpdateDf, "grade_uuid", "grade-id")
    assert[String](stuLatestUpdateDf, "student_created_time", "1970-07-17 19:50:00.0")

    val enableDisableDf = df.filter(col("student_uuid") === "student-uuid-3")
    assert(enableDisableDf.count == 1)
    assert[String](enableDisableDf, "student_uuid", "student-uuid-3")
    assert[String](enableDisableDf, "student_username", "student-username")
    assert[String](enableDisableDf, "section_uuid", "old-class-id")
    assert[Int](enableDisableDf, "student_status", 3)
    assert[String](enableDisableDf, "student_created_time", "1970-08-21 02:40:00.0")

    val stuDeleteDf = df.filter(col("student_uuid").contains("deleted"))
    assert(stuDeleteDf.count == 2)
    val deletedStudents = stuDeleteDf.select(col("student_uuid")).as(Encoders.STRING).collectAsList()
    assert(deletedStudents.size == 2)
    assert(deletedStudents.toArray.toSet == Set("deleted-student-1", "deleted-student-2"))
  }

  test("student grade movement and disable event") {
    val sprk = spark
    import sprk.implicits._

    when(service.readOptional(ParquetStudentSource, sprk)).thenReturn(None)
    when(service.readOptional(ParquetStudentTagUpdatedSource, sprk)).thenReturn(None)

    val studentRelData =
      """
        |[{
        |   "student_status":1,
        |   "student_uuid":"student-id",
        |   "student_created_time": "2019-03-09 02:30:00.0",
        |   "student_username":"student-username",
        |   "school_uuid" : "old-school-id",
        |   "grade_uuid":"old-grade-id",
        |   "section_uuid":"old-class-id",
        |   "student_tags":"ELITE",
        |   "student_special_needs":"n/a",
        |   "student_active_until": null
        |},
        |{
        |   "student_status":2,
        |   "student_uuid":"student-id",
        |   "student_created_time": "2019-03-09 01:40:00.0",
        |   "student_username":"student-username1",
        |   "school_uuid" : "old-school-id1",
        |   "grade_uuid":"old-grade-id",
        |   "section_uuid":"old-class-id",
        |   "student_tags":"",
        |   "student_special_needs":"n/a",
        |   "student_active_until": null
        |}
        |]
              """.stripMargin
    val studentRel = spark.read.json(Seq(studentRelData).toDS())
    when(service.readFromRedshift[StudentRel](RedshiftStudentSink)).thenReturn(studentRel)

    val studentSchoolOrGradeMoveEvent =
      """
        |[{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentMovedBetweenGrades",
        |   "occurredOn":"2019-03-10 02:40:00.0",
        |   "studentId" : "student-id",
        |   "loadtime":"wMC3yz1AAAAggyUA",
        |   "oldSchoolId":"old-school-id",
        |   "oldClassId":"old-class-id",
        |   "oldSectionId":"old-class-id",
        |   "oldGradeId": "old-grade-id",
        |   "oldK12Grade":6,
        |   "targetSchoolId":"new-school-id",
        |   "targetClassId":"new-class-id",
        |   "targetSectionId":"new-class-id",
        |   "targetGradeId":"new-grade-id",
        |   "targetK12Grade": 7
        |}]
              """.stripMargin
    val studentSchoolGrade = spark.read.json(Seq(studentSchoolOrGradeMoveEvent).toDS())
    when(service.readOptional(ParquetStudentSchoolOrGradeMoveSource, sprk)).thenReturn(Some(studentSchoolGrade))

    when(service.readOptional(ParquetStudentPromotedSource, sprk)).thenReturn(None)

    when(service.readOptional(ParquetStudentSectionUpdatedSource, sprk)).thenReturn(None)

    val redshiftSchoolData =
      """
        |[{
        |   "school_status":1,
        |   "school_dw_id" : 1,
        |   "school_id":"school-id",
        |   "school_created_time": "1970-01-15 02:40:00.0"
        |}
        |]""".stripMargin
    val redshiftSchool = spark.read.json(Seq(redshiftSchoolData).toDS())
    when(service.readFromRedshift[SchoolDim](RedshiftSchoolSink, selectedCols = List("school_dw_id", "school_id"))).thenReturn(redshiftSchool)

    val redshiftGradeData =
      """
        |[{
        |   "grade_status":1,
        |   "grade_dw_id" : 1,
        |   "grade_id":"grade-id",
        |   "grade_created_time": "1970-01-15 02:40:00.0"
        |}
        |]""".stripMargin
    val redshiftGrade = spark.read.json(Seq(redshiftGradeData).toDS())
    when(service.readFromRedshift[GradeDim](RedshiftGradeSink, selectedCols = List("grade_dw_id", "grade_id"))).thenReturn(redshiftGrade)

    val redshiftSectionData =
      """
        |[{
        |   "section_status":1,
        |   "section_dw_id" : 1,
        |   "section_id":"class-id",
        |   "section_created_time": "1970-01-15 02:40:00.0"
        |}
        |]
              """.stripMargin
    val redshiftSection = spark.read.json(Seq(redshiftSectionData).toDS())
    when(service.readFromRedshift[SectionDim](RedshiftSectionSink, selectedCols = List("section_dw_id", "section_id"))).thenReturn(redshiftSection)

    val redshiftStudentData =
      """
        |[{
        |   "student_status":1,
        |   "student_id":"student-id1",
        |   "student_created_time": "1970-01-15 02:40:00.0",
        |   "student_username":"student-username",
        |   "student_school_dw_id" : "1",
        |   "student_grade_dw_id":"1",
        |   "student_section_dw_id":"1",
        |   "student_tags":"ELITE",
        |   "student_special_needs":"n/a",
        |   "student_active_until": null
        |}
        |]""".stripMargin
    val redshiftStudent = spark.read.json(Seq(redshiftStudentData).toDS())
    when(service.readFromRedshift[StudentDim](RedshiftDimStudentSink)).thenReturn(redshiftStudent)

    val studentEnabledDisabledEvent =
      """[{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentDisabledEvent",
        |   "createdOn":12345,
        |   "schoolId" : "school-id",
        |   "loadtime":"wMC3yz1AAAAggyUA",
        |   "eventDateDw":"20181008",
        |   "uuid": "student-id",
        |   "k12Grade":6,
        |   "classId":"class-id",
        |   "sectionId":"class-id",
        |   "gradeId":"grade-id",
        |   "username":"student-u1",
        |   "occurredOn": "2019-03-11 02:40:00.0"
        |}
        |]""".stripMargin
    val studentEnabledDisabled = spark.read.json(Seq(studentEnabledDisabledEvent).toDS())
    when(service.readOptional(ParquetStudentEnableDisableSource, sprk)).thenReturn(Some(studentEnabledDisabled))

    val studentDeletedEvent =
      """
        |[{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentsDeletedEvent",
        |   "createdOn":12345,
        |   "schoolId" : "school-id",
        |   "events" : [{"studentId": "deleted-student-1", "classId": "class-id-1","sectionId": "class-id-1", "gradeId" : "grade-id", "userName" : "del-uname-1"}, {"studentId": "deleted-student-2", "classId": "class-id-1","sectionId": "class-id-1", "gradeId" : "grade-id", "userName" : "del-uname-2"}],
        |   "loadtime":"wMC3yz1AAAAggyUA",
        |   "eventDateDw":"20181008",
        |   "occurredOn": "1970-07-14 08:40:00.0"
        |}]
              """.stripMargin
    val studentDeleted = spark.read.json(Seq(studentDeletedEvent).toDS())
    when(service.readOptional(ParquetStudentsDeletedSource, sprk)).thenReturn(Some(studentDeleted))

    val transformer = new StudentTransform(sprk, service)
    val df = transformer.transform().get.output
    assert(df.count == 1)
    assert(df.columns.toSet === studentDimExpectedColumns)
    assert[String](df, "student_uuid", "student-id")
    assert[String](df, "grade_uuid", "new-grade-id")
    assert[String](df, "section_uuid", "new-class-id")
    assert[String](df, "grade_uuid", "new-grade-id")
    assert[String](df, "student_username", "student-username")
    assert[String](df, "student_tags", "ELITE")
    assert[Int](df, "student_status", Disabled)
  }

  test("student grade movement multiple event") {
    val sprk = spark
    import sprk.implicits._

    when(service.readOptional(ParquetStudentSource, sprk)).thenReturn(None)
    when(service.readOptional(ParquetStudentTagUpdatedSource, sprk)).thenReturn(None)

    val studentSchoolOrGradeMoveEvent =
      """
        |[{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentMovedBetweenGrades",
        |   "occurredOn":"2019-03-10 02:40:00.0",
        |   "studentId" : "student-id",
        |   "loadtime":"wMC3yz1AAAAggyUA",
        |   "oldSchoolId":"old-school-id",
        |   "oldClassId":"old-class-id",
        |   "oldSectionId":"old-class-id",
        |   "oldGradeId": "old-grade-id",
        |   "oldK12Grade":6,
        |   "targetSchoolId":"new-school-id",
        |   "targetClassId":"new-class-id",
        |   "targetSectionId":"new-class-id",
        |   "targetGradeId":"new-grade-id",
        |   "targetK12Grade": 7
        |},
        |{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentMovedBetweenGrades",
        |   "occurredOn":"2019-03-10 02:41:00.0",
        |   "studentId" : "student-id",
        |   "loadtime":"wMC3yz1AAAAggyUA",
        |   "oldSchoolId":"old-school-id",
        |   "oldClassId":"old-class-id",
        |   "oldSectionId":"old-class-id",
        |   "oldGradeId": "old-grade-id",
        |   "oldK12Grade":6,
        |   "targetSchoolId":"new-school-id1",
        |   "targetClassId":"new-class-id1",
        |   "targetSectionId":"new-class-id1",
        |   "targetGradeId":"new-grade-id1",
        |   "targetK12Grade": 7
        |}]
              """.stripMargin
    val studentSchoolGrade = spark.read.json(Seq(studentSchoolOrGradeMoveEvent).toDS())
    when(service.readOptional(ParquetStudentSchoolOrGradeMoveSource, sprk)).thenReturn(Some(studentSchoolGrade))

    when(service.readOptional(ParquetStudentPromotedSource, sprk)).thenReturn(None)
    when(service.readOptional(ParquetStudentSectionUpdatedSource, sprk)).thenReturn(None)
    when(service.readOptional(ParquetStudentEnableDisableSource, sprk)).thenReturn(None)
    when(service.readOptional(ParquetStudentsDeletedSource, sprk)).thenReturn(None)

    val studentRelData =
      """
        |[{
        |   "student_status":1,
        |   "student_uuid":"student-id",
        |   "student_created_time": "2019-03-09 02:30:00.0",
        |   "student_username":"student-username",
        |   "school_uuid" : "old-school-id",
        |   "grade_uuid":"old-grade-id",
        |   "section_uuid":"old-class-id",
        |   "student_tags":"ELITE",
        |   "student_special_needs":"n/a",
        |   "student_active_until": null
        |},
        |{
        |   "student_status":2,
        |   "student_uuid":"student-id",
        |   "student_created_time": "2019-03-09 01:40:00.0",
        |   "student_username":"student-username1",
        |   "school_uuid" : "old-school-id1",
        |   "grade_uuid":"old-grade-id",
        |   "section_uuid":"old-class-id",
        |   "student_tags":"",
        |   "student_special_needs":"n/a",
        |   "student_active_until": null
        |}
        |]
              """.stripMargin
    val studentRel = spark.read.json(Seq(studentRelData).toDS())
    when(service.readFromRedshift[StudentRel](RedshiftStudentSink)).thenReturn(studentRel)
    val redshiftSchoolData =
      """
        |[{
        |   "school_status":1,
        |   "school_dw_id" : 1,
        |   "school_id":"school-id",
        |   "school_created_time": "1970-01-15 02:40:00.0"
        |}
        |]""".stripMargin
    val redshiftSchool = spark.read.json(Seq(redshiftSchoolData).toDS())
    when(service.readFromRedshift[SchoolDim](RedshiftSchoolSink, selectedCols = List("school_dw_id", "school_id"))).thenReturn(redshiftSchool)

    val redshiftGradeData =
      """
        |[{
        |   "grade_status":1,
        |   "grade_dw_id" : 1,
        |   "grade_id":"grade-id",
        |   "grade_created_time": "1970-01-15 02:40:00.0"
        |}
        |]""".stripMargin
    val redshiftGrade = spark.read.json(Seq(redshiftGradeData).toDS())
    when(service.readFromRedshift[GradeDim](RedshiftGradeSink, selectedCols = List("grade_dw_id", "grade_id"))).thenReturn(redshiftGrade)

    val redshiftSectionData =
      """
        |[{
        |   "section_status":1,
        |   "section_dw_id" : 1,
        |   "section_id":"class-id",
        |   "section_created_time": "1970-01-15 02:40:00.0"
        |}
        |]
              """.stripMargin
    val redshiftSection = spark.read.json(Seq(redshiftSectionData).toDS())
    when(service.readFromRedshift[SectionDim](RedshiftSectionSink, selectedCols = List("section_dw_id", "section_id"))).thenReturn(redshiftSection)

    val redshiftStudentData =
      """
        |[{
        |   "student_status":1,
        |   "student_id":"student-id1",
        |   "student_created_time": "1970-01-15 02:40:00.0",
        |   "student_username":"student-username",
        |   "student_school_dw_id" : "1",
        |   "student_grade_dw_id":"1",
        |   "student_section_dw_id":"1",
        |   "student_tags":"ELITE",
        |   "student_special_needs":"n/a",
        |   "student_active_until": null
        |}
        |]""".stripMargin
    val redshiftStudent = spark.read.json(Seq(redshiftStudentData).toDS())
    when(service.readFromRedshift[StudentDim](RedshiftDimStudentSink)).thenReturn(redshiftStudent)

    val transformer = new StudentTransform(sprk, service)
    val df = transformer.transform().get.output
    assert(df.count == 1)
    assert(df.columns.toSet === studentDimExpectedColumns)
    assert[String](df, "student_uuid", "student-id")
    assert[String](df, "grade_uuid", "new-grade-id1")
    assert[String](df, "section_uuid", "new-class-id1")
    assert[String](df, "grade_uuid", "new-grade-id1")
    assert[String](df, "student_username", "student-username")
    assert[String](df, "student_tags", "ELITE")
    assert[Int](df, "student_status", ActiveEnabled)
  }

  test("student grade and school movement multiple events together") {
    val sprk = spark
    import sprk.implicits._

    when(service.readOptional(ParquetStudentSource, sprk)).thenReturn(None)
    when(service.readOptional(ParquetStudentTagUpdatedSource, sprk)).thenReturn(None)

    val studentSchoolOrGradeMoveEvent =
      """
        |[{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentMovedBetweenGrades",
        |   "occurredOn":"2019-03-10 02:40:00.0",
        |   "studentId" : "student-id",
        |   "loadtime":"wMC3yz1AAAAggyUA",
        |   "oldSchoolId":"old-school-id",
        |   "oldClassId":"old-class-id",
        |   "oldSectionId":"old-class-id",
        |   "oldGradeId": "old-grade-id",
        |   "oldK12Grade":6,
        |   "targetSchoolId":"new-school-id-0",
        |   "targetClassId":"new-class-id-0",
        |   "targetSectionId":"new-class-id-0",
        |   "targetGradeId":"new-grade-id-0",
        |   "targetK12Grade": 7
        |},
        |{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentMovedBetweenGrades",
        |   "occurredOn":"2019-03-10 01:40:00.0",
        |   "studentId" : "student-id",
        |   "loadtime":"wMC3yz1AAAAggyUA",
        |   "oldSchoolId":"old-school-id",
        |   "oldClassId":"old-class-id",
        |   "oldSectionId":"old-class-id",
        |   "oldGradeId": "old-grade-id",
        |   "oldK12Grade":6,
        |   "targetSchoolId":"new-school-id-0",
        |   "targetClassId":"new-class-id-0",
        |   "targetSectionId":"new-class-id-0",
        |   "targetGradeId":"new-grade-id-0",
        |   "targetK12Grade": 7
        |},
        |{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentMovedBetweenSchools",
        |   "occurredOn":"2019-03-10 02:38:00.0",
        |   "studentId" : "student-id",
        |   "loadtime":"wMC3yz1AAAAggyUA",
        |   "oldSchoolId":"old-school-id",
        |   "oldClassId":"old-class-id",
        |   "oldSectionId":"old-class-id",
        |   "oldGradeId": "old-grade-id",
        |   "oldK12Grade":6,
        |   "targetSchoolId":"new-school-id-0",
        |   "targetClassId":"new-class-id-0",
        |   "targetSectionId":"new-class-id-0",
        |   "targetGradeId":"new-grade-id-0",
        |   "targetK12Grade": 7
        |},
        |{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentMovedBetweenSchools",
        |   "occurredOn":"2019-03-12 02:40:00.0",
        |   "studentId" : "student-id",
        |   "loadtime":"wMC3yz1AAAAggyUA",
        |   "oldSchoolId":"old-school-id",
        |   "oldClassId":"old-class-id",
        |   "oldSectionId":"old-class-id",
        |   "oldGradeId": "old-grade-id",
        |   "oldK12Grade":6,
        |   "targetSchoolId":"new-school-id",
        |   "targetClassId":"new-class-id",
        |   "targetSectionId":"new-class-id",
        |   "targetGradeId":"new-grade-id",
        |   "targetK12Grade": 7
        |}]
              """.stripMargin
    val studentSchoolGrade = spark.read.json(Seq(studentSchoolOrGradeMoveEvent).toDS())
    when(service.readOptional(ParquetStudentSchoolOrGradeMoveSource, sprk)).thenReturn(Some(studentSchoolGrade))
    when(service.readOptional(ParquetStudentPromotedSource, sprk)).thenReturn(None)
    when(service.readOptional(ParquetStudentSectionUpdatedSource, sprk)).thenReturn(None)
    when(service.readOptional(ParquetStudentEnableDisableSource, sprk)).thenReturn(None)
    when(service.readOptional(ParquetStudentsDeletedSource, sprk)).thenReturn(None)

    val studentRelData =
      """
        |[{
        |   "student_status":1,
        |   "student_uuid":"student-id",
        |   "student_created_time": "2019-03-09 02:30:00.0",
        |   "student_username":"student-username",
        |   "school_uuid" : "old-school-id",
        |   "grade_uuid":"old-grade-id",
        |   "section_uuid":"old-class-id",
        |   "student_tags":"ELITE",
        |   "student_special_needs":"n/a",
        |   "student_active_until": null
        |},
        |{
        |   "student_status":2,
        |   "student_uuid":"student-id",
        |   "student_created_time": "2019-03-09 01:40:00.0",
        |   "student_username":"student-username1",
        |   "school_uuid" : "old-school-id1",
        |   "grade_uuid":"old-grade-id",
        |   "section_uuid":"old-class-id",
        |   "student_tags":"",
        |   "student_special_needs":"n/a",
        |   "student_active_until": null
        |}
        |]
              """.stripMargin
    val studentRel = spark.read.json(Seq(studentRelData).toDS())
    when(service.readFromRedshift[StudentRel](RedshiftStudentSink)).thenReturn(studentRel)
    val redshiftSchoolData =
      """
        |[{
        |   "school_status":1,
        |   "school_dw_id" : 1,
        |   "school_id":"school-id",
        |   "school_created_time": "1970-01-15 02:40:00.0"
        |}
        |]""".stripMargin
    val redshiftSchool = spark.read.json(Seq(redshiftSchoolData).toDS())
    when(service.readFromRedshift[SchoolDim](RedshiftSchoolSink, selectedCols = List("school_dw_id", "school_id"))).thenReturn(redshiftSchool)

    val redshiftGradeData =
      """
        |[{
        |   "grade_status":1,
        |   "grade_dw_id" : 1,
        |   "grade_id":"grade-id",
        |   "grade_created_time": "1970-01-15 02:40:00.0"
        |}
        |]""".stripMargin
    val redshiftGrade = spark.read.json(Seq(redshiftGradeData).toDS())
    when(service.readFromRedshift[GradeDim](RedshiftGradeSink, selectedCols = List("grade_dw_id", "grade_id"))).thenReturn(redshiftGrade)

    val redshiftSectionData =
      """
        |[{
        |   "section_status":1,
        |   "section_dw_id" : 1,
        |   "section_id":"class-id",
        |   "section_created_time": "1970-01-15 02:40:00.0"
        |}
        |]
              """.stripMargin
    val redshiftSection = spark.read.json(Seq(redshiftSectionData).toDS())
    when(service.readFromRedshift[SectionDim](RedshiftSectionSink, selectedCols = List("section_dw_id", "section_id"))).thenReturn(redshiftSection)

    val redshiftStudentData =
      """
        |[{
        |   "student_status":1,
        |   "student_id":"student-id1",
        |   "student_created_time": "1970-01-15 02:40:00.0",
        |   "student_username":"student-username",
        |   "student_school_dw_id" : "1",
        |   "student_grade_dw_id":"1",
        |   "student_section_dw_id":"1",
        |   "student_tags":"ELITE",
        |   "student_special_needs":"n/a",
        |   "student_active_until": null
        |}
        |]""".stripMargin
    val redshiftStudent = spark.read.json(Seq(redshiftStudentData).toDS())
    when(service.readFromRedshift[StudentDim](RedshiftDimStudentSink)).thenReturn(redshiftStudent)

    val transformer = new StudentTransform(sprk, service)
    val df = transformer.transform().get.output

    assert(df.count == 1)
    assert(df.columns.toSet === studentDimExpectedColumns)
    assert[String](df, "student_uuid", "student-id")
    assert[String](df, "grade_uuid", "new-grade-id")
    assert[String](df, "section_uuid", "new-class-id")
    assert[String](df, "grade_uuid", "new-grade-id")
    assert[String](df, "student_username", "student-username")
    assert[String](df, "student_tags", "ELITE")
    assert[Int](df, "student_status", ActiveEnabled)
  }

  test("student school movement multiple events") {
    val sprk = spark
    import sprk.implicits._

    when(service.readOptional(ParquetStudentSource, sprk)).thenReturn(None)
    when(service.readOptional(ParquetStudentTagUpdatedSource, sprk)).thenReturn(None)

    val studentSchoolOrGradeMoveEvent =
      """
        |[{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentMovedBetweenSchools",
        |   "occurredOn":"2019-03-10 02:40:00.0",
        |   "studentId" : "student-id",
        |   "loadtime":"wMC3yz1AAAAggyUA",
        |   "oldSchoolId":"old-school-id",
        |   "oldClassId":"old-class-id",
        |   "oldSectionId":"old-class-id",
        |   "oldGradeId": "old-grade-id",
        |   "oldK12Grade":6,
        |   "targetSchoolId":"new-school-id",
        |   "targetClassId":"new-class-id",
        |   "targetSectionId":"new-class-id",
        |   "targetGradeId":"new-grade-id",
        |   "targetK12Grade": 7
        |},
        |{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentMovedBetweenSchools",
        |   "occurredOn":"2019-03-10 02:40:00.1",
        |   "studentId" : "student-id",
        |   "loadtime":"wMC3yz1AAAAggyUA",
        |   "oldSchoolId":"old-school-id",
        |   "oldClassId":"old-class-id",
        |   "oldSectionId":"old-class-id",
        |   "oldGradeId": "old-grade-id",
        |   "oldK12Grade":6,
        |   "targetSchoolId":"new-school-id1",
        |   "targetClassId":"new-class-id1",
        |   "targetSectionId":"new-class-id1",
        |   "targetGradeId":"new-grade-id1",
        |   "targetK12Grade": 7
        |}]
              """.stripMargin
    val studentSchoolGrade = spark.read.json(Seq(studentSchoolOrGradeMoveEvent).toDS())
    when(service.readOptional(ParquetStudentSchoolOrGradeMoveSource, sprk)).thenReturn(Some(studentSchoolGrade))
    when(service.readOptional(ParquetStudentPromotedSource, sprk)).thenReturn(None)
    when(service.readOptional(ParquetStudentSectionUpdatedSource, sprk)).thenReturn(None)
    when(service.readOptional(ParquetStudentEnableDisableSource, sprk)).thenReturn(None)
    when(service.readOptional(ParquetStudentsDeletedSource, sprk)).thenReturn(None)

    val studentRelData =
      """
        |[{
        |   "student_status":1,
        |   "student_uuid":"student-id",
        |   "student_created_time": "2019-03-09 02:30:00.0",
        |   "student_username":"student-username",
        |   "school_uuid" : "old-school-id",
        |   "grade_uuid":"old-grade-id",
        |   "section_uuid":"old-class-id",
        |   "student_tags":"ELITE",
        |   "student_special_needs":"n/a",
        |   "student_active_until": null
        |},
        |{
        |   "student_status":2,
        |   "student_uuid":"student-id",
        |   "student_created_time": "2019-03-09 01:40:00.0",
        |   "student_username":"student-username1",
        |   "school_uuid" : "old-school-id1",
        |   "grade_uuid":"old-grade-id",
        |   "section_uuid":"old-class-id",
        |   "student_tags":"",
        |   "student_special_needs":"n/a",
        |   "student_active_until": null
        |}
        |]
              """.stripMargin
    val studentRel = spark.read.json(Seq(studentRelData).toDS())
    when(service.readFromRedshift[StudentRel](RedshiftStudentSink)).thenReturn(studentRel)
    val redshiftSchoolData =
      """
        |[{
        |   "school_status":1,
        |   "school_dw_id" : 1,
        |   "school_id":"school-id",
        |   "school_created_time": "1970-01-15 02:40:00.0"
        |}
        |]""".stripMargin
    val redshiftSchool = spark.read.json(Seq(redshiftSchoolData).toDS())
    when(service.readFromRedshift[SchoolDim](RedshiftSchoolSink, selectedCols = List("school_dw_id", "school_id"))).thenReturn(redshiftSchool)

    val redshiftGradeData =
      """
        |[{
        |   "grade_status":1,
        |   "grade_dw_id" : 1,
        |   "grade_id":"grade-id",
        |   "grade_created_time": "1970-01-15 02:40:00.0"
        |}
        |]""".stripMargin
    val redshiftGrade = spark.read.json(Seq(redshiftGradeData).toDS())
    when(service.readFromRedshift[GradeDim](RedshiftGradeSink, selectedCols = List("grade_dw_id", "grade_id"))).thenReturn(redshiftGrade)

    val redshiftSectionData =
      """
        |[{
        |   "section_status":1,
        |   "section_dw_id" : 1,
        |   "section_id":"class-id",
        |   "section_created_time": "1970-01-15 02:40:00.0"
        |}
        |]
              """.stripMargin
    val redshiftSection = spark.read.json(Seq(redshiftSectionData).toDS())
    when(service.readFromRedshift[SectionDim](RedshiftSectionSink, selectedCols = List("section_dw_id", "section_id"))).thenReturn(redshiftSection)

    val redshiftStudentData =
      """
        |[{
        |   "student_status":1,
        |   "student_id":"student-id1",
        |   "student_created_time": "1970-01-15 02:40:00.0",
        |   "student_username":"student-username",
        |   "student_school_dw_id" : "1",
        |   "student_grade_dw_id":"1",
        |   "student_section_dw_id":"1",
        |   "student_tags":"ELITE",
        |   "student_special_needs":"n/a",
        |   "student_active_until": null
        |}
        |]""".stripMargin
    val redshiftStudent = spark.read.json(Seq(redshiftStudentData).toDS())
    when(service.readFromRedshift[StudentDim](RedshiftDimStudentSink)).thenReturn(redshiftStudent)

    val transformer = new StudentTransform(sprk, service)
    val df = transformer.transform().get.output

    assert(df.count == 1)
    assert(df.columns.toSet === studentDimExpectedColumns)
    assert[String](df, "student_uuid", "student-id")
    assert[String](df, "grade_uuid", "new-grade-id1")
    assert[String](df, "section_uuid", "new-class-id1")
    assert[String](df, "grade_uuid", "new-grade-id1")
    assert[String](df, "student_username", "student-username")
    assert[String](df, "student_tags", "ELITE")
    assert[Int](df, "student_status", ActiveEnabled)
  }

  test("multiple student grade movement events") {
    val sprk = spark
    import sprk.implicits._

    when(service.readOptional(ParquetStudentSource, sprk)).thenReturn(None)
    when(service.readOptional(ParquetStudentTagUpdatedSource, sprk)).thenReturn(None)

    val studentSchoolOrGradeMoveEvent =
      """
        |[{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentMovedBetweenGrades",
        |   "occurredOn":"2019-03-10 02:40:00.0",
        |   "studentId" : "student-id",
        |   "loadtime":"wMC3yz1AAAAggyUA",
        |   "oldSchoolId":"old-school-id",
        |   "oldClassId":"old-class-id",
        |   "oldSectionId":"old-class-id",
        |   "oldGradeId": "old-grade-id",
        |   "oldK12Grade":6,
        |   "targetSchoolId":"new-school-id",
        |   "targetClassId":"new-class-id",
        |   "targetSectionId":"new-class-id",
        |   "targetGradeId":"new-grade-id",
        |   "targetK12Grade": 7
        |},
        |{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentMovedBetweenSchools",
        |   "occurredOn":"2019-03-10 02:41:00.0",
        |   "studentId" : "student-id1",
        |   "loadtime":"wMC3yz1AAAAggyUA",
        |   "oldSchoolId":"old-school-id",
        |   "oldClassId":"old-class-id",
        |   "oldSectionId":"old-class-id",
        |   "oldGradeId": "old-grade-id",
        |   "oldK12Grade":6,
        |   "targetSchoolId":"new-school-id",
        |   "targetClassId":"new-class-id",
        |   "targetSectionId":"new-class-id",
        |   "targetGradeId":"new-grade-id",
        |   "targetK12Grade": 7
        |}]
              """.stripMargin
    val studentSchoolGrade = spark.read.json(Seq(studentSchoolOrGradeMoveEvent).toDS())
    when(service.readOptional(ParquetStudentSchoolOrGradeMoveSource, sprk)).thenReturn(Some(studentSchoolGrade))
    when(service.readOptional(ParquetStudentPromotedSource, sprk)).thenReturn(None)
    when(service.readOptional(ParquetStudentSectionUpdatedSource, sprk)).thenReturn(None)
    when(service.readOptional(ParquetStudentEnableDisableSource, sprk)).thenReturn(None)
    when(service.readOptional(ParquetStudentsDeletedSource, sprk)).thenReturn(None)

    val studentRelData =
      """
        |[{
        |   "student_status":1,
        |   "student_uuid":"student-id",
        |   "student_created_time": "2019-03-09 02:30:00.0",
        |   "student_username":"student-username",
        |   "school_uuid" : "old-school-id",
        |   "grade_uuid":"old-grade-id",
        |   "section_uuid":"old-class-id",
        |   "student_tags":"ELITE",
        |   "student_special_needs":"n/a",
        |   "student_active_until": null
        |},
        |{
        |   "student_status":1,
        |   "student_uuid":"student-id1",
        |   "student_created_time": "2019-03-09 01:40:00.0",
        |   "student_username":"student-username1",
        |   "school_uuid" : "old-school-id",
        |   "grade_uuid":"old-grade-id",
        |   "section_uuid":"old-class-id",
        |   "student_tags":"",
        |   "student_special_needs":"n/a",
        |   "student_active_until": null
        |}
        |]
              """.stripMargin
    val studentRel = spark.read.json(Seq(studentRelData).toDS())
    when(service.readFromRedshift[StudentRel](RedshiftStudentSink)).thenReturn(studentRel)
    val redshiftSchoolData =
      """
        |[{
        |   "school_status":1,
        |   "school_dw_id" : 1,
        |   "school_id":"school-id",
        |   "school_created_time": "1970-01-15 02:40:00.0"
        |}
        |]""".stripMargin
    val redshiftSchool = spark.read.json(Seq(redshiftSchoolData).toDS())
    when(service.readFromRedshift[SchoolDim](RedshiftSchoolSink, selectedCols = List("school_dw_id", "school_id"))).thenReturn(redshiftSchool)

    val redshiftGradeData =
      """
        |[{
        |   "grade_status":1,
        |   "grade_dw_id" : 1,
        |   "grade_id":"grade-id",
        |   "grade_created_time": "1970-01-15 02:40:00.0"
        |}
        |]""".stripMargin
    val redshiftGrade = spark.read.json(Seq(redshiftGradeData).toDS())
    when(service.readFromRedshift[GradeDim](RedshiftGradeSink, selectedCols = List("grade_dw_id", "grade_id"))).thenReturn(redshiftGrade)

    val redshiftSectionData =
      """
        |[{
        |   "section_status":1,
        |   "section_dw_id" : 1,
        |   "section_id":"class-id",
        |   "section_created_time": "1970-01-15 02:40:00.0"
        |}
        |]
              """.stripMargin
    val redshiftSection = spark.read.json(Seq(redshiftSectionData).toDS())
    when(service.readFromRedshift[SectionDim](RedshiftSectionSink, selectedCols = List("section_dw_id", "section_id"))).thenReturn(redshiftSection)

    val redshiftStudentData =
      """
        |[{
        |   "student_status":1,
        |   "student_id":"student-id1",
        |   "student_created_time": "1970-01-15 02:40:00.0",
        |   "student_username":"student-username",
        |   "student_school_dw_id" : "1",
        |   "student_grade_dw_id":"1",
        |   "student_section_dw_id":"1",
        |   "student_tags":"ELITE",
        |   "student_special_needs":"n/a",
        |   "student_active_until": null
        |}
        |]""".stripMargin
    val redshiftStudent = spark.read.json(Seq(redshiftStudentData).toDS())
    when(service.readFromRedshift[StudentDim](RedshiftDimStudentSink)).thenReturn(redshiftStudent)

    val transformer = new StudentTransform(sprk, service)
    val result = transformer.transform().get.output

    assert(result.count == 2)

    val df1 = result.filter(col("student_uuid") === "student-id")
    assert(df1.columns.toSet === studentDimExpectedColumns)
    assert[String](df1, "student_uuid", "student-id")
    assert[String](df1, "grade_uuid", "new-grade-id")
    assert[String](df1, "section_uuid", "new-class-id")
    assert[String](df1, "student_username", "student-username")
    assert[String](df1, "student_tags", "ELITE")
    assert[Int](df1, "student_status", ActiveEnabled)

    val df2 = result.filter(col("student_uuid") === "student-id1")
    assert(df2.columns.toSet === studentDimExpectedColumns)
    assert[String](df2, "student_uuid", "student-id1")
    assert[String](df2, "grade_uuid", "new-grade-id")
    assert[String](df2, "section_uuid", "new-class-id")
    assert[String](df2, "student_username", "student-username1")
    assert[String](df2, "student_tags", "")
    assert[Int](df2, "student_status", ActiveEnabled)
  }

  test("grade movement and deleted event") {
    val sprk = spark
    import sprk.implicits._

    when(service.readOptional(ParquetStudentSource, sprk)).thenReturn(None)
    when(service.readOptional(ParquetStudentTagUpdatedSource, sprk)).thenReturn(None)

    val studentSchoolOrGradeMoveEvent =
      """
        |[{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentMovedBetweenGrades",
        |   "occurredOn":"2019-03-10 02:40:00.0",
        |   "studentId" : "student-id",
        |   "loadtime":"wMC3yz1AAAAggyUA",
        |   "oldSchoolId":"old-school-id",
        |   "oldClassId":"old-class-id",
        |   "oldSectionId":"old-class-id",
        |   "oldGradeId": "old-grade-id",
        |   "oldK12Grade":6,
        |   "targetSchoolId":"new-school-id",
        |   "targetClassId":"new-class-id",
        |   "targetSectionId":"new-class-id",
        |   "targetGradeId":"new-grade-id",
        |   "targetK12Grade": 7
        |}]
              """.stripMargin
    val studentSchoolGrade = spark.read.json(Seq(studentSchoolOrGradeMoveEvent).toDS())
    when(service.readOptional(ParquetStudentSchoolOrGradeMoveSource, sprk)).thenReturn(Some(studentSchoolGrade))
    when(service.readOptional(ParquetStudentPromotedSource, sprk)).thenReturn(None)
    when(service.readOptional(ParquetStudentSectionUpdatedSource, sprk)).thenReturn(None)
    when(service.readOptional(ParquetStudentEnableDisableSource, sprk)).thenReturn(None)

    val studentDeletedEvent =
      """
        |[{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentsDeletedEvent",
        |   "createdOn":12345,
        |   "schoolId" : "school-id",
        |   "events" : [{"studentId": "student-id", "classId": "new-class-id","sectionId": "new-class-id", "gradeId" : "new-grade-id", "userName" : "student-username"}],
        |   "loadtime":"wMC3yz1AAAAggyUA",
        |   "eventDateDw":"20181008",
        |   "occurredOn": "2019-03-10 02:40:01.0"
        |}]
              """.stripMargin
    val studentDeleted = spark.read.json(Seq(studentDeletedEvent).toDS())
    when(service.readOptional(ParquetStudentsDeletedSource, sprk)).thenReturn(Some(studentDeleted))

    val studentRelData =
      """
        |[{
        |   "student_status":1,
        |   "student_uuid":"student-id",
        |   "student_created_time": "2019-03-09 02:30:00.0",
        |   "student_username":"student-username",
        |   "school_uuid" : "old-school-id",
        |   "grade_uuid":"old-grade-id",
        |   "section_uuid":"old-class-id",
        |   "student_tags":"ELITE",
        |   "student_special_needs":"n/a",
        |   "student_active_until": null
        |},
        |{
        |   "student_status":1,
        |   "student_uuid":"student-id1",
        |   "student_created_time": "2019-03-09 01:40:00.0",
        |   "student_username":"student-username1",
        |   "school_uuid" : "old-school-id",
        |   "grade_uuid":"old-grade-id",
        |   "section_uuid":"old-class-id",
        |   "student_tags":"",
        |   "student_special_needs":"n/a",
        |   "student_active_until": null
        |}
        |]
              """.stripMargin
    val studentRel = spark.read.json(Seq(studentRelData).toDS())
    when(service.readFromRedshift[StudentRel](RedshiftStudentSink)).thenReturn(studentRel)
    val redshiftSchoolData =
      """
        |[{
        |   "school_status":1,
        |   "school_dw_id" : 1,
        |   "school_id":"school-id",
        |   "school_created_time": "1970-01-15 02:40:00.0"
        |}
        |]""".stripMargin
    val redshiftSchool = spark.read.json(Seq(redshiftSchoolData).toDS())
    when(service.readFromRedshift[SchoolDim](RedshiftSchoolSink, selectedCols = List("school_dw_id", "school_id"))).thenReturn(redshiftSchool)

    val redshiftGradeData =
      """
        |[{
        |   "grade_status":1,
        |   "grade_dw_id" : 1,
        |   "grade_id":"grade-id",
        |   "grade_created_time": "1970-01-15 02:40:00.0"
        |}
        |]""".stripMargin
    val redshiftGrade = spark.read.json(Seq(redshiftGradeData).toDS())
    when(service.readFromRedshift[GradeDim](RedshiftGradeSink, selectedCols = List("grade_dw_id", "grade_id"))).thenReturn(redshiftGrade)

    val redshiftSectionData =
      """
        |[{
        |   "section_status":1,
        |   "section_dw_id" : 1,
        |   "section_id":"class-id",
        |   "section_created_time": "1970-01-15 02:40:00.0"
        |}
        |]
              """.stripMargin
    val redshiftSection = spark.read.json(Seq(redshiftSectionData).toDS())
    when(service.readFromRedshift[SectionDim](RedshiftSectionSink, selectedCols = List("section_dw_id", "section_id"))).thenReturn(redshiftSection)

    val redshiftStudentData =
      """
        |[{
        |   "student_status":1,
        |   "student_id":"student-id1",
        |   "student_created_time": "1970-01-15 02:40:00.0",
        |   "student_username":"student-username",
        |   "student_school_dw_id" : "1",
        |   "student_grade_dw_id":"1",
        |   "student_section_dw_id":"1",
        |   "student_tags":"ELITE",
        |   "student_special_needs":"n/a",
        |   "student_active_until": null
        |}
        |]""".stripMargin
    val redshiftStudent = spark.read.json(Seq(redshiftStudentData).toDS())
    when(service.readFromRedshift[StudentDim](RedshiftDimStudentSink)).thenReturn(redshiftStudent)

    val transformer = new StudentTransform(sprk, service)
    val result = transformer.transform().get.output
    assert(result.count == 1)

    assert(result.columns.toSet === studentDimExpectedColumns)
    assert[String](result, "student_uuid", "student-id")
    assert[String](result, "grade_uuid", "new-grade-id")
    assert[String](result, "section_uuid", "new-class-id")
    assert[String](result, "student_username", "student-username")
    assert[String](result, "student_tags", "ELITE")
    assert[Int](result, "student_status", Deleted)
  }

  test("grade movement , section movement and school movement event in disabled student with username changed") {
    val sprk = spark
    import sprk.implicits._

    val studentEvent =
      """
        |[{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentUpdatedEvent",
        |   "createdOn":12345,
        |   "uuid":"student-id",
        |   "username":"good-student",
        |   "grade": "6",
        |   "gradeId" : "grade-id",
        |   "classId": "class-id",
        |   "sectionId": "class-id",
        |   "schoolId" : "school-id",
        |   "tags":["Test"],
        |   "specialNeeds":[],
        |   "loadtime":"wMC3yz1AAAAggyUA",
        |   "eventDateDw":"20181008",
        |   "occurredOn": "2019-03-10 02:30:00.0"
        |}]
              """.stripMargin
    val student = spark.read.json(Seq(studentEvent).toDS())
    when(service.readOptional(ParquetStudentSource, sprk)).thenReturn(Some(student))

    val tagUpdatedEvent =
      """
        |[{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentTagUpdatedEvent",
        |   "occurredOn":"2019-03-10 02:30:00.0",
        |   "studentId" : "student-id",
        |   "tags":["Elite"]
        |}
        |]
              """.stripMargin
    val tagUpdated = spark.read.json(Seq(tagUpdatedEvent).toDS())
    when(service.readOptional(ParquetStudentTagUpdatedSource, sprk)).thenReturn(Some(tagUpdated))

    val studentSchoolOrGradeMoveEvent =
      """
        |[{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentMovedBetweenGrades",
        |   "occurredOn":"2019-02-10 02:40:00.0",
        |   "studentId" : "student-id",
        |   "loadtime":"wMC3yz1AAAAggyUA",
        |   "oldSchoolId":"old-school-id",
        |   "oldClassId":"old-class-id",
        |   "oldSectionId":"old-class-id",
        |   "oldGradeId": "old-grade-id",
        |   "oldK12Grade":6,
        |   "targetSchoolId":"new-school-id",
        |   "targetClassId":"new-class-id",
        |   "targetSectionId":"new-class-id",
        |   "targetGradeId":"new-grade-id",
        |   "targetK12Grade": 7
        |},
        |{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentMovedBetweenSchools",
        |   "occurredOn":"2019-02-11 02:41:00.0",
        |   "studentId" : "student-id",
        |   "loadtime":"wMC3yz1AAAAggyUA",
        |   "oldSchoolId":"old-school-id",
        |   "oldClassId":"old-class-id",
        |   "oldSectionId":"old-class-id",
        |   "oldGradeId": "old-grade-id",
        |   "oldK12Grade":6,
        |   "targetSchoolId":"new-school-id1",
        |   "targetClassId":"new-class-id1",
        |   "targetSectionId":"new-class-id1",
        |   "targetGradeId":"new-grade-id1",
        |   "targetK12Grade": 7
        |}
        |]
              """.stripMargin
    val studentSchoolOrGrade = spark.read.json(Seq(studentSchoolOrGradeMoveEvent).toDS())
    when(service.readOptional(ParquetStudentSchoolOrGradeMoveSource, sprk)).thenReturn(Some(studentSchoolOrGrade))

    val studentPromotedEvent =
      """
        |[{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentPromotedEvent",
        |   "occurredOn":"2019-02-10 02:40:00.0",
        |   "studentId" : "student-id",
        |   "loadtime":"wMC3yz1AAAAggyUA",
        |   "rolledOverClassId":"new-class-id",
        |   "rolledOverSectionId":"new-class-id",
        |   "rolledOverGradeId": "new-grade-id"
        |}]
              """.stripMargin
    val studentPromoted = spark.read.json(Seq(studentPromotedEvent).toDS())
    when(service.readOptional(ParquetStudentPromotedSource, sprk)).thenReturn(Some(studentPromoted))

    val studentSectionUpdatedEvent =
      """
        |[{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentSectionUpdatedEvent",
        |   "occurredOn":"2019-02-12 02:40:00.0",
        |   "id" : "student-id",
        |   "grade":"6",
        |   "gradeId": "new-grade-id1",
        |   "schoolId":"new-school-id1",
        |   "oldSectionId":"new-class-id1",
        |   "newSectionId":"new-class-id2"
        |}
        |]
              """.stripMargin
    val studentSectionUpdated = spark.read.json(Seq(studentSectionUpdatedEvent).toDS())
    when(service.readOptional(ParquetStudentSectionUpdatedSource, sprk)).thenReturn(Some(studentSectionUpdated))

    val enabledDisabledEvent =
      """
        |[{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentEnabledEvent",
        |   "createdOn":12345,
        |   "schoolId" : "school-id",
        |   "loadtime":"wMC3yz1AAAAggyUA",
        |   "eventDateDw":"20181008",
        |   "uuid": "student-id",
        |   "k12Grade":6,
        |   "classId":"class-id",
        |   "sectionId":"class-id",
        |   "gradeId":"grade-id",
        |   "username":"student-u1",
        |   "occurredOn": "1970-08-20 03:40:00.0"
        |},
        |{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentDisabledEvent",
        |   "createdOn":12346,
        |   "schoolId" : "school-id",
        |   "loadtime":"wMC3yz1AAAAggyUA",
        |   "eventDateDw":"20181008",
        |   "uuid": "student-id",
        |   "k12Grade":6,
        |   "classId":"class-id",
        |   "sectionId":"class-id",
        |   "gradeId":"grade-id",
        |   "username":"student-u1",
        |   "occurredOn": "1970-08-21 02:40:00.0"
        |}]
              """.stripMargin
    val enabledDisabled = spark.read.json(Seq(enabledDisabledEvent).toDS())
    when(service.readOptional(ParquetStudentEnableDisableSource, sprk)).thenReturn(Some(enabledDisabled))

    when(service.readOptional(ParquetStudentsDeletedSource, sprk)).thenReturn(None)
    when(service.readFromRedshift[StudentRel](RedshiftStudentSink)).thenReturn(Seq.empty[StudentRel].toDF)

    val redshiftSchoolData =
      """
        |[{
        |   "school_status":1,
        |   "school_dw_id" : 1,
        |   "school_id":"school-id",
        |   "school_created_time": "1970-01-15 02:40:00.0"
        |}
        |]""".stripMargin
    val redshiftSchool = spark.read.json(Seq(redshiftSchoolData).toDS())
    when(service.readFromRedshift[SchoolDim](RedshiftSchoolSink, selectedCols = List("school_dw_id", "school_id"))).thenReturn(redshiftSchool)

    val redshiftGradeData =
      """
        |[{
        |   "grade_status":1,
        |   "grade_dw_id" : 1,
        |   "grade_id":"grade-id",
        |   "grade_created_time": "1970-01-15 02:40:00.0"
        |}
        |]""".stripMargin
    val redshiftGrade = spark.read.json(Seq(redshiftGradeData).toDS())
    when(service.readFromRedshift[GradeDim](RedshiftGradeSink, selectedCols = List("grade_dw_id", "grade_id"))).thenReturn(redshiftGrade)

    val redshiftSectionData =
      """
        |[{
        |   "section_status":1,
        |   "section_dw_id" : 1,
        |   "section_id":"class-id",
        |   "section_created_time": "1970-01-15 02:40:00.0"
        |}
        |]
              """.stripMargin
    val redshiftSection = spark.read.json(Seq(redshiftSectionData).toDS())
    when(service.readFromRedshift[SectionDim](RedshiftSectionSink, selectedCols = List("section_dw_id", "section_id"))).thenReturn(redshiftSection)

    val redshiftStudentData =
      """
        |[{
        |   "student_status":1,
        |   "student_id":"student-id1",
        |   "student_created_time": "1970-01-15 02:40:00.0",
        |   "student_username":"student-username",
        |   "student_school_dw_id" : "1",
        |   "student_grade_dw_id":"1",
        |   "student_section_dw_id":"1",
        |   "student_tags":"ELITE",
        |   "student_special_needs":"n/a",
        |   "student_active_until": null
        |}
        |]""".stripMargin
    val redshiftStudent = spark.read.json(Seq(redshiftStudentData).toDS())
    when(service.readFromRedshift[StudentDim](RedshiftDimStudentSink)).thenReturn(redshiftStudent)

    val transformer = new StudentTransform(sprk, service)
    val result = transformer.transform().get.output

    assert(result.count == 1)
    assert(result.columns.toSet === studentDimExpectedColumns)
    assert[String](result, "student_uuid", "student-id")
    assert[String](result, "grade_uuid", "new-grade-id1")
    assert[String](result, "school_uuid", "new-school-id1")
    assert[String](result, "section_uuid", "new-class-id2")
    assert[String](result, "student_username", "good-student")
    assert[String](result, "student_tags", "Elite")
    assert[Int](result, "student_status", Disabled)
  }

  test("class updated and student updated events at same time") {
    val sprk = spark
    import sprk.implicits._

    val studentEvent =
      """
        |[{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentUpdatedEvent",
        |   "createdOn":12345,
        |   "uuid":"student-id",
        |   "username":"uname-updated",
        |   "grade": "6",
        |   "gradeId" : "grade-id",
        |   "classId": "class-id",
        |   "sectionId": "class-id",
        |   "schoolId" : "school-id",
        |   "tags":["Test"],
        |   "specialNeeds":[],
        |   "loadtime":"wMC3yz1AAAAggyUA",
        |   "eventDateDw":"20181008",
        |   "occurredOn": "2019-02-12 02:40:00.0"
        |}]
              """.stripMargin
    val student = spark.read.json(Seq(studentEvent).toDS())
    when(service.readOptional(ParquetStudentSource, sprk)).thenReturn(Some(student))

    val tagUpdatedEvent =
      """
        |[{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentTagUpdatedEvent",
        |   "occurredOn":"2019-02-12 02:40:00.0",
        |   "studentId" : "student-id",
        |   "tags":["Elite", "Bad"]
        |}
        |]
              """.stripMargin
    val tagUpdated = spark.read.json(Seq(tagUpdatedEvent).toDS())
    when(service.readOptional(ParquetStudentTagUpdatedSource, sprk)).thenReturn(Some(tagUpdated))

    when(service.readOptional(ParquetStudentSchoolOrGradeMoveSource, sprk)).thenReturn(None)

    val studentPromotedEvent =
      """
        |[{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentPromotedEvent",
        |   "occurredOn":"2019-02-11 02:40:00.0",
        |   "studentId" : "student-id",
        |   "loadtime":"wMC3yz1AAAAggyUA",
        |   "rolledOverClassId":"new-class-id",
        |   "rolledOverSectionId":"new-class-id",
        |   "rolledOverGradeId": "new-grade-id"
        |}]
              """.stripMargin
    val studentPromoted = spark.read.json(Seq(studentPromotedEvent).toDS())
    when(service.readOptional(ParquetStudentPromotedSource, sprk)).thenReturn(Some(studentPromoted))

    val studentSectionUpdatedEvent =
      """
        |[{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentSectionUpdatedEvent",
        |   "occurredOn":"2019-02-12 02:40:00.0",
        |   "id" : "student-id",
        |   "grade":"6",
        |   "gradeId": "new-grade-id1",
        |   "schoolId":"school-id",
        |   "oldSectionId":"new-class-id1",
        |   "newSectionId":"new-class-id3"
        |}
        |]
              """.stripMargin
    val studentSectionUpdated = spark.read.json(Seq(studentSectionUpdatedEvent).toDS())
    when(service.readOptional(ParquetStudentSectionUpdatedSource, sprk)).thenReturn(Some(studentSectionUpdated))

    when(service.readOptional(ParquetStudentEnableDisableSource, sprk)).thenReturn(None)
    when(service.readOptional(ParquetStudentsDeletedSource, sprk)).thenReturn(None)
    when(service.readFromRedshift[StudentRel](RedshiftStudentSink)).thenReturn(Seq.empty[StudentRel].toDF)

    val redshiftSchoolData =
      """
        |[{
        |   "school_status":1,
        |   "school_dw_id" : 1,
        |   "school_id":"school-id",
        |   "school_created_time": "1970-01-15 02:40:00.0"
        |}
        |]""".stripMargin
    val redshiftSchool = spark.read.json(Seq(redshiftSchoolData).toDS())
    when(service.readFromRedshift[SchoolDim](RedshiftSchoolSink, selectedCols = List("school_dw_id", "school_id"))).thenReturn(redshiftSchool)

    val redshiftGradeData =
      """
        |[{
        |   "grade_status":1,
        |   "grade_dw_id" : 1,
        |   "grade_id":"grade-id",
        |   "grade_created_time": "1970-01-15 02:40:00.0"
        |}
        |]""".stripMargin
    val redshiftGrade = spark.read.json(Seq(redshiftGradeData).toDS())
    when(service.readFromRedshift[GradeDim](RedshiftGradeSink, selectedCols = List("grade_dw_id", "grade_id"))).thenReturn(redshiftGrade)

    val redshiftSectionData =
      """
        |[{
        |   "section_status":1,
        |   "section_dw_id" : 1,
        |   "section_id":"class-id",
        |   "section_created_time": "1970-01-15 02:40:00.0"
        |}
        |]
              """.stripMargin
    val redshiftSection = spark.read.json(Seq(redshiftSectionData).toDS())
    when(service.readFromRedshift[SectionDim](RedshiftSectionSink, selectedCols = List("section_dw_id", "section_id"))).thenReturn(redshiftSection)

    val redshiftStudentData =
      """
        |[{
        |   "student_status":1,
        |   "student_id":"student-id",
        |   "student_created_time": "1970-01-15 02:40:00.0",
        |   "student_username":"student-username",
        |   "student_school_dw_id" : "1",
        |   "student_grade_dw_id":"1",
        |   "student_section_dw_id":"1",
        |   "student_tags":"ELITE",
        |   "student_special_needs":"n/a",
        |   "student_active_until": null
        |}
        |]""".stripMargin
    val redshiftStudent = spark.read.json(Seq(redshiftStudentData).toDS())
    when(service.readFromRedshift[StudentDim](RedshiftDimStudentSink)).thenReturn(redshiftStudent)

    val transformer = new StudentTransform(sprk, service)
    val result = transformer.transform().get.output
    assert(result.count == 1)

    assert(result.columns.toSet === studentDimExpectedColumns)
    assert[String](result, "student_uuid", "student-id")
    assert[String](result, "grade_uuid", "new-grade-id1")
    assert[String](result, "school_uuid", "school-id")
    assert[String](result, "section_uuid", "new-class-id3")
    assert[String](result, "student_username", "uname-updated")
    assert[String](result, "student_tags", "Elite, Bad")
    assert[String](result, "student_created_time", "2019-02-12 02:40:00.0")
    assert[Int](result, "student_status", ActiveEnabled)
  }

  test("section updated events") {
    val sprk = spark
    import sprk.implicits._

    when(service.readOptional(ParquetStudentSource, sprk)).thenReturn(None)
    when(service.readOptional(ParquetStudentTagUpdatedSource, sprk)).thenReturn(None)
    when(service.readOptional(ParquetStudentSchoolOrGradeMoveSource, sprk)).thenReturn(None)

    val studentPromotedEvent =
      """
        |[{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentPromotedEvent",
        |   "occurredOn":"2019-02-11 02:40:00.0",
        |   "studentId" : "student-id",
        |   "loadtime":"wMC3yz1AAAAggyUA",
        |   "rolledOverClassId":"new-class-id",
        |   "rolledOverSectionId":"new-class-id",
        |   "rolledOverGradeId": "new-grade-id"
        |}]
              """.stripMargin
    val studentPromoted = spark.read.json(Seq(studentPromotedEvent).toDS())
    when(service.readOptional(ParquetStudentPromotedSource, sprk)).thenReturn(Some(studentPromoted))

    val studentSectionUpdatedEvent =
      """
        |[{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentSectionUpdatedEvent",
        |   "occurredOn":"2019-02-12 02:40:00.0",
        |   "id" : "student-id",
        |   "grade":"6",
        |   "gradeId": "new-grade-id1",
        |   "schoolId":"school-id",
        |   "oldSectionId":"new-class-id1",
        |   "newSectionId":"new-class-id3"
        |}
        |]
              """.stripMargin
    val studentSectionUpdated = spark.read.json(Seq(studentSectionUpdatedEvent).toDS())
    when(service.readOptional(ParquetStudentSectionUpdatedSource, sprk)).thenReturn(Some(studentSectionUpdated))

    when(service.readOptional(ParquetStudentEnableDisableSource, sprk)).thenReturn(None)
    when(service.readOptional(ParquetStudentsDeletedSource, sprk)).thenReturn(None)
    when(service.readFromRedshift[StudentRel](RedshiftStudentSink)).thenReturn(Seq.empty[StudentRel].toDF)

    val redshiftSchoolData =
      """
        |[{
        |   "school_status":1,
        |   "school_dw_id" : 1,
        |   "school_id":"school-id",
        |   "school_created_time": "1970-01-15 02:40:00.0"
        |}
        |]""".stripMargin
    val redshiftSchool = spark.read.json(Seq(redshiftSchoolData).toDS())
    when(service.readFromRedshift[SchoolDim](RedshiftSchoolSink, selectedCols = List("school_dw_id", "school_id"))).thenReturn(redshiftSchool)

    val redshiftGradeData =
      """
        |[{
        |   "grade_status":1,
        |   "grade_dw_id" : 1,
        |   "grade_id":"grade-id",
        |   "grade_created_time": "1970-01-15 02:40:00.0"
        |}
        |]""".stripMargin
    val redshiftGrade = spark.read.json(Seq(redshiftGradeData).toDS())
    when(service.readFromRedshift[GradeDim](RedshiftGradeSink, selectedCols = List("grade_dw_id", "grade_id"))).thenReturn(redshiftGrade)

    val redshiftSectionData =
      """
        |[{
        |   "section_status":1,
        |   "section_dw_id" : 1,
        |   "section_id":"class-id",
        |   "section_created_time": "1970-01-15 02:40:00.0"
        |}
        |]
              """.stripMargin
    val redshiftSection = spark.read.json(Seq(redshiftSectionData).toDS())
    when(service.readFromRedshift[SectionDim](RedshiftSectionSink, selectedCols = List("section_dw_id", "section_id"))).thenReturn(redshiftSection)

    val redshiftStudentData =
      """
        |[{
        |   "student_status":1,
        |   "student_id":"student-id",
        |   "student_created_time": "1970-01-15 02:40:00.0",
        |   "student_username":"student-username",
        |   "student_school_dw_id" : "1",
        |   "student_grade_dw_id":"1",
        |   "student_section_dw_id":"1",
        |   "student_tags":"ELITE",
        |   "student_special_needs":"n/a",
        |   "student_active_until": null
        |}
        |]""".stripMargin
    val redshiftStudent = spark.read.json(Seq(redshiftStudentData).toDS())
    when(service.readFromRedshift[StudentDim](RedshiftDimStudentSink)).thenReturn(redshiftStudent)

    val transformer = new StudentTransform(sprk, service)
    val result = transformer.transform().get.output
    assert(result.count == 1)

    assert(result.columns.toSet === studentDimExpectedColumns)
    assert[String](result, "student_uuid", "student-id")
    assert[String](result, "grade_uuid", "new-grade-id1")
    assert[String](result, "school_uuid", "school-id")
    assert[String](result, "section_uuid", "new-class-id3")
    assert[String](result, "student_username", "student-username")
    assert[String](result, "student_tags", "ELITE")
    assert[String](result, "student_created_time", "2019-02-12 02:40:00.0")
    assert[Int](result, "student_status", ActiveEnabled)
  }

  test("empty tags to n/a") {
    val sprk = spark
    import sprk.implicits._

    val studentEvent =
      """
        |[{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentCreatedEvent",
        |   "createdOn":12345,
        |   "uuid":"student-id",
        |   "username":"student-username",
        |   "grade": "6",
        |   "gradeId" : "grade-id",
        |   "classId": "class-id",
        |   "sectionId": "class-id",
        |   "schoolId" : "school-id",
        |   "tags":["Test"],
        |   "specialNeeds":[],
        |   "loadtime":"wMC3yz1AAAAggyUA",
        |   "eventDateDw":"20181008",
        |   "occurredOn": "2019-01-14 02:40:00.0"
        |}]
              """.stripMargin
    val student = spark.read.json(Seq(studentEvent).toDS())
    when(service.readOptional(ParquetStudentSource, sprk)).thenReturn(Some(student))

    val tagUpdatedEvent =
      """
        |[{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentTagUpdatedEvent",
        |   "occurredOn":"2019-01-14 02:40:00.0",
        |   "studentId" : "student-id",
        |   "tags":[]
        |}
        |]
              """.stripMargin
    val tagUpdated = spark.read.json(Seq(tagUpdatedEvent).toDS())
    when(service.readOptional(ParquetStudentTagUpdatedSource, sprk)).thenReturn(Some(tagUpdated))

    when(service.readOptional(ParquetStudentSchoolOrGradeMoveSource, sprk)).thenReturn(None)

    val studentPromotedEvent =
      """
        |[{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentPromotedEvent",
        |   "occurredOn":"2019-01-13 02:40:00.0",
        |   "studentId" : "student-id",
        |   "loadtime":"wMC3yz1AAAAggyUA",
        |   "rolledOverClassId":"new-class-id",
        |   "rolledOverSectionId":"new-class-id",
        |   "rolledOverGradeId": "new-grade-id"
        |}]
              """.stripMargin
    val studentPromoted = spark.read.json(Seq(studentPromotedEvent).toDS())
    when(service.readOptional(ParquetStudentPromotedSource, sprk)).thenReturn(Some(studentPromoted))
    when(service.readOptional(ParquetStudentSectionUpdatedSource, sprk)).thenReturn(None)
    when(service.readOptional(ParquetStudentEnableDisableSource, sprk)).thenReturn(None)
    when(service.readOptional(ParquetStudentsDeletedSource, sprk)).thenReturn(None)
    when(service.readFromRedshift[StudentRel](RedshiftStudentSink)).thenReturn(Seq.empty[StudentRel].toDF)

    val redshiftSchoolData =
      """
        |[{
        |   "school_status":1,
        |   "school_dw_id" : 1,
        |   "school_id":"school-id",
        |   "school_created_time": "1970-01-15 02:40:00.0"
        |}
        |]""".stripMargin
    val redshiftSchool = spark.read.json(Seq(redshiftSchoolData).toDS())
    when(service.readFromRedshift[SchoolDim](RedshiftSchoolSink, selectedCols = List("school_dw_id", "school_id"))).thenReturn(redshiftSchool)

    val redshiftGradeData =
      """
        |[{
        |   "grade_status":1,
        |   "grade_dw_id" : 1,
        |   "grade_id":"grade-id",
        |   "grade_created_time": "1970-01-15 02:40:00.0"
        |}
        |]""".stripMargin
    val redshiftGrade = spark.read.json(Seq(redshiftGradeData).toDS())
    when(service.readFromRedshift[GradeDim](RedshiftGradeSink, selectedCols = List("grade_dw_id", "grade_id"))).thenReturn(redshiftGrade)

    val redshiftSectionData =
      """
        |[{
        |   "section_status":1,
        |   "section_dw_id" : 1,
        |   "section_id":"class-id",
        |   "section_created_time": "1970-01-15 02:40:00.0"
        |}
        |]
              """.stripMargin
    val redshiftSection = spark.read.json(Seq(redshiftSectionData).toDS())
    when(service.readFromRedshift[SectionDim](RedshiftSectionSink, selectedCols = List("section_dw_id", "section_id"))).thenReturn(redshiftSection)

    val redshiftStudentData =
      """
        |[{
        |   "student_status":1,
        |   "student_id":"student-id",
        |   "student_created_time": "1970-01-15 02:40:00.0",
        |   "student_username":"student-username",
        |   "student_school_dw_id" : "1",
        |   "student_grade_dw_id":"1",
        |   "student_section_dw_id":"1",
        |   "student_tags":"ELITE",
        |   "student_special_needs":"n/a",
        |   "student_active_until": null
        |}
        |]""".stripMargin
    val redshiftStudent = spark.read.json(Seq(redshiftStudentData).toDS())
    when(service.readFromRedshift[StudentDim](RedshiftDimStudentSink)).thenReturn(redshiftStudent)

    val transformer = new StudentTransform(sprk, service)
    val result = transformer.transform().get.output
    assert(result.count == 1)

    assert(result.columns.toSet === studentDimExpectedColumns)
    assert[String](result, "student_tags", "n/a")
    assert[String](result, "student_created_time", "2019-01-14 02:40:00.0")
    assert[Int](result, "student_status", ActiveEnabled)
  }

  test("student promoted event and student has latest associated school") {
    val sprk = spark
    import sprk.implicits._

    val studentEvent =
      """
        |[{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentUpdatedEvent",
        |   "createdOn":12345,
        |   "uuid":"student-id",
        |   "username":"good-student",
        |   "grade": "6",
        |   "gradeId" : "grade-id",
        |   "classId": "class-id",
        |   "sectionId": "class-id",
        |   "schoolId" : "old-school-id",
        |   "tags":["Test"],
        |   "specialNeeds":[],
        |   "loadtime":"wMC3yz1AAAAggyUA",
        |   "eventDateDw":"20181008",
        |   "occurredOn": "2019-03-10 02:30:00.0"
        |}]
              """.stripMargin
    val student = spark.read.json(Seq(studentEvent).toDS())
    when(service.readOptional(ParquetStudentSource, sprk)).thenReturn(Some(student))

    val tagUpdatedEvent =
      """
        |[{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentTagUpdatedEvent",
        |   "occurredOn":"2019-03-10 02:30:00.0",
        |   "studentId" : "student-id",
        |   "tags":["Elite"]
        |}
        |]
              """.stripMargin
    val tagUpdated = spark.read.json(Seq(tagUpdatedEvent).toDS())
    when(service.readOptional(ParquetStudentTagUpdatedSource, sprk)).thenReturn(Some(tagUpdated))

    val schoolGradeMovedEvent =
      """
        |[{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentMovedBetweenSchools",
        |   "occurredOn":"2019-03-11 02:40:00.0",
        |   "studentId" : "student-id",
        |   "loadtime":"wMC3yz1AAAAggyUA",
        |   "oldSchoolId":"old-school-id",
        |   "oldClassId":"old-class-id",
        |   "oldSectionId":"old-class-id",
        |   "oldGradeId": "old-grade-id",
        |   "oldK12Grade":6,
        |   "targetSchoolId":"new-school-id",
        |   "targetClassId":"new-class-id",
        |   "targetSectionId":"new-class-id",
        |   "targetGradeId":"new-grade-id",
        |   "targetK12Grade": 7
        |}]
              """.stripMargin
    val schoolGradeMoved = spark.read.json(Seq(schoolGradeMovedEvent).toDS())
    when(service.readOptional(ParquetStudentSchoolOrGradeMoveSource, sprk)).thenReturn(Some(schoolGradeMoved))

    val studentPromotedEvent =
      """
        |[{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentPromotedEvent",
        |   "occurredOn":"2019-03-12 02:40:00.0",
        |   "studentId" : "student-id",
        |   "loadtime":"wMC3yz1AAAAggyUA",
        |   "rolledOverClassId":"new-class-id",
        |   "rolledOverSectionId":"new-class-id",
        |   "rolledOverGradeId": "new-grade-id"
        |}]
              """.stripMargin
    val studentPromoted = spark.read.json(Seq(studentPromotedEvent).toDS())
    when(service.readOptional(ParquetStudentPromotedSource, sprk)).thenReturn(Some(studentPromoted))

    when(service.readOptional(ParquetStudentSectionUpdatedSource, sprk)).thenReturn(None)
    when(service.readOptional(ParquetStudentEnableDisableSource, sprk)).thenReturn(None)
    when(service.readOptional(ParquetStudentsDeletedSource, sprk)).thenReturn(None)
    when(service.readFromRedshift[StudentRel](RedshiftStudentSink)).thenReturn(Seq.empty[StudentRel].toDF)

    val redshiftSchoolData =
      """
        |[{
        |   "school_status":1,
        |   "school_dw_id" : 1,
        |   "school_id":"school-id",
        |   "school_created_time": "1970-01-15 02:40:00.0"
        |}
        |]""".stripMargin
    val redshiftSchool = spark.read.json(Seq(redshiftSchoolData).toDS())
    when(service.readFromRedshift[SchoolDim](RedshiftSchoolSink, selectedCols = List("school_dw_id", "school_id"))).thenReturn(redshiftSchool)

    val redshiftGradeData =
      """
        |[{
        |   "grade_status":1,
        |   "grade_dw_id" : 1,
        |   "grade_id":"grade-id",
        |   "grade_created_time": "1970-01-15 02:40:00.0"
        |}
        |]""".stripMargin
    val redshiftGrade = spark.read.json(Seq(redshiftGradeData).toDS())
    when(service.readFromRedshift[GradeDim](RedshiftGradeSink, selectedCols = List("grade_dw_id", "grade_id"))).thenReturn(redshiftGrade)

    val redshiftSectionData =
      """
        |[{
        |   "section_status":1,
        |   "section_dw_id" : 1,
        |   "section_id":"class-id",
        |   "section_created_time": "1970-01-15 02:40:00.0"
        |}
        |]
              """.stripMargin
    val redshiftSection = spark.read.json(Seq(redshiftSectionData).toDS())
    when(service.readFromRedshift[SectionDim](RedshiftSectionSink, selectedCols = List("section_dw_id", "section_id"))).thenReturn(redshiftSection)

    val redshiftStudentData =
      """
        |[{
        |   "student_status":1,
        |   "student_id":"student-id",
        |   "student_created_time": "1970-01-15 02:40:00.0",
        |   "student_username":"student-username",
        |   "student_school_dw_id" : "1",
        |   "student_grade_dw_id":"1",
        |   "student_section_dw_id":"1",
        |   "student_tags":"ELITE",
        |   "student_special_needs":"n/a",
        |   "student_active_until": null
        |}
        |]""".stripMargin
    val redshiftStudent = spark.read.json(Seq(redshiftStudentData).toDS())
    when(service.readFromRedshift[StudentDim](RedshiftDimStudentSink)).thenReturn(redshiftStudent)

    val transformer = new StudentTransform(sprk, service)
    val result = transformer.transform().get.output
    assert(result.count == 1)

    assert(result.columns.toSet === studentDimExpectedColumns)
    assert[String](result, "student_tags", "Elite")
    assert[String](result, "student_created_time", "2019-03-12 02:40:00.0")
    assert[Int](result, "student_status", ActiveEnabled)
    assert[String](result, "section_uuid", "new-class-id")
    assert[String](result, "grade_uuid", "new-grade-id")
    assert[String](result, "school_uuid", "new-school-id")
  }

  test("student created event with tag event coming earlier than created event") {
    val sprk = spark
    import sprk.implicits._

    val studentEvent =
      """
        |[{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentCreatedEvent",
        |   "createdOn":12345,
        |   "uuid":"student-id",
        |   "username":"student-username",
        |   "grade": "6",
        |   "gradeId" : "grade-id",
        |   "classId": "class-id",
        |   "sectionId": "class-id",
        |   "schoolId" : "school-id",
        |   "tags":["Test"],
        |   "specialNeeds":[],
        |   "loadtime":"wMC3yz1AAAAggyUA",
        |   "eventDateDw":"20181008",
        |   "occurredOn": "2019-07-14 02:40:00.0"
        |}]
              """.stripMargin
    val student = spark.read.json(Seq(studentEvent).toDS())
    when(service.readOptional(ParquetStudentSource, sprk)).thenReturn(Some(student))

    val tagUpdatedEvent =
      """
        |[{
        |   "tenantId":"tenant-id",
        |   "eventType":"StudentTagUpdatedEvent",
        |   "occurredOn":"2019-05-14 02:40:00.0",
        |   "studentId" : "student-id",
        |   "tags":["Elite"]
        |}
        |]
              """.stripMargin
    val tagUpdated = spark.read.json(Seq(tagUpdatedEvent).toDS())
    when(service.readOptional(ParquetStudentTagUpdatedSource, sprk)).thenReturn(Some(tagUpdated))

    when(service.readOptional(ParquetStudentSchoolOrGradeMoveSource, sprk)).thenReturn(None)
    when(service.readOptional(ParquetStudentPromotedSource, sprk)).thenReturn(None)
    when(service.readOptional(ParquetStudentSectionUpdatedSource, sprk)).thenReturn(None)
    when(service.readOptional(ParquetStudentEnableDisableSource, sprk)).thenReturn(None)
    when(service.readOptional(ParquetStudentsDeletedSource, sprk)).thenReturn(None)
    when(service.readFromRedshift[StudentRel](RedshiftStudentSink)).thenReturn(Seq.empty[StudentRel].toDF)

    val redshiftSchoolData =
      """
        |[{
        |   "school_status":1,
        |   "school_dw_id" : 1,
        |   "school_id":"school-id",
        |   "school_created_time": "1970-01-15 02:40:00.0"
        |}
        |]""".stripMargin
    val redshiftSchool = spark.read.json(Seq(redshiftSchoolData).toDS())
    when(service.readFromRedshift[SchoolDim](RedshiftSchoolSink, selectedCols = List("school_dw_id", "school_id"))).thenReturn(redshiftSchool)

    val redshiftGradeData =
      """
        |[{
        |   "grade_status":1,
        |   "grade_dw_id" : 1,
        |   "grade_id":"grade-id",
        |   "grade_created_time": "1970-01-15 02:40:00.0"
        |}
        |]""".stripMargin
    val redshiftGrade = spark.read.json(Seq(redshiftGradeData).toDS())
    when(service.readFromRedshift[GradeDim](RedshiftGradeSink, selectedCols = List("grade_dw_id", "grade_id"))).thenReturn(redshiftGrade)

    val redshiftSectionData =
      """
        |[{
        |   "section_status":1,
        |   "section_dw_id" : 1,
        |   "section_id":"class-id",
        |   "section_created_time": "1970-01-15 02:40:00.0"
        |}
        |]
              """.stripMargin
    val redshiftSection = spark.read.json(Seq(redshiftSectionData).toDS())
    when(service.readFromRedshift[SectionDim](RedshiftSectionSink, selectedCols = List("section_dw_id", "section_id"))).thenReturn(redshiftSection)

    val redshiftStudentData =
      """
        |[{
        |   "student_status":1,
        |   "student_id":"student-id",
        |   "student_created_time": "1970-01-15 02:40:00.0",
        |   "student_username":"student-username",
        |   "student_school_dw_id" : "1",
        |   "student_grade_dw_id":"1",
        |   "student_section_dw_id":"1",
        |   "student_tags":"ELITE",
        |   "student_special_needs":"n/a",
        |   "student_active_until": null
        |}
        |]""".stripMargin
    val redshiftStudent = spark.read.json(Seq(redshiftStudentData).toDS())
    when(service.readFromRedshift[StudentDim](RedshiftDimStudentSink)).thenReturn(redshiftStudent)

    val transformer = new StudentTransform(sprk, service)
    val studentCreatedDf = transformer.transform().get.output
    assert(studentCreatedDf.count == 1)
    assert(studentCreatedDf.columns.toSet === studentDimExpectedColumns)
    assert[String](studentCreatedDf, "student_uuid", "student-id")
    assert[String](studentCreatedDf, "student_username", "student-username")
    assert[String](studentCreatedDf, "student_tags", "Elite")
    assert[Int](studentCreatedDf, "student_status", ActiveEnabled)
    assert[String](studentCreatedDf, "student_created_time", "2019-07-14 02:40:00.0")
  }

  test("process happy path of plain student created event for Delta") {
    val sprk = spark
    import sprk.implicits._

    val studentEvent =
      """
        |[{
        |"student_username":"student-username1",
        |"school_uuid":"school-id",
        |"student_tags":"Test",
        |"student_uuid":"student-id1",
        |"student_special_needs":"n/a",
        |"student_status":1,
        |"section_uuid":"class-id",
        |"grade_uuid":"grade-id",
        |"student_created_time":"1970-07-14 02:40:00.000",
        |"student_updated_time":null,
        |"student_dw_updated_time":null,
        |"student_deleted_time":null,
        |"student_dw_created_time":"1970-07-14 02:40:00.000",
        |"student_active_until":null,
        |"student_dw_created_time":"2022-11-15 12:13:28.815"
        |}]""".stripMargin
    val student = spark.read.json(Seq(studentEvent).toDS())
    when(service.readOptional(StudentTransformedSource, sprk)).thenReturn(Some(student))

    val delta = new StudentDelta(sprk, service)
    val studentCreatedDf = delta.transform().get.output

    assert(studentCreatedDf.count == 1)
    assert(studentCreatedDf.columns.toSet === studentDeltaDimExpectedColumns)
    assert[String](studentCreatedDf, "student_id", "student-id1")
    assert[String](studentCreatedDf, "student_username", "student-username1")
    assert[String](studentCreatedDf, "student_tags", "Test")
    assert[Int](studentCreatedDf, "student_status", ActiveEnabled)
    assert[String](studentCreatedDf, "student_created_time", "1970-07-14 02:40:00.000")
  }

  test("process happy path of plain student created event for redshift") {
    val sprk = spark
    import sprk.implicits._

    val studentEvent =
      """
        |[{
        |"student_username":"student-username1",
        |"school_uuid":"school-id",
        |"student_tags":"Test",
        |"student_uuid":"student-id1",
        |"student_special_needs":"n/a",
        |"student_status":1,
        |"section_uuid":"class-id",
        |"grade_uuid":"grade-id",
        |"student_created_time":"1970-07-14 02:40:00.000",
        |"student_updated_time":null,
        |"student_dw_updated_time":null,
        |"student_deleted_time":null,
        |"student_dw_created_time":"1970-07-14 02:40:00.000",
        |"student_active_until":null,
        |"student_dw_created_time":"2022-11-15 12:13:28.815"
        |}]""".stripMargin
    val student = spark.read.json(Seq(studentEvent).toDS())
    when(service.readOptional(StudentTransformedSource, sprk)).thenReturn(Some(student))

    val delta = new StudentRedshift(sprk, service)
    val studentCreatedDf = delta.transform().get.output

    assert(studentCreatedDf.count == 1)
    assert(studentCreatedDf.columns.toSet === studentDimExpectedColumns)
    assert[String](studentCreatedDf, "student_uuid", "student-id1")
    assert[String](studentCreatedDf, "student_username", "student-username1")
    assert[String](studentCreatedDf, "student_tags", "Test")
    assert[Int](studentCreatedDf, "student_status", ActiveEnabled)
    assert[String](studentCreatedDf, "student_created_time", "1970-07-14 02:40:00.000")
  }
}
