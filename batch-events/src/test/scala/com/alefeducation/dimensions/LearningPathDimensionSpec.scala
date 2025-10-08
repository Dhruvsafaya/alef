package com.alefeducation.dimensions

import com.alefeducation.bigdata.batch.delta.{Alias, DeltaCreateSink, DeltaDeleteSink, DeltaUpsertSink}
import com.alefeducation.bigdata.commons.testutils.{ExpectedFields, SparkSuite}
import com.alefeducation.dimensions.DeltaTests._
import com.alefeducation.service.DataSink
import com.alefeducation.util.Helpers._
import org.apache.spark.sql.types.{BooleanType, IntegerType, LongType, StringType}
import org.scalatest.matchers.must.Matchers

class LearningPathDimensionSpec extends SparkSuite with Matchers {

  import ExpectedFields._

  val Entity = "learning_path"

  private val RedshiftExpectedFields = Set(
    s"${Entity}_created_time",
    s"${Entity}_updated_time",
    s"${Entity}_deleted_time",
    s"${Entity}_dw_created_time",
    s"${Entity}_dw_updated_time",
    s"${Entity}_id",
    s"${Entity}_name",
    s"${Entity}_language_type_script",
    s"${Entity}_lp_status",
    s"${Entity}_status",
    s"${Entity}_experiential_learning",
    s"${Entity}_tutor_dhabi_enabled",
    s"${Entity}_default",
    s"${Entity}_curriculum_id",
    s"${Entity}_curriculum_grade_id",
    s"${Entity}_curriculum_subject_id",
    s"${Entity}_uuid",
    s"${Entity}_school_id",
    s"${Entity}_class_id",
    s"${Entity}_subject_id",
    s"${Entity}_academic_year_id",
    s"${Entity}_content_academic_year"
  )

  object Delta {

    val ExpectedFields = List(
      ExpectedField(name = s"${Entity}_id", dataType = StringType),
      ExpectedField(name = s"${Entity}_uuid", dataType = StringType),
      ExpectedField(name = s"${Entity}_school_id", dataType = StringType),
      ExpectedField(name = s"${Entity}_name", dataType = StringType),
      ExpectedField(name = s"${Entity}_lp_status", dataType = StringType),
      ExpectedField(name = s"${Entity}_language_type_script", dataType = StringType),
      ExpectedField(name = s"${Entity}_experiential_learning", dataType = BooleanType),
      ExpectedField(name = s"${Entity}_tutor_dhabi_enabled", dataType = StringType),
      ExpectedField(name = s"${Entity}_default", dataType = BooleanType),
      ExpectedField(name = s"${Entity}_curriculum_id", dataType = StringType),
      ExpectedField(name = s"${Entity}_curriculum_grade_id", dataType = StringType),
      ExpectedField(name = s"${Entity}_curriculum_subject_id", dataType = StringType),
      ExpectedField(name = s"${Entity}_subject_id", dataType = StringType),
      ExpectedField(name = s"${Entity}_status", dataType = IntegerType),
      ExpectedField(name = s"${Entity}_academic_year_id", dataType = StringType),
      ExpectedField(name = s"${Entity}_content_academic_year", dataType = LongType),
      ExpectedField(name = s"${Entity}_class_id", dataType = StringType)
    ) ++ getCommonExpectedTimestampFields(Entity)

    val ExpectedUpdate = Map(
      s"${Entity}_subject_id" -> s"${Entity}_subject_id",
      s"${Entity}_status" -> s"${Entity}_status",
      s"${Entity}_uuid" -> s"${Entity}_uuid",
      s"${Entity}_language_type_script" -> s"${Entity}_language_type_script",
      s"${Entity}_id" -> s"${Entity}_id",
      s"${Entity}_default" -> s"${Entity}_default",
      s"${Entity}_name" -> s"${Entity}_name",
      s"${Entity}_curriculum_grade_id" -> s"${Entity}_curriculum_grade_id",
      s"${Entity}_academic_year_id" -> s"${Entity}_academic_year_id",
      s"${Entity}_content_academic_year" -> s"${Entity}_content_academic_year",
      s"${Entity}_curriculum_id" -> s"${Entity}_curriculum_id",
      s"${Entity}_curriculum_subject_id" -> s"${Entity}_curriculum_subject_id",
      s"${Entity}_lp_status" -> s"${Entity}_lp_status",
      s"${Entity}_tutor_dhabi_enabled" -> s"${Entity}_tutor_dhabi_enabled",
      s"${Entity}_school_id" -> s"${Entity}_school_id",
      s"${Entity}_class_id" -> s"${Entity}_class_id",
      s"${Entity}_experiential_learning" -> s"${Entity}_experiential_learning"
    ) ++ getCommonExpectedUpdateTimestampFields(Entity)

  }

  trait Setup {
    implicit val transformer = LearningPathDimension(spark)
  }

  test("mutated events") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetLearningPathSource,
          value = """
              |[
              |{
              |  "eventType": "LevelCreatedEvent",
              |  "id": "learning-path-id",
              |  "schoolId": "school_id",
              |  "name": "lp-name",
              |  "status": "online",
              |  "languageTypeScript": "Arabic",
              |  "experientialLearning": true,
              |  "tutorDhabiEnabled": "ONLINE",
              |  "default": true,
              |  "curriculumId": "learning_path_curriculum_id",
              |  "curriculumGradeId": "learning_path_curriculum_grade_id",
              |  "curriculumSubjectId": "learning_path_curriculum_subject_id",
              |  "academicYearId": "academic_year_id",
              |	 "academicYear": 2019,
              |  "schoolSubjectId" : "schoolSubjectId",
              |  "classId" : null,
              |  "occurredOn": "1970-07-14 02:40:00.0",
              |  "tenantId": "tenant-id"
              |},
              |{
              |  "eventType": "LevelUpdatedEvent",
              |  "id": "learning-path-id",
              |  "schoolId": "school_id",
              |  "name": "lp-name-updated",
              |  "status": "online",
              |  "languageTypeScript": "Arabic",
              |  "experientialLearning": true,
              |  "tutorDhabiEnabled": "ONLINE",
              |  "default": true,
              |  "curriculumId": "learning_path_curriculum_id",
              |  "curriculumGradeId": "learning_path_curriculum_grade_id",
              |  "curriculumSubjectId": "learning_path_curriculum_subject_id",
              |  "academicYearId": "academic_year_id",
              |	 "academicYear": 2019,
              |  "classId" : null,
              |  "schoolSubjectId" : "schoolSubjectId",
              |  "occurredOn": "1970-07-14 02:40:00.0",
              |  "tenantId": "tenant-id"
              |}
              |]
            """.stripMargin
        )
      )

      withSparkFixtures(
        fixtures, { sinks =>
          assert(sinks.size == 5)

          val dataSinks = sinks.filter(_.isInstanceOf[DataSink])
          val s3Sink = sinks.head.asInstanceOf[DataSink]
          val redshiftSink = dataSinks.last.asInstanceOf[DataSink].dataFrame

          assert(redshiftSink.count === 1)
          assert(redshiftSink.columns.toSet === RedshiftExpectedFields)
          assert[String](redshiftSink, s"${Entity}_id", "learning-path-idschool_idschoolSubjectId")
          assert[Int](redshiftSink, s"${Entity}_status", 1)

          assert(s3Sink.options.get("partitionBy") === Some("eventdate"))
          assert[String](s3Sink.dataFrame, "eventdate", "1970-07-14")

          val deltaCreateSink = sinks.collectFirst { case s: DeltaCreateSink => s }.get
          val deltaCreatedSinkDF = deltaCreateSink.df.cache()
          Delta.ExpectedFields.map(_.name).toSet must equal(RedshiftExpectedFields)
          assertExpectedFields(deltaCreatedSinkDF.schema.fields.toList, Delta.ExpectedFields)
          assert[Int](deltaCreatedSinkDF, s"${Entity}_status", 1)
          assert[String](deltaCreatedSinkDF, s"${LearningPathEntity}_name", "lp-name")

          val deltaUpdateSink = sinks.collectFirst { case s: DeltaUpsertSink => s }.get
          val deltaUpdateSinkDF = deltaUpdateSink.df.cache()
          Delta.ExpectedFields.map(_.name).toSet must equal(RedshiftExpectedFields)
          assertExpectedFields(deltaUpdateSinkDF.schema.fields.toList, Delta.ExpectedFields)
          assert[Int](deltaUpdateSinkDF, s"${Entity}_status", 1)
          deltaUpdateSink.matchConditions must include(s"${Alias.Delta}.${Entity}_id = ${Alias.Events}.${Entity}_id")
          assertDeltaUpdateFields(deltaUpdateSink.columnsToUpdate, Delta.ExpectedUpdate)
          assert[String](deltaUpdateSinkDF, s"${Entity}_name", "lp-name-updated")
        }
      )
    }
  }

  test("latest update events") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetLearningPathSource,
          value = """
              |[
              |{
              |  "eventType": "LevelUpdatedEvent",
              |  "id": "learning-path-id",
              |  "schoolId": "school_id",
              |  "name": "lp-name-first-update",
              |  "status": "online",
              |  "languageTypeScript": "Arabic",
              |  "experientialLearning": true,
              |  "tutorDhabiEnabled": "ONLINE",
              |  "default": true,
              |  "curriculumId": "learning_path_curriculum_id",
              |  "curriculumGradeId": "learning_path_curriculum_grade_id",
              |  "curriculumSubjectId": "learning_path_curriculum_subject_id",
              |  "academicYearId": "academic_year_id",
              |	 "academicYear": 2019,
              |  "schoolSubjectId" : "schoolSubjectId",
              |  "classId" : null,
              |  "occurredOn": "1970-07-14 02:40:00.0",
              |  "tenantId": "tenant-id"
              |}
              |,
              |{
              |  "eventType": "LevelUpdatedEvent",
              |  "id": "learning-path-id1",
              |  "schoolId": "school_id1",
              |  "name": "lp-name-second-update",
              |  "status": "online",
              |  "languageTypeScript": "Arabic",
              |  "experientialLearning": true,
              |  "tutorDhabiEnabled": "ONLINE",
              |  "default": true,
              |  "curriculumId": "learning_path_curriculum_id",
              |  "curriculumGradeId": "learning_path_curriculum_grade_id",
              |  "curriculumSubjectId": "learning_path_curriculum_subject_id",
              |  "academicYearId": "academic_year_id",
              |	 "academicYear": 2019,
              |  "schoolSubjectId" : "schoolSubjectId1",
              |  "classId" : null,
              |  "occurredOn": "1970-07-14 02:41:00.0",
              |  "tenantId": "tenant-id"
              |}
              |]
            """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          //Since here we have only updated events only one event will be taken into consideration
          assert(sinks.size == 3)

          val dataSinks = sinks.filter(_.isInstanceOf[DataSink])
          val s3Sink = sinks.head.asInstanceOf[DataSink]
          val redshiftSink = dataSinks.last.asInstanceOf[DataSink].dataFrame

          assert(redshiftSink.count === 2)
          assert(redshiftSink.columns.toSet === RedshiftExpectedFields)
          assert[String](redshiftSink, s"${Entity}_id", "learning-path-id1school_id1schoolSubjectId1")
          assert[Int](redshiftSink, s"${Entity}_status", 1)

          assert(s3Sink.options.get("partitionBy") === Some("eventdate"))
          assert[String](s3Sink.dataFrame, "eventdate", "1970-07-14")

          val deltaUpdateSink = sinks.collectFirst { case s: DeltaUpsertSink => s }.get
          val deltaUpdateSinkDF = deltaUpdateSink.df.cache()
          Delta.ExpectedFields.map(_.name).toSet must equal(RedshiftExpectedFields)
          assertExpectedFields(deltaUpdateSinkDF.schema.fields.toList, Delta.ExpectedFields)
          assert[Int](deltaUpdateSinkDF, s"${Entity}_status", 1)
          deltaUpdateSink.matchConditions must include(s"${Alias.Delta}.${Entity}_id = ${Alias.Events}.${Entity}_id")
          assertDeltaUpdateFields(deltaUpdateSink.columnsToUpdate, Delta.ExpectedUpdate)
          assert[String](deltaUpdateSinkDF, s"${Entity}_name", "lp-name-second-update")
        }
      )
    }
  }

  test("deleted events") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetLearningPathSource,
          value = """
              |[
              |{
              |  "eventType": "LevelDeletedEvent",
              |  "id": "learning-path-id",
              |  "schoolId": "school_id",
              |  "name": "lp-name",
              |  "status": "online",
              |  "languageTypeScript": "Arabic",
              |  "experientialLearning": true,
              |  "tutorDhabiEnabled": "ONLINE",
              |  "default": true,
              |  "curriculumId": "learning_path_curriculum_id",
              |  "curriculumGradeId": "learning_path_curriculum_grade_id",
              |  "curriculumSubjectId": "learning_path_curriculum_subject_id",
              |  "academicYearId": "academic_year_id",
              |	 "academicYear": 2019,
              |  "schoolSubjectId" : "schoolSubjectId",
              |  "classId" : null,
              |  "occurredOn": "1970-07-14 02:40:00.0",
              |  "tenantId": "tenant-id"
              |}]
            """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          //Since here we have only updated events only one event will be taken into consideration
          assert(sinks.size == 3)

          val dataSinks = sinks.filter(_.isInstanceOf[DataSink])
          val s3Sink = sinks.head.asInstanceOf[DataSink]
          val redshiftSink = dataSinks.last.asInstanceOf[DataSink].dataFrame

          assert(redshiftSink.count === 1)
          assert(redshiftSink.columns.toSet === RedshiftExpectedFields)
          assert[String](redshiftSink, s"${Entity}_id", "learning-path-idschool_idschoolSubjectId")
          assert[Int](redshiftSink, s"${Entity}_status", Deleted)

          assert(s3Sink.options.get("partitionBy") === Some("eventdate"))
          assert[String](s3Sink.dataFrame, "eventdate", "1970-07-14")

          val deltaDeleteSink: DeltaDeleteSink = sinks.collectFirst { case s: DeltaDeleteSink => s }.get
          val deltaDeleteDF = deltaDeleteSink.df.cache()
          assert[Int](deltaDeleteDF, s"${Entity}_status", 4)
          deltaDeleteSink.matchConditions must include(s"${Alias.Delta}.${Entity}_id = ${Alias.Events}.${Entity}_id")
          assertDeltaDeleteFields(deltaDeleteSink.updateFields, Entity)
        }
      )
    }
  }

  test("created events with classId") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetLearningPathSource,
          value = """
                    |[
                    |{
                    |  "eventType": "LevelCreatedEvent",
                    |  "id": "learning-path-id",
                    |  "schoolId": "school_id",
                    |  "name": "lp-name",
                    |  "status": "online",
                    |  "languageTypeScript": "Arabic",
                    |  "experientialLearning": true,
                    |  "tutorDhabiEnabled": "ONLINE",
                    |  "default": true,
                    |  "curriculumId": "learning_path_curriculum_id",
                    |  "curriculumGradeId": "learning_path_curriculum_grade_id",
                    |  "curriculumSubjectId": "learning_path_curriculum_subject_id",
                    |  "academicYearId": "academic_year_id",
                    |	 "academicYear": 2019,
                    |  "schoolSubjectId" : null,
                    |  "classId" : "classId",
                    |  "occurredOn": "1970-07-14 02:40:00.0",
                    |  "tenantId": "tenant-id"
                    |}
                    |]
                  """.stripMargin
        )
      )

      withSparkFixtures(
        fixtures, { sinks =>
          assert(sinks.size == 3)

          val dataSinks = sinks.filter(_.isInstanceOf[DataSink])
          val s3Sink = sinks.head.asInstanceOf[DataSink]
          val redshiftSink = dataSinks.last.asInstanceOf[DataSink].dataFrame

          assert(redshiftSink.count === 1)
          assert(redshiftSink.columns.toSet === RedshiftExpectedFields)
          assert[String](redshiftSink, s"${Entity}_id", "learning-path-idschool_idclassId")
          assert[Int](redshiftSink, s"${Entity}_status", 1)

          assert(s3Sink.options.get("partitionBy") === Some("eventdate"))
          assert[String](s3Sink.dataFrame, "eventdate", "1970-07-14")

          val deltaCreateSink = sinks.collectFirst { case s: DeltaCreateSink => s }.get
          val deltaCreatedSinkDF = deltaCreateSink.df.cache()
          Delta.ExpectedFields.map(_.name).toSet must equal(RedshiftExpectedFields)
          assertExpectedFields(deltaCreatedSinkDF.schema.fields.toList, Delta.ExpectedFields)
          assert[Int](deltaCreatedSinkDF, s"${Entity}_status", 1)
          assert[String](deltaCreatedSinkDF, s"${LearningPathEntity}_name", "lp-name")
        }
      )
    }
  }

}
