package com.alefeducation.dimensions.course.delta

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.batch.delta.Alias
import com.alefeducation.dimensions.course.transform.CourseCurriculumMutatedTransform.CourseCurriculumEntityPrefix
import com.alefeducation.util.Constants.CourseInactiveStatusVal
import com.alefeducation.util.SparkSessionUtils

object CourseCurriculumMutatedDelta {
  import com.alefeducation.bigdata.batch.delta.Delta.Transformations._

  private val CourseCurriculumMutatedDeltaService = "delta-course-curriculum-mutated"
  val CourseCurriculumMutatedTransformedSink = "parquet-course-curriculum-mutated-transformed-source"
  private val CourseCurriculumDeltaSink: String = "delta-course-curriculum-sink"

  private val courseCurriculumDeltaMatchConditions: String =
    s"""
       |${Alias.Delta}.${CourseCurriculumEntityPrefix}_course_id = ${Alias.Events}.${CourseCurriculumEntityPrefix}_course_id
     """.stripMargin

  private val session = SparkSessionUtils.getSession(CourseCurriculumMutatedDeltaService)
  val service = new SparkBatchService(CourseCurriculumMutatedDeltaService, session)

  def main(args: Array[String]): Unit = {
    service.run {
      val source = service.readOptional(CourseCurriculumMutatedTransformedSink, session)
      source.flatMap(
        _.toIWH(matchConditions = courseCurriculumDeltaMatchConditions,
                uniqueIdColumns = List("cc_course_id"),
                inactiveStatus = CourseInactiveStatusVal)
          .map(_.toSink(CourseCurriculumDeltaSink, CourseCurriculumEntityPrefix))
      )
    }
  }
}
