package com.alefeducation.dimensions.curriculum.dim_curriculum_subject.delta

import com.alefeducation.base.SparkBatchService
import com.alefeducation.util.Helpers.DeltaCurriculumSubjectCreatedSink
import com.alefeducation.util.Resources.{getSink, getSource}
import com.alefeducation.util.SparkSessionUtils

object CurriculumSubjectCreatedDelta {

  import com.alefeducation.bigdata.batch.delta.Delta.Transformations._

  private val currSubjectCreatedDeltaService = "delta-curriculum-subject-created-service"

  private val session = SparkSessionUtils.getSession(currSubjectCreatedDeltaService)
  val service = new SparkBatchService(currSubjectCreatedDeltaService, session)

  def main(args: Array[String]): Unit = {
    service.run {
      val currSubjectCreatedTransformedDF = service.readOptional(getSource(currSubjectCreatedDeltaService).head, session)

      currSubjectCreatedTransformedDF.flatMap(_.toCreate().map(_.toSink(getSink(currSubjectCreatedDeltaService).head)))
    }
  }
}
