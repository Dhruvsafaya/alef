package com.alefeducation.dimensions.curriculum.dim_curriculum_grade

import com.alefeducation.base.SparkBatchService
import com.alefeducation.util.Resources.{getSink, getSource}
import com.alefeducation.util.SparkSessionUtils

object CurriculumGradeDimDelta {

  import com.alefeducation.bigdata.batch.delta.Delta.Transformations._

  private val currGradeCreatedDeltaService = "delta-curriculum-grade-created-service"

  private val session = SparkSessionUtils.getSession(currGradeCreatedDeltaService)
  val service = new SparkBatchService(currGradeCreatedDeltaService, session)

  def main(args: Array[String]): Unit = {
    service.run {
      val currGradeCreatedTransformedDF = service.readOptional(getSource(currGradeCreatedDeltaService).head, session)

      currGradeCreatedTransformedDF.flatMap(
        _.toCreate().map(_.toSink(getSink(currGradeCreatedDeltaService).head))
      )
    }
  }
}
