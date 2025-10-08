package com.alefeducation.dimensions.curriculum.dim_curriculum

import com.alefeducation.base.SparkBatchService
import com.alefeducation.util.Resources.{getSink, getSource}
import com.alefeducation.util.SparkSessionUtils

object CurriculumDimDelta {

  import com.alefeducation.bigdata.batch.delta.Delta.Transformations._

  private val currCreatedDeltaService = "delta-curriculum-created-service"

  private val session = SparkSessionUtils.getSession(currCreatedDeltaService)
  val service = new SparkBatchService(currCreatedDeltaService, session)

  def main(args: Array[String]): Unit = {
    service.run {
      val currCreatedTransformedDF = service.readOptional(getSource(currCreatedDeltaService).head, session)

      currCreatedTransformedDF.flatMap(
        _.toCreate().map(_.toSink(getSink(currCreatedDeltaService).head))
      )
    }
  }
}
