package com.alefeducation.dimensions.curriculum.dim_curriculum

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.dimensions.curriculum.dim_curriculum.CurriculumDimTransform.{CurriculumDimKey, CurriculumDimSinkName, CurriculumDimSourceName}
import com.alefeducation.schema.internal.ControlTableUtils.ProductMaxIdType
import com.alefeducation.service.DataSink
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Helpers._
import com.alefeducation.util.Resources.{getSink, getSource}
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

class CurriculumDimTransform(val session: SparkSession, val service: SparkBatchService) {
  def transform(): List[Option[Sink]] = {

    val currCreatedDF: Option[DataFrame] = service.readOptional(CurriculumDimSourceName, session, extraProps = List(("mergeSchema", "true")))

    val startId = service.getStartIdUpdateStatus(CurriculumDimKey)

    val currCreatedTransformedDF = currCreatedDF.map(
      _.genDwId("curr_dw_id", startId)
        .transformForInsertDim(CurriculumCreatedDimCols, CurriculumEntity, List("curriculumId"))
    )

    List (
      currCreatedTransformedDF.map(df => {
        DataSink(CurriculumDimSinkName, df, controlTableUpdateOptions = Map(ProductMaxIdType -> CurriculumDimKey))
      })
    )
  }

}

object CurriculumDimTransform {

  private val CurriculumDimTransformService: String = "transform-curriculum-created-service"

  val CurriculumDimKey = "dim_curriculum"

  val CurriculumDimSourceName: String = getSource(CurriculumDimTransformService).head
  val CurriculumDimSinkName: String = getSink(CurriculumDimTransformService).head

  val session: SparkSession = SparkSessionUtils.getSession(CurriculumDimTransformService)
  val service = new SparkBatchService(CurriculumDimTransformService, session)
  def main(args: Array[String]): Unit = {
    val transformer = new CurriculumDimTransform(session, service)
    service.runAll(transformer.transform().flatten)
  }
}
