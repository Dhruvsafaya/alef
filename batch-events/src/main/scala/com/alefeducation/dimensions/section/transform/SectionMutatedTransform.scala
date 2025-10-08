package com.alefeducation.dimensions.section.transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.dimensions.section.transform.SectionMutatedTransform.{sectionCreatedTransformedSink, sectionMutatedSource, sectionUpdatedTransformedSink}
import com.alefeducation.service.DataSink
import com.alefeducation.util.Constants.{AdminSectionCreated, AdminSectionUpdated}
import com.alefeducation.util.Helpers._
import com.alefeducation.util.Resources.{getSink, getSource}
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number}

class SectionMutatedTransform(val session: SparkSession, val service: SparkBatchService) {

  def transform(): List[Option[Sink]] = {
    import com.alefeducation.util.BatchTransformerUtility._

    val sectionMutatedDF = service.readOptional(sectionMutatedSource, session, extraProps = List(("mergeSchema", "true")))

    val sectionCreatedTransformedDf = sectionMutatedDF.map(
      _.filter(col("EventType") === AdminSectionCreated)
        .transformForInsertOrUpdate(SectionDimensionCols, SectionEntity)
    )

    //To get the latest Updated state for each section
    val windowSpec = Window.partitionBy("uuid").orderBy(col("occurredOn").desc)

    val sectionUpdatedTransformedDf = sectionMutatedDF.map(
      _.filter(col("EventType") === AdminSectionUpdated)
        .withColumn("row_number", row_number().over(windowSpec))
        .filter(col("row_number") === 1).drop("row_number")
        .transformForInsertOrUpdate(SectionDimensionCols, SectionEntity)
    )

    List(
      sectionCreatedTransformedDf.map(DataSink(sectionCreatedTransformedSink, _)),
      sectionUpdatedTransformedDf.map(DataSink(sectionUpdatedTransformedSink, _))
    )

  }
}



object SectionMutatedTransform {

  val sectionMutatedTransformService = "transform-section-mutated-service"
  val sectionMutatedSource = getSource(sectionMutatedTransformService).head
  val sectionCreatedTransformedSink = getSink(sectionMutatedTransformService).head
  val sectionUpdatedTransformedSink = getSink(sectionMutatedTransformService)(1)

  val session: SparkSession = SparkSessionUtils.getSession(sectionMutatedTransformService)
  val service = new SparkBatchService(sectionMutatedTransformService, session)

  def main(args: Array[String]): Unit = {
    val transformer = new SectionMutatedTransform(session, service)
    service.runAll(transformer.transform().flatten)
  }

}
