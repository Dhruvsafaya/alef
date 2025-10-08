package com.alefeducation.dimensions.section.delta

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.batch.delta.Alias
import com.alefeducation.dimensions.grade.transform.GradeCreatedTransform.GradeCreatedSinkName
import com.alefeducation.io.data.RedshiftIO.TempTableAlias
import com.alefeducation.util.Helpers.SectionEntity
import com.alefeducation.util.Resources.{getSink, getSource}
import com.alefeducation.util.SparkSessionUtils

object SectionMutatedDelta {


  import com.alefeducation.bigdata.batch.delta.Delta.Transformations._

  private val sectionMutatedDeltaService = "delta-section-service"

  private val session = SparkSessionUtils.getSession(sectionMutatedDeltaService)
  val service = new SparkBatchService(sectionMutatedDeltaService, session)

  val sectionDeltaMatchCondition =
    s"""
       |${Alias.Delta}.${SectionEntity}_id = ${Alias.Events}.${SectionEntity}_id
     """.stripMargin

  val sectionDeltaCols: Seq[String] = Seq(
    "section_id",
    "section_status",
    "section_alias",
    "section_name",
    "section_enabled",
    "section_source_id",
    "tenant_id",
    "grade_id",
    "school_id",
    "section_created_time",
    "section_updated_time",
    "section_deleted_time",
    "section_dw_created_time",
    "section_dw_updated_time"
  )

  val sectionColumnsToInsert: Map[String, String] = sectionDeltaCols.map { column =>
    column -> s"${Alias.Events}.$column"
  }.toMap

  val sectionColumnsToUpdate: Map[String, String] = sectionColumnsToInsert -
    "section_dw_created_time" +
    ("section_dw_updated_time" -> s"${Alias.Events}.section_dw_created_time",
      "section_updated_time" -> s"${Alias.Events}.section_created_time")

  println("Hello")
  println(sectionColumnsToUpdate)

  private def processSectionMutatedDelta(sourceIndex: Int): Unit = {
    val transformedDF = service.readOptional(getSource(sectionMutatedDeltaService)(sourceIndex), session)

    val deltaSink = transformedDF.flatMap(
      _.toUpsert(
        partitionBy = Nil,
        matchConditions = sectionDeltaMatchCondition,
        columnsToInsert = sectionColumnsToInsert,
        columnsToUpdate = sectionColumnsToUpdate
      ).map(_.toSink(getSink(sectionMutatedDeltaService).head))
    )

    service.run(deltaSink)
  }

  def main(args: Array[String]): Unit = {

    processSectionMutatedDelta(0) // Process sectionCreated
    processSectionMutatedDelta(1) // Process sectionUpdated

  }
}
