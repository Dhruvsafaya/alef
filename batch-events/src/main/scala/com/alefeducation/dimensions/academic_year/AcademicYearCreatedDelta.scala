package com.alefeducation.dimensions.academic_year

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.bigdata.batch.delta.Alias
import com.alefeducation.bigdata.batch.delta.Delta.Transformations._
import com.alefeducation.dimensions.academic_year.AcademicYearCreatedDelta.MatchConditions
import com.alefeducation.dimensions.academic_year.AcademicYearCreatedRedshift.{ColumnsToInsert, ColumnsToUpdate, toMapCols}
import com.alefeducation.util.Resources.getNestedString
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.SparkSession

class AcademicYearCreatedDelta(val session: SparkSession, val service: SparkBatchService) {

  def transform(createSourceName: String, sinkName: String): Option[Sink] = {
    val createdSource = service.readOptional(createSourceName, session)

    createdSource.flatMap(_.toUpsert(
      matchConditions = MatchConditions,
      partitionBy = Nil,
      columnsToInsert = toMapCols(ColumnsToInsert, Alias.Events),
      columnsToUpdate = toMapCols(ColumnsToUpdate, Alias.Events),
    )).map(_.toSink(sinkName))
  }
}

object AcademicYearCreatedDelta {

  val AcademicYearCreatedDeltaService = "delta-academic-year-created-service"

  private val session = SparkSessionUtils.getSession(AcademicYearCreatedDeltaService)
  val service = new SparkBatchService(AcademicYearCreatedDeltaService, session)

  val MatchConditions: String = s"""|${Alias.Delta}.academic_year_id = ${Alias.Events}.academic_year_id
                                    | AND ${Alias.Delta}.academic_year_school_id <=> ${Alias.Events}.academic_year_school_id
                                    |""".stripMargin

  def main(args: Array[String]): Unit = {
    val delta = new AcademicYearCreatedDelta(session, service)
    val createSourceName = getNestedString(AcademicYearCreatedDeltaService, "create-source")
    val sinkName = getNestedString(AcademicYearCreatedDeltaService, "sink")

    service.run(delta.transform(createSourceName, sinkName))
  }
}
