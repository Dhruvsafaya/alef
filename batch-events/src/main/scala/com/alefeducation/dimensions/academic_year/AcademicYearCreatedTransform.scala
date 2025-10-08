package com.alefeducation.dimensions.academic_year

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.schema.internal.ControlTableUtils.ProductMaxIdType
import com.alefeducation.service.DataSink
import com.alefeducation.util.Helpers.AcademicYearEntity
import com.alefeducation.util.Resources.getNestedString
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

class AcademicYearCreatedTransform(val session: SparkSession, val service: SparkBatchService) {

  import AcademicYearCreatedTransform._
  import com.alefeducation.util.BatchTransformerUtility._

  def transform(createSourceName: String, createSinkName: String): Option[Sink] = {
    val createSource = service.readOptional(createSourceName, session)

    val startId = service.getStartIdUpdateStatus(AcademicYearKey)

    val createTransformed = createSource.map(_.transform(applyTransformations).genDwId(s"${AcademicYearEntity}_delta_dw_id", startId))

    createTransformed.map(DataSink(createSinkName, _, controlTableUpdateOptions = Map(ProductMaxIdType -> AcademicYearKey)))
  }

}

object AcademicYearCreatedTransform {

  import com.alefeducation.bigdata.batch.BatchUtils.DataFrameUtils
  import com.alefeducation.util.BatchTransformerUtility._

  val AcademicYearCreatedService = "transform-academic-year-created-service"

  val AcademicYearKey = "dim_academic_year"

  val session: SparkSession = SparkSessionUtils.getSession(AcademicYearCreatedService)
  val service = new SparkBatchService(AcademicYearCreatedService, session)

  val AcademicYearMutatedCols: Map[String, String] = Map(
    "uuid" -> s"${AcademicYearEntity}_id",
    "schoolId" -> s"${AcademicYearEntity}_school_id",
    "organization" -> s"${AcademicYearEntity}_organization_code",
    "startDate" -> s"${AcademicYearEntity}_start_date",
    "endDate" -> s"${AcademicYearEntity}_end_date",
    "createdBy" -> s"${AcademicYearEntity}_created_by",
    "updatedBy" -> s"${AcademicYearEntity}_updated_by",
    "status" -> s"${AcademicYearEntity}_state",
    s"${AcademicYearEntity}_status" -> s"${AcademicYearEntity}_status",
    s"${AcademicYearEntity}_is_roll_over_completed" -> s"${AcademicYearEntity}_is_roll_over_completed",
    "type" -> s"${AcademicYearEntity}_type",
    "occurredOn" -> "occurredOn"
  )

  val RollOverStatus = "CONCLUDED"

  def addRollOverCompletedCol(df: DataFrame): DataFrame =
    df.withColumn(s"${AcademicYearEntity}_is_roll_over_completed", when(col("status") === RollOverStatus, lit(true)).otherwise(lit(false)))

  def applyTransformations(df: DataFrame): DataFrame = {
    df.epochToDate("startDate")
      .epochToDate("endDate")
      .transform(addRollOverCompletedCol)
      .transformForInsertDim(
        AcademicYearMutatedCols,
        AcademicYearEntity,
        List("uuid")
      )
  }

  def main(args: Array[String]): Unit = {
    val transformer = new AcademicYearCreatedTransform(session, service)
    val createSourceName = getNestedString(AcademicYearCreatedService, "create-source")
    val createSinkName = getNestedString(AcademicYearCreatedService, "create-sink")
    service.run(transformer.transform(createSourceName, createSinkName))
  }
}
