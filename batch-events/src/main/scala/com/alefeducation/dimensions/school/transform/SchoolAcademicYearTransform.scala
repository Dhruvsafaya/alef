package com.alefeducation.dimensions.school.transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.dimensions.school.transform.SchoolAcademicYearTransform.{
  CreatedMapping,
  RolledOverMapping,
  SchoolAcademicYearRollOverCompletedSource,
  SchoolAcademicYearSwitchedSource,
  SchoolMutatedSource,
  SwitchedMapping,
  entityPrefix,
  key,
  sinkName
}
import com.alefeducation.schema.internal.ControlTableUtils.ProductMaxIdType
import com.alefeducation.service.DataSink
import com.alefeducation.util.Constants.AdminSchoolCreated
import com.alefeducation.util.Helpers.SchoolAcademicYearDimensionCols
import com.alefeducation.util.Resources.{getNestedString, getSink}
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}

class SchoolAcademicYearTransform(val session: SparkSession, val service: SparkBatchService) {
  import com.alefeducation.util.BatchTransformerUtility._
  import session.implicits._

  private def transformCreated(mutated: Option[DataFrame]): Option[DataFrame] = {
    mutated.map(
      _.filter($"eventType" === AdminSchoolCreated)
        .withColumn(s"${entityPrefix}Type", lit("CREATE"))
        .withColumn("previousAcademicYearId", lit(null).cast(StringType))
        .selectColumnsWithMapping(CreatedMapping)
    )

  }

  private def transformSwitched(academicYearSwitched: Option[DataFrame]): Option[DataFrame] = {
    academicYearSwitched.map(
      _.withColumn(s"${entityPrefix}Type", lit("SWITCH"))
        .selectColumnsWithMapping(SwitchedMapping)
    )
  }

  private def transformRolledOver(academicYearRolledOver: Option[DataFrame]): Option[DataFrame] = {
    academicYearRolledOver.map(
      _.withColumn(s"${entityPrefix}Type", lit("ROLLOVER"))
        .selectColumnsWithMapping(RolledOverMapping)
    )
  }

  def transform(): Option[DataSink] = {
    val createdColumns = CreatedMapping.values.toSet
    val switchedColumns = SwitchedMapping.values.toSet
    val rolledOverColumns = RolledOverMapping.values.toSet

    if ((createdColumns != switchedColumns) || (switchedColumns != rolledOverColumns)) {
      throw new Exception("The column mappings should have same columns set")
    }

    val schoolMutated = service.readOptional(SchoolMutatedSource, session)
    val created: Option[DataFrame] = transformCreated(schoolMutated)

    val academicYearSwitched = service.readOptional(SchoolAcademicYearSwitchedSource, session)
    val switched = transformSwitched(academicYearSwitched)

    val academicYearRolledOver = service.readOptional(SchoolAcademicYearRollOverCompletedSource, session)
    val rolledOver = transformRolledOver(academicYearRolledOver)

    val mutated = created.unionOptionalByName(switched).unionOptionalByName(rolledOver)

    val startId = service.getStartIdUpdateStatus(key)

    val mutatedIWH: Option[DataFrame] = mutated.flatMap(
      _.transformForIWH2(
        SchoolAcademicYearDimensionCols,
        entityPrefix,
        0,
        Nil,
        Nil,
        List("schoolId"),
        inactiveStatus = 2
      ).drop(s"${entityPrefix}_iwh_type")
        .genDwId(s"${entityPrefix}_dw_id", startId)
        .checkEmptyDf
    )

    mutatedIWH.map(DataSink(sinkName, _, controlTableUpdateOptions = Map(ProductMaxIdType -> key)))
  }
}

object SchoolAcademicYearTransform {
  val SchoolMutatedSource = "parquet-school-source"
  val SchoolAcademicYearSwitchedSource = "parquet-school-academic-year-switched-source"
  val SchoolAcademicYearRollOverCompletedSource = "parquet-school-academic-year-roll-over-completed-source"

  private val serviceName = "transform-school-academic-year"

  private val sinkName = getSink(serviceName).head
  private val key = getNestedString(serviceName, "key")
  private val entityPrefix = getNestedString(serviceName, "entity-prefix")

  private val session: SparkSession = SparkSessionUtils.getSession(serviceName)
  private val service = new SparkBatchService(serviceName, session)

  private val CreatedMapping = Map(
    "eventType" -> "eventType",
    "occurredOn" -> "occurredOn",
    s"${entityPrefix}Type" -> s"${entityPrefix}Type",
    "uuid" -> "schoolId",
    "currentAcademicYearId" -> "currentAcademicYearId",
    "previousAcademicYearId" -> "previousAcademicYearId",
  )

  private val SwitchedMapping = Map(
    "eventType" -> "eventType",
    "occurredOn" -> "occurredOn",
    s"${entityPrefix}Type" -> s"${entityPrefix}Type",
    "schoolId" -> "schoolId",
    "currentAcademicYearId" -> "currentAcademicYearId",
    "oldAcademicYearId" -> "previousAcademicYearId",
  )

  private val RolledOverMapping = Map(
    "eventType" -> "eventType",
    "occurredOn" -> "occurredOn",
    s"${entityPrefix}Type" -> s"${entityPrefix}Type",
    "schoolId" -> "schoolId",
    "id" -> "currentAcademicYearId",
    "previousId" -> "previousAcademicYearId",
  )

  def main(args: Array[String]): Unit = {
    val transformer = new SchoolAcademicYearTransform(session, service)
    service.run(transformer.transform())
  }

}