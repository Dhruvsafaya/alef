package com.alefeducation.dimensions.adt.transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.dimensions.adt.transform.CourseAbilityTestTransform.moduleTypeStrToValue
import com.alefeducation.schema.internal.ControlTableUtils.ProductMaxIdType
import com.alefeducation.service.DataSink
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Resources.{getList, getNestedMap, getNestedString, getSink}
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.functions.{col, typedLit}
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

class CourseAbilityTestTransform(val session: SparkSession, val service: SparkBatchService, val serviceName: String) {

  private def transformTestEvents(): Option[DataFrame] = {
    val plannedSource = getList(serviceName, "test-planned-source").head
    val updatedSource = getList(serviceName, "test-updated-source").head
    val unplannedSource = getList(serviceName, "test-unplanned-source").head
    val colsMap = getNestedMap(serviceName, "testColsMap")

    val plannedEvents = service.readOptional(plannedSource, session).map(_.selectColumnsWithMapping(colsMap))
    val updatedEvents = service.readOptional(updatedSource, session).map(_.selectColumnsWithMapping(colsMap))
    val unplannedEvents = service.readOptional(unplannedSource, session).map(_.selectColumnsWithMapping(colsMap))

    val mutated = plannedEvents.unionOptionalByNameWithEmptyCheck(updatedEvents).unionOptionalByNameWithEmptyCheck(unplannedEvents)
    mutated.map(_.withColumn("cata_ability_test_type", moduleTypeStrToValue(col("parentComponentType"))))
  }

  def transform(): List[Option[Sink]] = {
    val mutatedAdt = transformTestEvents()
    val sinkName = getSink(serviceName).head
    val key = getNestedString(serviceName, "key")
    val plannedEventNames = getList(serviceName, "planned-event-names")
    val unPlannedEventNames = getList(serviceName, "un-planned-event-names")
    val entityPrefix = getNestedString(serviceName, "entity-prefix")
    val cols = getNestedMap(serviceName, "colsMap")
    val groupKeys = getList(serviceName, "groupKeys")

    val startId = service.getStartIdUpdateStatus(key)
    val sinkAdt: Option[DataFrame] = mutatedAdt.flatMap(
      _.transformForIWH2(
        cols,
        entityPrefix,
        1,
        plannedEventNames,
        unPlannedEventNames,
        groupKey = groupKeys
      ).genDwId(s"${entityPrefix}_dw_id", startId)
        .checkEmptyDf
    )
    List(sinkAdt.map(DataSink(sinkName, _,  controlTableUpdateOptions = Map(ProductMaxIdType -> key))))
  }
}

object CourseAbilityTestTransform {
  private val ADT_MODULE_TYPE = 1
  private val moduleTypeStrToValue = typedLit(Map("DIAGNOSTIC_TEST" -> ADT_MODULE_TYPE))

  val metadataSchema: StructType = StructType(
    Seq(
      StructField("tags",
        ArrayType(
          StructType(
            Seq(
              StructField("key", StringType),
              StructField("values", ArrayType(StringType)),
              StructField("attributes", ArrayType(StructType(
                Seq(
                  StructField("value", StringType),
                  StructField("color", StringType),
                  StructField("translation", StringType)
                )
              ))),
              StructField("type", StringType)
            )
          )
        )
      ),
      StructField("version", StringType),
    )
  )

  def main(args: Array[String]): Unit = {
    val serviceName = args(0)
    val session: SparkSession = SparkSessionUtils.getSession(serviceName)
    val service = new SparkBatchService(serviceName, session)
    val transformer = new CourseAbilityTestTransform(session, service, serviceName)
    service.runAll(
      transformer.transform().flatten
    )
  }
}
