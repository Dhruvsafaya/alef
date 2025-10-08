package com.alefeducation.dimensions.course_activity_container.transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.dimensions.course_activity_container.transform.ContainerMutatedTransform.{ContainerAddedEvent, ContainerDeletedEvent, ContainerPublishedEvent, ContainerUpdatedEvent, metadataSchema}
import com.alefeducation.schema.internal.ControlTableUtils.ProductMaxIdType
import com.alefeducation.service.DataSink
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Helpers.Deleted
import com.alefeducation.util.Resources.{getNestedString, getSink, getSource}
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.types.{BooleanType, IntegerType, StringType}
import org.apache.spark.sql.{DataFrame, SparkSession, functions => F}

class ContainerGradeTransform(val session: SparkSession, val service: SparkBatchService) {

  import ContainerGradeTransform._

  val requiredCols: List[String] = List(
    "eventType",
    "id",
    "courseId",
    "occurredOn",
    "metadata"
  )

  def transform(sourceNames: List[String], sinkName: String): Option[Sink] = {
    val sourceFiles: List[Option[DataFrame]] = sourceNames
      .map(service
        .readUniqueOptional(_, session, uniqueColNames = UniqueColNames)
        .map(
          _.select(requiredCols.head, requiredCols.tail: _*)
            .addColIfNotExists("isAccelerated", BooleanType))
      )

    val sources = sourceFiles.map(source => addGrade(source).map(_.select("id", "courseId", "cacga_grade", "occurredOn", "eventType")))
    val explodeGrade = sources.tail.foldLeft(sources.head)(_ unionOptionalByName  _)

    val startId = service.getStartIdUpdateStatus(ContainerGroupKey)

    val transformed = explodeGrade.map(
      _.transformForIWH2(
        ContainerGradeCols,
        ContainerGradeEntity,
        0,
        attachedEvents = List(ContainerPublishedEvent, ContainerAddedEvent, ContainerUpdatedEvent),
        detachedEvents = List(ContainerDeletedEvent),
        groupKey = GroupKey,
        associationDetachedStatusVal = Deleted,
        inactiveStatus = Deleted
      ).genDwId("cacga_dw_id", startId)
    )

    transformed.map(DataSink(sinkName, _, controlTableUpdateOptions = Map(ProductMaxIdType -> ContainerGroupKey)))
  }

  def addGrade(container: Option[DataFrame]): Option[DataFrame] = {

    import session.implicits._

    val containerWithIsEmptyMetadataFlag: Option[DataFrame] = container.map(
      _.withColumn(
        "is_metadata_empty", F.when(F.size($"metadata.tags") === 0, F.lit(true))
          .otherwise(F.lit(false))
      )
    )
    val containerNonEmptyMetadata: Option[DataFrame] = containerWithIsEmptyMetadataFlag.flatMap(
      _.filter($"is_metadata_empty" === false)
        .drop("is_metadata_empty")
        .checkEmptyDf
    )
    val containerWithGrade: Option[DataFrame] = containerNonEmptyMetadata.map(
      _.withColumn(
        "grade_index", F.array_position($"metadata.tags.key", "Grade").cast(IntegerType)
      ).withColumn(
        "grades_tag", F.when($"grade_index" === 0, F.lit(null))
          .otherwise(F.element_at($"metadata.tags", $"grade_index"))
      ).withColumn(
        "grades", $"grades_tag.values"
      ).withColumn(
        "cacga_grade", F.explode_outer($"grades")
      ).drop("grade_index", "grades_tag", "grades")
    )

    val containerEmptyMetadata: Option[DataFrame] = containerWithIsEmptyMetadataFlag.flatMap(
      _.filter($"is_metadata_empty" === true)
        .withColumn("cacga_grade", F.lit(null).cast(StringType))
        .drop("is_metadata_empty")
        .checkEmptyDf
    )

    containerEmptyMetadata.unionOptionalByNameWithEmptyCheck(containerWithGrade)
  }

}

object ContainerGradeTransform {

  def readSources(sourceNames: List[DataFrame]): Option[DataFrame] = {
    val sources = sourceNames
      .map(_.addColIfNotExists("isAccelerated", BooleanType))

    if (sources.isEmpty) None
    else {
      Some(sources.tail.foldLeft(sources.head)(_ unionByName _))
    }
  }

  val ContainerGradeTransformService = "container-grade-transform"

  val ContainerGradeEntity = "cacga"

  val ContainerGroupKey = "dim_course_activity_container_grade_association"

  val GroupKey: List[String] = List("id")
  val UniqueColNames: Seq[String] = "courseVersion" :: GroupKey

  val session: SparkSession = SparkSessionUtils.getSession(ContainerGradeTransformService)
  val service = new SparkBatchService(ContainerGradeTransformService, session)

  val ContainerGradeCols: Map[String, String] = Map[String, String](
    "id" -> "cacga_container_id",
    "courseId" -> "cacga_course_id",
    "cacga_grade" -> "cacga_grade",
    "occurredOn" -> "occurredOn",
    "cacga_status" -> "cacga_status"
  )

  def main(args: Array[String]): Unit = {
    val t = new ContainerGradeTransform(session, service)
    val sourceNames = getSource(ContainerGradeTransformService)
    val sinkName = getSink(ContainerGradeTransformService).head
    service.run(t.transform(sourceNames, sinkName))
  }
}


