package com.alefeducation.dimensions

import com.alefeducation.bigdata.Sink
import com.alefeducation.bigdata.batch.delta.Delta.Transformations._
import com.alefeducation.service.SparkBatchService
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Constants._
import com.alefeducation.util.Helpers._
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, IntegerType}
import org.apache.spark.sql.{DataFrame, SparkSession}

class ContentDimension(override val name: String, override implicit val session: SparkSession) extends SparkBatchService {
  override def transform(): List[Sink] = {

    val createdDF = readOptional(ParquetContentCreatedSource, session, isMandatory = false).map(transformTimeStatusAndCreateEvtCols)
    val updatedDF = readOptional(ParquetContentUpdatedSource, session, isMandatory = false).map(transformTimeAndStatus)
    val publishedDF = readOptional(ParquetContentPublishedSource, session, isMandatory = false).map(handleColsForPublishEvt)
    val deletedDF = readOptional(ParquetContentDeletedSource, session, isMandatory = false).map(handleColsForDeleteEvt)

    val transformCreatedDf = createdDF.map(_.transformForInsertDim(ContentCreatedDimensionCols, ContentEntity, List("contentId")))
    val transformUpdatedDf = updatedDF.map(_.transformForUpdateDim(ContentDimensionCols, ContentEntity, List("contentId")))
    val transformPublishedDf = publishedDF.map(_.transformForUpdateDim(ContentPublishedDimensionCols, ContentEntity, List("contentId")))
    val transformDeletedDf = deletedDF.map(_.transformForUpdateDim(ContentDeletedDimensionCols, ContentEntity, List("contentId")))

    val parquetCreatedSink = createdDF.map(_.toParquetSink(ParquetContentCreatedSink))
    val parquetUpdatedSink = updatedDF.map(_.toParquetSink(ParquetContentUpdatedSink))
    val parquetPublishedSink = publishedDF.map(_.toParquetSink(ParquetContentPublishedSink))
    val parquetDeletedSink = deletedDF.map(_.toParquetSink(ParquetContentDeletedSink))

    val colToExcludeFromRS =
      Seq("content_description", "content_copyrights")

    val redshiftCreatedSink =
      transformCreatedDf.map(
        _.drop(colToExcludeFromRS: _*)
          .transform(transformRedshiftColumns)
          .toRedshiftInsertSink(RedshiftContentCreatedSink, ContentCreatedEvent))
    val redshiftUpdatedSink = transformUpdatedDf.map(
      _.drop(colToExcludeFromRS: _*)
        .transform(transformRedshiftColumns)
        .toRedshiftUpdateSink(RedshiftContentUpdatedSink, ContentUpdatedEvent, ContentEntity))
    val redshiftPublishedSink =
      transformPublishedDf.map(_.toRedshiftUpdateSink(RedshiftContentPublishedSink, ContentPublishedEvent, ContentEntity))
    val redshiftDeletedSink = transformDeletedDf.map(_.toRedshiftUpdateSink(RedshiftContentDeletedSink, ContentDeletedEvent, ContentEntity))

    val delta = transformCreatedDf.flatMap(
      _.withColumn("content_published_date", lit(null).cast(DateType))
        .withColumn("content_publisher_id", lit(null).cast(IntegerType))
        .toCreate()
        .map(_.toSink(DeltaContentCreatedSink)))
    val deltaUpdate = transformUpdatedDf.flatMap(_.toUpdate().map(_.toSink(DeltaContentUpdatedSink, ContentEntity)))
    val deltaPublish = transformPublishedDf.flatMap(_.toUpdate().map(_.toSink(DeltaContentPublishedSink, ContentEntity)))
    val deltaDelete = transformDeletedDf.flatMap(_.toDelete().map(_.toSink(DeltaContentDeletedSink, ContentEntity)))

    parquetCreatedSink.toList ++ parquetUpdatedSink ++ parquetPublishedSink ++ parquetDeletedSink ++
      redshiftCreatedSink.toList ++ redshiftUpdatedSink ++ redshiftPublishedSink ++ redshiftDeletedSink ++
      delta.toList ++ deltaUpdate.toList ++ deltaPublish.toList ++ deltaDelete.toList
  }

  def handleColsForDeleteEvt(df: DataFrame): DataFrame = df.withColumn("content_status", lit(4))

  def handleColsForPublishEvt(df: DataFrame): DataFrame = df.withColumn("publishedDate", col("publishedDate").cast("date"))
  def handleColsForCreateEvt(df: DataFrame): DataFrame =
    df.withColumn("createdAt", to_timestamp(from_unixtime(col("createdAt") / 1000, "yyyy-MM-dd hh:mm:ss")))
      .withColumn("content_status", lit(1))

  def transformTimeAndStatus(df: DataFrame): DataFrame =
    df.withColumn("fileUpdatedAt", to_timestamp(from_unixtime(col("fileUpdatedAt") / 1000, "yyyy-MM-dd HH:mm:ss")))
      .withColumn("authoredDate", col("authoredDate").cast("date"))
      .withColumn("status",
                  when(col("status") === "DRAFT", 1)
                    .when(col("status") === "PUBLISHED", 4)
                    .when(col("status") === "RE-PUBLISHED", 5)
                    .otherwise(999))

  def transformTimeStatusAndCreateEvtCols(df: DataFrame): DataFrame = (transformTimeAndStatus _ andThen handleColsForCreateEvt)(df)

  def transformRedshiftColumns(df: DataFrame): DataFrame =
    df.withColumn(
        "content_learning_resource_types",
        when(size(col("content_learning_resource_types")) > ZeroArraySize, concat_ws(", ", col("content_learning_resource_types")))
          .otherwise(lit(DefaultStringValue))
      )
      .withColumn(
        "content_cognitive_dimensions",
        when(size(col("content_cognitive_dimensions")) > ZeroArraySize, concat_ws(", ", col("content_cognitive_dimensions")))
          .otherwise(lit(DefaultStringValue))
      )

}
object ContentDimension {
  def main(args: Array[String]): Unit =
    new ContentDimension(ContentDimensionName, SparkSessionUtils.getSession(ContentDimensionName)).run
}
