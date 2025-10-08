package com.alefeducation.dimensions

import com.alefeducation.bigdata.Sink
import com.alefeducation.bigdata.batch.delta.Delta
import com.alefeducation.schema.activity.{PerformanceConditions, QuestionAttempts}
import com.alefeducation.service.SparkBatchService
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Constants._
import com.alefeducation.util.Helpers._
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, IntegerType, MetadataBuilder, StringType, StructType, TimestampType}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}


class CCLActivityTemplateDimension(override val session: SparkSession) extends SparkBatchService {
  private val colLength = 1024
  private val metadata = new MetadataBuilder().putLong("maxlength", colLength).build()

  import Delta.Transformations._

  override val name: String = CCLActivityTemplateDimensionName

  private val colsFilter = Set(MainComponents, SupportingComponents, SideComponents)
  private val typeField = "type"

  private val uniqueRedshiftKey = List("at_uuid")
  private val uniqueDeltaKey = List("at_id")
  private val groupKey = List("templateUuid")

  private val associationType = 0

  private val eventType = "ActivityTemplateType"

  private val orderByField = "dateCreated"


  override def transform(): List[Sink] = {
    val activityTemplateDf: Option[DataFrame] = readOptional(ParquetCCLActivityTemplateSource, session, isMandatory = false)

    val activityTemplateCachedDf = activityTemplateDf.map(_.cache())

    val flattenActivityTemplateDf = for {
      cols <- activityTemplateCachedDf.map(_.columns.filterNot(colsFilter(_)).map(col))

      mainCompExpl = cols :+ explode(col(MainComponents))
      supportingCompExpl = cols :+ explode(col(SupportingComponents))
      sideCompExpl = cols :+ explode(col(SideComponents))

      atDf <- activityTemplateCachedDf

      mainCompDf = processCols(atDf, mainCompExpl, SectionTypeMainComp)
      supportingCompDf = processCols(atDf, supportingCompExpl, SectionTypeSupportingComp)
      sideCompDf = processCols(atDf, sideCompExpl, SectionTypeSideComp)

    } yield mainCompDf.unionByName(supportingCompDf).unionByName(sideCompDf)

    val flattenActivityTemplateWithCols = flattenActivityTemplateDf.map(
      _.transform(addComponentType)
        .transform(castPublishedDate)
    )

    val activityTemplateTransformedRedshiftDf: Option[DataFrame] =
      flattenActivityTemplateWithCols.map(
        _.transformForIWH2(
          CCLActivityTemplateRedshiftCols,
          ActivityTemplateEntity,
          associationType,
          List(ActivityTemplatePublishedEvent),
          Nil,
          groupKey,
          orderByField
        ).withColumn("at_description", col("at_description")
          .as("at_description", metadata))
      )

    val activityTemplateTransformerDeltaDf: Option[DataFrame] =
      activityTemplateCachedDf.map(
        _.transform(castPublishedDate)
          .transformForIWH2(
            CCLActivityTemplateDeltaCols,
            ActivityTemplateEntity,
            associationType,
            List(ActivityTemplatePublishedEvent),
            Nil,
            groupKey,
            orderByField
          )
      )

    val activityTemplateParquetSink = activityTemplateDf.map(_.toParquetSink(ParquetCCLActivityTemplateSink, orderByField))

    val activityTemplateRedshiftSink = activityTemplateTransformedRedshiftDf.map(
      _.toRedshiftIWHSink(
        RedshiftCCLActivityTemplateSink,
        ActivityTemplateEntity,
        ids = uniqueRedshiftKey,
        eventType = eventType
      )
    )

    val activityTemplateDeltaSink = activityTemplateTransformerDeltaDf.flatMap(
      _.toIWH(uniqueIdColumns = uniqueDeltaKey)
        .map(_.toSink(DeltaCCLActivityTemplateSink, ActivityTemplateEntity, eventType))
    )

    (activityTemplateParquetSink ++ activityTemplateRedshiftSink ++ activityTemplateDeltaSink).toList
  }

  def addComponentType(df: DataFrame): DataFrame =
    df.withColumn("at_component_type", when(col(typeField) === Content, lit(ContentVal))
      .when(col(typeField) === Assignment, lit(AssignmentVal))
      .when(col(typeField) === Assessment, lit(AssessmentVal))
      .when(col(typeField) === KeyTerm, lit(KeyTermVal))
      .otherwise(Undefined)
    )

  def processCols(df: DataFrame, cols: Array[Column], sectionTypeVal: Int): DataFrame =
    explodeCols(df, cols).transform(addSectionType(sectionTypeVal)).transform(castCols)

  def explodeCols(df: DataFrame, cols: Array[Column]): DataFrame =
    df.select(cols: _*)
      .withColumnRenamed("name", "activityTemplateName")
      .select("*", "col.*")
      .drop("col")
      .castStructCol[QuestionAttempts]("questionAttempts")
      .select("*", "questionAttempts.*")
      .select(col("*"), explode_outer(col("performanceConditions")).as("performanceConditionsExp"))
      .castStructCol[PerformanceConditions]("performanceConditionsExp")
      .select(col("*"), col("performanceConditionsExp.*"))
      .drop("questionAttempts", "performanceConditions", "performanceConditionsExp", "col")

  def addSectionType(sectionTypeVal: Int)(df: DataFrame): DataFrame =
    df.withColumn(SectionType, lit(sectionTypeVal))

  def castCols(df: DataFrame): DataFrame = {
    val newDf = df.withColumn("maxRepeat", col("maxRepeat").cast(IntegerType))
      .withColumn("passingScore", col("passingScore").cast(IntegerType))
      .withColumn("assessmentsAttempts", col("assessmentsAttempts").cast(IntegerType))
      .addColIfNotExists("releaseConditions")

    val nullCol = lit(null).cast(ArrayType(StringType))
    val releaseConditionField = newDf.schema.fields.find(_.name == "releaseConditions")
    val replacementCol = releaseConditionField.get.dataType match {
      case _: StringType => nullCol
      case _ => col("releaseConditions")
    }
    newDf.withColumn("releaseConditions", replacementCol)
  }

  def castPublishedDate(df: DataFrame): DataFrame =
    df.withColumn("publishedDate", regexp_replace(col("publishedDate"), "T", " ").cast(TimestampType))
}

object CCLActivityTemplateDimension {

  def main(args: Array[String]): Unit = {
    new CCLActivityTemplateDimension(SparkSessionUtils.getSession(CCLActivityTemplateDimensionName)).run
  }
}
