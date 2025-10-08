package com.alefeducation

import com.alefeducation.readers.{IDataFrameReader, IStreamReader, KafkaBasicReader, StagingParquetReader}
import com.alefeducation.util.DataFrameUtility.DataFrameExtraMethods
import com.alefeducation.util.Resources.loadMap
import com.alefeducation.util.{AWSSecretManager, Resources, SparkSessionUtils}
import com.alefeducation.utils.StreamingUtils._
import com.alefeducation.writers.{DeltaWriter, IDataFrameWriter, StagingParquetWriter}
import org.apache.log4j.Logger
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import com.alefeducation.util.StringUtilities.isAlpha
import org.apache.spark.sql.types.{ArrayType, BooleanType, DataType, DoubleType, LongType, StringType, StructType, TimestampType}

import scala.util.Try

class BehavioralV3StreamUnified(val reader: IStreamReader,
                                val writers: List[IDataFrameWriter],
                                val stagingReader: IDataFrameReader,
                                val stagingWriter: IDataFrameWriter)(implicit val spark: SparkSession) {

  val log: Logger = Logger.getLogger(classOf[BehavioralV3StreamUnified])

  import BehavioralV3StreamUnified.v3DeltaPath
  import spark.implicits._

  val DateTimeFormat = "yyyy-MM-dd HH:mm:ss.SSS"

  val thirtyMinutes: Int = 30 * 60

  val valueSchema: StructType = new StructType()
    .add("headers", "string")
    .add("body", "string")

  val mapTypes: Map[String, DataType] = Map(
    "StringType" -> StringType,
    "LongType" -> LongType,
    "DoubleType" -> DoubleType,
    "BooleanType" -> BooleanType,
    "TimestampType" -> TimestampType
  )

  val partitionOrderByClause: WindowSpec =
    Window.partitionBy(col("body_actor_id")).orderBy(col("body_timestamp"))

  def run(): Unit = {
    val kafkaStream = reader.read

    val inputDF = kafkaStream
      .select($"value".cast(StringType))
      .withColumn("value", from_json($"value", valueSchema))

    inputDF.writeStream
      .trigger(Trigger.Once)
      .format("delta")
      .option("checkpointLocation", s"$v3DeltaPath/_checkpoints/")
      .foreachBatch(processBatch _)
      .outputMode(OutputMode.Append())
      .start()
      .awaitTermination
  }

  def processBatch(df: Dataset[Row], batchId: Long): Unit = {
    df.persist()

    val parsedDF = df.parseJsonCol("value.headers", "headers")
    val journeys = parsedDF.select("headers.journey").distinct.as[String].collect.toList
    journeys.foreach(handleJourneyDF(parsedDF))

    df.unpersist()
  }

  def handleJourneyDF(df: DataFrame)(journey: String): Unit = {
    val journeyDf = df.where($"headers.journey" === journey)
      .parseJsonColWithReadJson("value.body", "body")
      .drop("value")
      .flattenDf()

    val path = makePath(journey)
    val eiStagingDf = readSafely(path).map(_.cache())

    val transformedDf = journeyDf.appendCommonColumnsForV3
      .transform(castAttemptForStudent(journey))
      .transform(castBodyScore)

    val joinedDf = eiStagingDf.fold(handleReadError(transformedDf), join(transformedDf))
      .transform(calculatePrevEventType)
      .transform(calculateTimeSpentCols)

    val rdf = joinedDf.filter(col("time_spent").isNotNull)
    val stagingDf = joinedDf.filter(col("time_spent").isNull)

    writers.foreach(_.insert(rdf, partitionBy = Seq("eventdate"))(path))
    stagingWriter.insert(stagingDf)(path)
  }

  def readSafely(path: String): Either[Throwable, DataFrame] = {
    Try(stagingReader.read(path)).toEither
  }

  def handleReadError(df: DataFrame)(t: Throwable): DataFrame = t match {
    case ae: AnalysisException if ae.message.startsWith("Path does not exist") => log.info(ae.message); df
    case e: Throwable => log.error(e.getMessage, e); df
  }

  def join(df: DataFrame)(stagingDf: DataFrame): DataFrame = {
    df.transform(addDefCols(stagingDf)).unionByName(stagingDf.transform(addDefCols(df)))
  }

  def addDefCols(stagingDf: DataFrame)(df: DataFrame): DataFrame = {
    val dfSchemaMap = df.schema.fields.map(f => f.name -> f).toMap

    stagingDf.schema.fields.foldLeft(df)((rdf, field) => {
      val name = field.name
      val dataType = field.dataType
      dataType match {
        case ArrayType(StructType(stagingFields), _) if df.columns.contains(name) =>
          //if the column type is an array type and the second table contains the same column

          //Find the difference between two dfs columns sets
          val (newArrayFields, allArrayFields) = dfSchemaMap(name).dataType match {
            case ArrayType(StructType(fields), _) =>
              val stagingFieldsSet = stagingFields.toSet
              val dfFieldsSet = fields.toSet
              (stagingFieldsSet diff dfFieldsSet, stagingFieldsSet ++ dfFieldsSet)
            case t: DataType => throw new Exception(s"Can't match column $name of ArrayType with the column of $t type")
          }

          //Add new nested fields
          val rdfWithNewFields = newArrayFields.foldLeft(rdf) {
            case (rrdf, f) => rrdf.withColumn(name,
              transform(col(name), arrStruct => arrStruct.withField(f.name, lit(null).cast(f.dataType))))
            //withField is available in Spark 3.1+
          }

          //Sort nested fields by name
          val sortedFields = allArrayFields.toList.sortWith(_.name > _.name)
          val arrayStructType = sortedFields.foldLeft(new StructType()) {
            case (aType, field) => aType.add(field)
          }
          val sortedCols = sortedFields.map(f => col(s"$name.${f.name}"))
          rdfWithNewFields
            .withColumn(name, arrays_zip(sortedCols: _*))
            .withColumn(name, col(name).cast(ArrayType(arrayStructType)))

        case _ if !df.columns.contains(name) => rdf.withColumn(name, lit(null).cast(dataType))
        case _ if df.columns.contains(name) => rdf
      }
    })
  }

  def castAttemptForStudent(journey: String)(df: DataFrame): DataFrame =
    if (journey == "student" && df.columns.contains("body_context_attempt")) {
      df.withColumn("body_context_attempt", col("body_context_attempt").cast(LongType))
    } else if (journey == "content" && !df.columns.contains("body_context_attempt")) {
      df.withColumn("body_context_attempt", lit(null).cast(LongType))
    } else df

  def castBodyScore(df: DataFrame): DataFrame =
    if (df.columns.contains("body_score")) {
      df.withColumn("body_score", col("body_score").cast(DoubleType))
    } else df

  def makePath(journey: String): String = {
    if (journey == "student" || journey == "content") "/journeyType=student_content"
    else if(isAlpha(journey)) s"/journeyType=$journey"
    else s"/journeyType=unsupported"
  }

  def calculateTimeSpentCols(df: DataFrame): DataFrame =
    df.withColumn("end_timestamp", lead(col("body_timestamp"), 1) over partitionOrderByClause)
      .withColumn("next_eventType", lead(col("eventType"), 1) over partitionOrderByClause)
      .withColumn("time_spent",
        unix_timestamp(col("end_timestamp").cast(TimestampType), DateTimeFormat) -
          unix_timestamp(col("body_timestamp").cast(TimestampType), DateTimeFormat)
      )
      .withColumn("time_spent",
        when(col("time_spent").isNull,
          when(
            (unix_timestamp(current_timestamp()) - unix_timestamp(col("body_timestamp").cast(TimestampType), DateTimeFormat)) > thirtyMinutes,
            thirtyMinutes
          ).otherwise(lit(null))
        ).otherwise(col("time_spent")))

  def calculatePrevEventType(df: DataFrame): DataFrame = {
    val prevEventType = lag(col("eventType"), 1) over partitionOrderByClause
    if (df.columns.contains("prev_eventType")) {
      df.withColumn("prev_eventType", when(col("prev_eventType").isNull, prevEventType)
        .otherwise(col("prev_eventType")))
    } else {
      df.withColumn("prev_eventType", prevEventType)
    }
  }
}

object BehavioralV3StreamUnified {

  private val name = "behavioral-v3-stream-unified"

  val v3DeltaPath: String = Resources.conf.getString("delta-behavioral-v3-unified-sink")
  val v3StagingParquetPath: String = Resources.conf.getString("parquet-staging-behavioral-v3-unified-source")

  def apply(secretManager: AWSSecretManager.type)(implicit session: SparkSession): BehavioralV3StreamUnified = {
    val options = loadMap("behavioral-v3-source")
    val kafkaProps = secretManager.getKafkaProps
    val reader = new KafkaBasicReader(options ++ kafkaProps)

    val writers = List(
      new DeltaWriter(v3DeltaPath)
    )

    val stagingReader = new StagingParquetReader(v3StagingParquetPath, session)
    val stagingWriter = new StagingParquetWriter(v3StagingParquetPath)

    new BehavioralV3StreamUnified(reader, writers, stagingReader, stagingWriter)
  }

  def main(args: Array[String]): Unit = {
    val secretManager: AWSSecretManager.type = AWSSecretManager
    try {
      secretManager.init(name)
      secretManager.loadCertificates()

      BehavioralV3StreamUnified(secretManager)(SparkSessionUtils.getSession(name)).run
    } finally {
      secretManager.disposeCertificates()
      secretManager.close()
    }
  }
}