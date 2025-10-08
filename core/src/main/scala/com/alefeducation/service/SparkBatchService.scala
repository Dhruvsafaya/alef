package com.alefeducation.service

import com.alefeducation.bigdata.Sink
import com.alefeducation.bigdata.batch.{DeltaSink, DataSink => DataSinkAdapter}
import com.alefeducation.bigdata.batch.delta.{DeltaSink => NewDeltaSink}
import com.alefeducation.exception.{NoParquetFileException, SinkNotConfiguredException}
import com.alefeducation.util.AwsUtils.isConnectionEnabled
import com.alefeducation.util.Constants.{DbTable, Format, Path}
import com.alefeducation.util.DataFrameUtility.setNullableStateForAllStringColumns
import com.alefeducation.util.Resources.loadMap
import com.alefeducation.util.SparkSessionUtils.getJsonData
import com.alefeducation.util.{AwsUtils, Resources}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{AnalysisException, DataFrame, DataFrameReader, Encoder, SparkSession}

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

@deprecated("For new refactored jobs please user SparkBatchServiceReader and SparkBatchServiceWriter")
trait SparkBatchService extends Service {

  val parquetFileFormat = "parquet"
  val fileFormats: Set[String] = Set(parquetFileFormat, "csv", "delta", "text")
  val pathNotFoundSet = Set(
    "Unable to infer schema for Parquet.",
    "[UNABLE_TO_INFER_SCHEMA]",
    "Path does not exist:",
    "[PATH_NOT_FOUND]"
  )

  override def prehook: Unit = {}

  def readOptional(config: String,
                   session: SparkSession,
                   isMandatory: Boolean = false,
                   optionalSchema: Option[StructType] = None,
                   extraProps: List[(String, String)] = List.empty): Option[DataFrame] = {
    readWithExtraProps(config: String, session, isMandatory, optionalSchema, extraProps)
  }

  @deprecated("This will be made private and in all cases suggest to use readOptional method instead")
  override def read(config: String,
                    session: SparkSession,
                    isMandatory: Boolean = false,
                    optionalSchema: Option[StructType] = None): DataFrame = {
    readWithProps(config, session, isMandatory, optionalSchema).getOrElse(session.emptyDataFrame)
  }

  def readWithExtraProps(config: String,
                    session: SparkSession,
                    isMandatory: Boolean = false,
                    optionalSchema: Option[StructType] = None,
                    extraProps: List[(String, String)] = Nil): Option[DataFrame] = {
    readWithProps(config, session, isMandatory, optionalSchema, extraProps).map(_.cache())
  }

  private def readWithProps(config: String,
                              session: SparkSession,
                              isMandatory: Boolean = false,
                              optionalSchema: Option[StructType] = None,
                              extraProps: List[(String, String)] = Nil): Option[DataFrame] = {
    Try {
      if (isConnectionEnabled(session)) {
        val props: List[(String, String)] = loadMap(config).toList ++ extraProps
        Some(optionalSchema
          .map(schema => setSourceOps(props, session.read).schema(schema).load)
          .getOrElse(setSourceOps(props, session.read).load))
      } else getJsonData(session, config)
    } match {
      case Success(Some(df)) => Some(df.dropDuplicates((df.columns.toSet - "loadtime" - "_load_time").toSeq))
      case Success(None) => None
      case Failure(e:AnalysisException) if isParquetNotFound(e.getMessage()) =>
        val message = s"Could not find dataframe $config due to ${e.getMessage}"
        if (isMandatory) throw new NoParquetFileException(message) else None
      case Failure(e) => throw e
    }
  }

  private def isParquetNotFound(msg: String): Boolean = {
    pathNotFoundSet.exists(msg.startsWith) ||
      msg.contains("doesn't exist;") ||
      msg.contains("is not a Delta table.")
  }

  @deprecated("Use readFromRedshift from SparkBatchSeriveReader for new refactored jobs")
  def readFromRedshift[T](redshiftSource: String, options: List[(String, String)] = Nil, selectedCols: List[String] = List("*"))(implicit encoder: Encoder[T]): DataFrame = {
    readFromRedshift(redshiftSource, options).select(selectedCols.map(x => col(x)): _*)
  }

  def readFromFabric[T](redshiftSource: String, options: List[(String, String)] = Nil, selectedCols: List[String] = List("*"))(implicit encoder: Encoder[T]): DataFrame = {
    readFromFabric(redshiftSource, options).select(selectedCols.map(x => col(x)): _*)
  }

  def readFromRedshiftOptional[T](redshiftSource: String, selectedCols: List[String] = List("*"))(
    implicit encoder: Encoder[T]): Option[DataFrame] = {
    val df = readFromRedshift(redshiftSource)
    if (df.rdd.isEmpty) None else Some(df.select(selectedCols.map(x => col(x)): _*))
  }

  private def readFromRedshift[T](redshiftSource: String, options: List[(String, String)])(implicit encoder: Encoder[T]): DataFrame = {
    import session.implicits._
    val prop = secretManager.getRedshiftUrlProp
    readWithProps(
      redshiftSource,
      session,
      optionalSchema = Some(setNullableStateForAllStringColumns(Seq.empty[T].toDF(), nullable = true)),
      extraProps = List(prop) ++ options
    ).getOrElse(Seq.empty[T].toDF())
  }

  private def readFromFabric[T](redshiftSource: String, options: List[(String, String)])(implicit encoder: Encoder[T]): DataFrame = {
    import session.implicits._
    val prop = secretManager.getFabricUrlProp
    val token = secretManager.getFabricToken
    readWithProps(
      redshiftSource,
      session,
//      optionalSchema = Some(setNullableStateForAllStringColumns(Seq.empty[T].toDF(), nullable = true)),
      extraProps = List(prop) ++ options ++ List(token)
    ).getOrElse(Seq.empty[T].toDF())
  }

  protected def getSourcesFromConfig(name: String): Set[String] = {
    Resources.getResource(name, "source")
      .filter(x => fileFormats.exists(x.name.startsWith))
      .map { x =>
        val str = x.props("path")
        if(str.contains("processing"))
          str.substring(str.lastIndexOf("processing")).stripPrefix("processing/")
        else
          str.substring( str.lastIndexOf("/")).stripPrefix("/")
      }
  }

  def listS3Files(name: String, folder: String): Set[String] =
    if (name.isEmpty | !isConnectionEnabled(session)) Set.empty[String]
    else
      getSourcesFromConfig(name)
        .flatMap(AwsUtils.listFiles(_, folder))
        .filter(_.endsWith(parquetFileFormat))

  override def write(sinks: => List[Sink]): Unit = {
    log.info(s"Running app $name")
    log.info("Writing data transformations...")
    session.conf.set("_checkpoint", name.replaceAll("-", "_"))

    val incomingFileNames = listS3Files(name, "incoming")
    val processingFileNames = listS3Files(name, "processing")
    val stagingFileNames = listS3Files(name, "staging")

    Try {
      AwsUtils.moveFiles(incomingFileNames)
      log.info(incomingFileNames.size + " Files moved from /incoming to /processing")
      if (incomingFileNames.nonEmpty || processingFileNames.nonEmpty || stagingFileNames.nonEmpty) {
        saveDataFrame(sinks)
      }
    } match {
      case Success(_) =>
        log.info(s"Batch process completed.")
        val filesToBeDeleted = incomingFileNames.map(_.replace("incoming", "processing")) ++ processingFileNames
        AwsUtils.deleteFiles(filesToBeDeleted)
        log.info(filesToBeDeleted.size + " Files deleted from /processing")
      case Failure(e) =>
        log.error(s"Batch not processed for due to ${e.getMessage}", e)
        throw e
    }
  }

  def saveDataFrame(sinks: => List[Sink]): Unit = {

    val dataSinks = sinks.collect { case s: DataSink => s }.map { s =>
      DataSinkAdapter(session, s.sinkName, s.dataFrame, writerOptions = updateOptions(s.options))
    } ++ sinks.collect { case s: DataSinkAdapter       => s }
    val deltaSinks = sinks.collect { case s: DeltaSink => s } ++ sinks.collect { case s: NewDeltaSink => s }
    val deltaWrites = if (Resources.isDeltaEnabled()) dataSinks.filter(_.name.startsWith("delta")) ++ deltaSinks else Nil
    val otherSinks = dataSinks.filterNot(_.name.startsWith("delta"))
    val orderedSinks = deltaWrites ++ otherSinks.sortBy(_.name < "redshift")
    val filteredSinks = if (Resources.executionEnv() == "local") orderedSinks.filter(!_.name.startsWith("redshift")) else orderedSinks

    // println(filteredSinks)
    // println(filteredSinks)
    log.info("Writing data transformations...")
    filteredSinks.foreach   { sink =>
      val resource = getSinkResource(sink).getOrElse(throw SinkNotConfiguredException(s"Sink not configured for $name"))
      sink.write(resource)
    }
  }

  private def updateOptions(options: Map[String, String]): Map[String, String] = {
    val (key, url) = com.alefeducation.util.AWSSecretManager.getFabricUrlProp
    val (key2, token) = com.alefeducation.util.AWSSecretManager.getFabricToken
    options.updated(key, url).updated(key2, token)
  }

  private def getSinkResource(sink: Sink): Option[Resources.Resource] = {
    Resources
      .getResource(name, "sink")
      .find(_.name == sink.name)
  }

  @tailrec
  protected final def setSourceOps(props: List[(String, String)], reader: DataFrameReader): DataFrameReader = props match {
    case Nil                                                 => reader
    case (Format, value) :: xs                               => setSourceOps(xs, reader.format(value))
    case (DbTable, value) :: xs if value.startsWith("query") => setSourceOps(xs, reader.option(DbTable, replaceNewLineSymbol(value)))
    case (Path, value) :: xs                                 => replaceDataIfMergeOptionOn(reader, value, xs)
    case (key, value) :: xs                                  => setSourceOps(xs, reader.option(key, value))
  }

  private def replaceNewLineSymbol(value: String): String = Resources.getString(value).replaceAll("\n", "")

  private def replaceDataIfMergeOptionOn(reader: DataFrameReader, value: String, xs: List[(String, String)]): DataFrameReader = {
    session.conf.getOption("_merge") match {
      case Some(_) => setSourceOps(xs, reader.option(Path, value.replaceAll("/data/", "/incoming/")))
      case _       => setSourceOps(xs, reader.option(Path, value))
    }
  }

}
