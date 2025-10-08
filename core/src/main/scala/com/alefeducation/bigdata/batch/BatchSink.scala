package com.alefeducation.bigdata.batch

import com.alefeducation.bigdata.Sink
import com.alefeducation.io.data.{Delta => DeltaIO}
import com.alefeducation.util.Constants.{Format, Mode, PartitionBy, Path}
import com.alefeducation.util.DataFrameUtility.handleSqlException
import com.alefeducation.util.Resources.{Resource, getSqlExceptionAttempts}
import com.alefeducation.util.{DataFrameUtility, Resources}
import io.delta.tables.DeltaTable
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.{lit, when}
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row, SparkSession}

import scala.annotation.tailrec

trait Operation
trait Write extends Operation
trait Create extends Write
trait Update extends Operation
trait SubjectUpsert extends Operation
trait Upsert extends Operation
trait Delete extends Operation
@deprecated
trait Scd extends Operation
@deprecated
trait Iwh extends Operation
trait Reset extends Operation
trait SCDTypeII extends Operation
trait InsertIfNotExists extends Operation

trait BatchSink extends Sink {

  implicit def spark: SparkSession

  def df: DataFrame

  def write(resource: Resource): Unit

}

case class DataSinkConfig(writerOptions: Map[String, String] = Map())

trait DataSink extends BatchSink with Logging {

  def config: DataSinkConfig = DataSinkConfig()

  override def write(resource: Resources.Resource): Unit = {
    val itr = (resource.props ++ config.writerOptions).iterator
    val props = (resource.props ++ config.writerOptions)
    log.info("Writing batch results...")
    val coalesceValue = Resources.getCoalesceValue()
    val dataFrameToBeSaved =
      if ((name contains "xapi") || (name contains "behavioral") || (name contains "parquet"))
        df.coalesce(coalesceValue)
      else
        df.distinct().coalesce(coalesceValue) // dropping duplicates only for redshift and delta sinks.
    val writer = setWriterOptions(itr, dataFrameToBeSaved.write)

    handleSqlException(writer.save(), getSqlExceptionAttempts())
    props.get("postactions") match {
     case Some(v) => {
       println("performing post actions")
       com.alefeducation.util.AWSSecretManager.getFabricDriverManager.execute(v)
     }
     case None    => println("No post actions found")
   }
  }

  @tailrec
  protected final def setWriterOptions(itr: Iterator[(String, String)], writer: DataFrameWriter[Row]): DataFrameWriter[Row] =
    if (itr.isEmpty) writer
    else {
      val (key, value) = itr.next()
      key match {
        case Format                      => setWriterOptions(itr, writer.format(value))
        case Mode                        => setWriterOptions(itr, writer.mode(value))
        case Path if value contains "abfss" => setWriterOptions(itr, writer.option(key, outputFolderName(value)))
        // case Path                        => setWriterOptions(itr, writer.option(key, value + spark.conf.get("_batchDate")))
        case PartitionBy                 => setWriterOptions(itr, writer.partitionBy(value.split(",").toList: _*))
        case _                           => setWriterOptions(itr, writer.option(key, value))
      }
    }

  //TODO the path should be passed from conf always, not to be altered
  private def outputFolderName(value: String): String = {
    if (value.contains("alef-service-desk-request-created") || value.contains("alef-jira-issue-created"))
      value
    else
      value.replaceAll("/processing/", s"/data/")
  }

}

object DataSink {

  def apply(session: SparkSession, sinkName: String, inputDF: DataFrame, writerOptions: Map[String, String] = Map()): DataSink =
    new DataSink {

      override implicit def spark: SparkSession = session

      override def config: DataSinkConfig = super.config.copy(writerOptions = writerOptions)

      override def name: String = sinkName

      override def input: DataFrame = inputDF

      override def df: DataFrame = inputDF
    }

}

sealed trait DeltaOperation {

  def selectExpression: String

  def updateFields: Map[String, String]
}

sealed trait DeltaUpdate extends Update with DeltaOperation

object DeltaUpdate {

  def apply(select: String, update: Map[String, String]): DeltaUpdate = new DeltaUpdate {

    override def selectExpression: String = select

    override def updateFields: Map[String, String] = update

  }

}

trait DeltaSubjectUpsert extends SubjectUpsert with DeltaOperation

object DeltaSubjectUpsert {

  def apply(select: String, update: Map[String, String]): DeltaSubjectUpsert = new DeltaSubjectUpsert {

    override def selectExpression: String = select

    override def updateFields: Map[String, String] = update

  }

}

case class DeltaSinkConfig(operation: DeltaOperation, columnMappings: Map[String, String], entity: String, uniqueIdColumns: Seq[String])

trait DeltaSink extends BatchSink with Logging {

  def config: DeltaSinkConfig

  override def output: DataFrame = {
    val session = implicitly[SparkSession]
    import org.apache.spark.sql.functions._
    import session.implicits._

    val withPartitionColumn = df.withColumn("eventdate", date_format($"occurredOn", "yyyy-MM-dd"))
    val withStatusColumn = withStatus(withPartitionColumn)
    val lastUpdated = DataFrameUtility.selectLatestUpdated(withStatusColumn, "occurredOn", config.uniqueIdColumns: _*)
    val selected = DataFrameUtility.selectAvailableColumns(lastUpdated, config.columnMappings)
    DataFrameUtility.addTimestampColumns(selected, config.entity)
  }

  override def write(resource: Resources.Resource): Unit = {
    val session = implicitly[SparkSession]

    val tablePath: String = resource.props("path")
    if(!DeltaIO.isTableExists(session, tablePath)){
      DeltaIO.createEmptyTable(output, tablePath, partitionBy = Seq("eventdate"))
    }
    val deltaTable = DeltaIO.getDeltaTable(session, tablePath)

    val handler = config.operation match {
      case _: DeltaUpdate => update _
      case _: DeltaSubjectUpsert => upsert _
      case _              => throw new RuntimeException("Unsupported delta operation")
    }
    handler(deltaTable, output)
  }

  // TODO: taken from com.alefeducation.util.BatchUtility.addStatusColumn which should become part of common
  private def withStatus(df: DataFrame) = {
    val session = implicitly[SparkSession]
    import session.implicits._

    df.withColumn(
      s"${config.entity}_status",
      when($"eventType".contains("Disabled"), lit(Consts.Disabled))
        .otherwise(when($"eventType".contains("Deleted"), lit(Consts.Deleted)).otherwise(Consts.ActiveEnabled))
    )
  }

  private def upsert(deltaTable: DeltaTable, dataFrame: DataFrame): Unit = {

    deltaTable
      .as("delta")
      .merge(dataFrame.as("events"), config.operation.selectExpression)
      .whenMatched
      .updateExpr(config.operation.updateFields)
      .whenNotMatched
      .insertAll()
      .execute()
  }

  private def update(deltaTable: DeltaTable, dataFrame: DataFrame): Unit = {
    deltaTable
      .as("delta")
      .merge(dataFrame.as("events"), config.operation.selectExpression)
      .whenMatched
      .updateExpr(config.operation.updateFields)
      .execute()

  }

}

object DeltaSink {

  def apply(session: SparkSession, sinkName: String, inputDF: DataFrame, sinkConfig: DeltaSinkConfig): DeltaSink = new DeltaSink {

    override implicit def spark: SparkSession = session

    override def config: DeltaSinkConfig = sinkConfig

    override def name: String = sinkName

    override def input: DataFrame = inputDF

    override def df: DataFrame = inputDF

  }

  def getUpdateExpr(column: Seq[String], eventType: Action.Value, entity: String): Map[String, String] = {

    eventType match {
      case Action.CREATE | Action.UPDATE =>
        Map(s"${entity}_updated_time" -> s"events.${entity}_created_time",
            s"${entity}_dw_updated_time" -> s"events.${entity}_dw_created_time") ++ column
          .map(col => {
            col -> s"events.${col}"
          })
          .toMap
      case Action.DELETE =>
        Map(
          s"${entity}_updated_time" -> s"events.${entity}_created_time",
          s"${entity}_dw_updated_time" -> s"events.${entity}_dw_created_time",
          s"${entity}_deleted_time" -> s"events.${entity}_created_time",
          s"${entity}_status" -> "4"
        )
    }

  }

  def getMatchExprSeq(columns: Seq[String], entity: String = ""): Seq[String] = {
    columns.map(x => s"delta.${entity}_${x}  = events.${entity}_${x}") ++
      Seq(s" delta.${entity}_created_time <= events.${entity}_created_time ")
  }

  def getDeltaSink(
      sinkName: String,
      dataFrame: DataFrame,
      entity: String,
      colMapping: Map[String, String],
      eventType: String,
      action: Action.Value,
      uniqueColumnInRawData: Seq[String] = Seq("uuid")
  )(implicit spark: SparkSession): Option[Sink] = {

    import spark.implicits._
    if (!dataFrame.isEmpty && !dataFrame.filter($"eventType" === eventType).isEmpty) {

      val updateColumns = getUpdateColumns(colMapping)

      val deltaOperation = action match {
        case Action.CREATE | Action.UPDATE =>
          DeltaSubjectUpsert(
            select = DeltaSink.getMatchExprSeq(Seq("id"), entity).mkString(" and "),
            update = DeltaSink.getUpdateExpr(updateColumns, action, entity)
          )
        case Action.DELETE =>
          DeltaUpdate(
            select = DeltaSink.getMatchExprSeq(Seq("id"), entity).mkString(" and "),
            update = DeltaSink.getUpdateExpr(updateColumns, action, entity)
          )
      }

      Some(
        DeltaSink(
          spark,
          sinkName,
          dataFrame.filter($"eventType" === eventType),
          DeltaSinkConfig(
            deltaOperation,
            colMapping ++ Map("eventdate" -> "eventdate"),
            entity,
            uniqueColumnInRawData
          )
        ))
    } else None
  }

}
