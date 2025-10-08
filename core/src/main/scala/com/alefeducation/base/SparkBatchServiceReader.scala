package com.alefeducation.base

import com.alefeducation.schema.secretmanager.{JiraCred, ServiceDeskCred}
import com.alefeducation.util.Constants.{DbTable, Format, Path}
import com.alefeducation.util.DataFrameUtility.setNullableStateForAllStringColumns
import com.alefeducation.util.Resources
import com.alefeducation.util.Resources.loadMap
import org.apache.spark.sql._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType
import requests._

import java.sql.Timestamp
import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

trait SparkBatchServiceReader extends ReaderService {
  val name: String
  val session: SparkSession
  val fileFormats: Set[String] = Set("parquet", "csv", "delta", "text")
  val pathNotFoundError = "Path does not exist:"
  val pathNotFoundErrorNew = "[PATH_NOT_FOUND]"
  val notADeltaTableError = "is not a Delta table."

  def readUniqueOptional(config: String,
                         session: SparkSession,
                         optionalSchema: Option[StructType] = None,
                         extraProps: List[(String, String)] = Nil,
                         uniqueColNames: Seq[String]): Option[DataFrame] = {
    val df = read(config, session, optionalSchema, extraProps, uniqueColNames)
    if (df.isEmpty) None else Some(df)
  }

  def readOptional(config: String,
                   session: SparkSession,
                   optionalSchema: Option[StructType] = None,
                   extraProps: List[(String, String)] = Nil): Option[DataFrame] = {
    val df = read(config, session, optionalSchema, extraProps)
    if (df.isEmpty) None else Some(df)
  }

  override def read(config: String,
                    session: SparkSession,
                    optionalSchema: Option[StructType] = None,
                    extraProps: List[(String, String)] = Nil,
                    uniqueColNames: Seq[String] = Nil,
                    withoutDropDuplicates: Boolean = false): DataFrame = {
    val dataFrame = Try {
      val props: List[(String, String)] = loadMap(config).toList ++ extraProps
      optionalSchema
        .map(schema => setSourceOps(props, session.read).schema(schema).load)
        .getOrElse(setSourceOps(props, session.read).load)
    } match {
      case Success(df) =>
        if (!withoutDropDuplicates) {
          if (uniqueColNames != Nil) df.dropDuplicates(uniqueColNames)
          else df.dropDuplicates((df.columns.toSet - "loadtime").toSeq)
        } else {
          df
        }
      case Failure(e: AnalysisException) if e.getMessage.startsWith(pathNotFoundError) || e.getMessage.startsWith(pathNotFoundErrorNew) || e.getMessage().endsWith(notADeltaTableError) =>
        session.emptyDataFrame
      case Failure(e) => throw e
    }
    dataFrame
  }

  /**
   * Accepts the query string, fetches the data from redshift and returns the query result as dataframe
   * @param query The query that is used to fetch data from redshift
   * @param selectedCols list of column to be selected, when all columns are not required
   * @return result as dataframe
   */
  def readFromRedshiftQuery[T](query: String, selectedCols: List[String] = List("*"))(implicit encoder: Encoder[T]): DataFrame = {
    _readFromRedshift("redshift", Map("query" -> query)).select(selectedCols.map(x => col(x)): _*)
  }

  def readFromRedshift[T](redshiftSource: String,
                          selectedCols: List[String] = List("*"),
                          withoutDropDuplicates: Boolean = false
                         )(implicit encoder: Encoder[T]): DataFrame = {
    _readFromRedshift(redshiftSource, withoutDropDuplicates = withoutDropDuplicates).select(selectedCols.map(x => col(x)): _*)
  }

  def readFromRedshiftOptional[T](redshiftSource: String, selectedCols: List[String] = List("*"))(
      implicit encoder: Encoder[T]): Option[DataFrame] = {
    val df = _readFromRedshift(redshiftSource)
    if (df.isEmpty) None else Some(df.select(selectedCols.map(x => col(x)): _*))
  }

  def getStartIdUpdateStatus(tableName: String): Long = {
    val failIfWrongState = featureService.exists(_.isProductMaxIdConsistencyCheckEnabled)
    controlTableService.getStartIdUpdateStatus(tableName, failIfWrongState)
  }

  def getDataOffset(tableName: String): Timestamp = {
    dataOffsetService.getOffset(tableName)
  }

  private def _readFromRedshift[T](redshiftSource: String, options: Map[String, String] = Map.empty[String, String], withoutDropDuplicates: Boolean = false)(implicit encoder: Encoder[T]): DataFrame = {
    import session.implicits._
    val prop = secretManager.getRedshiftUrlProp
    val data = read(
      redshiftSource,
      session,
      optionalSchema = Some(setNullableStateForAllStringColumns(Seq.empty[T].toDF(), nullable = true)),
      extraProps = List(prop) ++ options,
      withoutDropDuplicates = withoutDropDuplicates
    )
    data
  }

  @tailrec
  protected final def setSourceOps(props: List[(String, String)], reader: DataFrameReader): DataFrameReader = props match {
    case Nil                                                 => reader
    case (Format, value) :: xs                               => setSourceOps(xs, reader.format(value))
    case (DbTable, value) :: xs if value.startsWith("query") => setSourceOps(xs, reader.option(DbTable, replaceNewLineSymbol(value)))
    case (Path, value) :: xs                                 => setSourceOps(xs, reader.option(Path, value))
    case (key, value) :: xs                                  => setSourceOps(xs, reader.option(key, value))
  }

  private def replaceNewLineSymbol(value: String): String = Resources.getString(value).replaceAll("\n", "")

  def getHttp(url: String, headers: Map[String, String], params: String): String = {
    val finalUrl = s"$url$params"
    val response = requests.get(finalUrl, headers = headers, connectTimeout = 150000, readTimeout = 150000)
    response.text()
  }

  def postHttp(url: String, headers: Map[String, String], params: Map[String, String]): String = {
    val response = requests.post(url, headers = headers, params = params)
    response.text()
  }

  def getServiceDeskSecret: ServiceDeskCred = secretManager.getServiceDeskSecret

  def getJiraSecret: JiraCred = secretManager.getJiraSecret

  def readJsonStringAsDf(jsonString: List[String]) = {
    import session.implicits._
    session.read.json(jsonString.toDS)
  }
}
