package com.alefeducation.util

import com.alefeducation.schema.lps.ActivityComponentResource
import com.alefeducation.service.DataSink
import com.alefeducation.util.BatchUtility._
import com.alefeducation.util.Constants._
import com.alefeducation.util.DataFrameUtility._
import com.alefeducation.util.Helpers._
import com.alefeducation.util.StringUtilities.lowerCamelToSnakeCase
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.util.Calendar
import scala.reflect.runtime.universe.TypeTag


object BatchTransformerUtility {

  final case class AssociationTypesMapping(colName: String, mapping: Map[String, Int])

  def getEntityPrefix(entityName: String) = if (entityName.equals("")) "" else s"${entityName}_"

  def combineOptionalDfs(df1: Option[DataFrame], df2: Option[DataFrame], dfs: Option[DataFrame]*): Option[DataFrame] = {
    def combineDf(df1: Option[DataFrame], df2: Option[DataFrame]): Option[DataFrame] = {
      (df1, df2) match {
        case (None, Some(df)) => Some(df)
        case (Some(df), None) => Some(df)
        case (Some(xdf), Some(ydf)) => Some(xdf.unionByName(ydf))
        case _ => None
      }
    }

    val dataFrames = df1 :: df2 :: dfs.toList
    dataFrames.reduceLeft(combineDf)
  }

  def getLatestRecords(records: DataFrame, partitionCol: String, sortCol: String): DataFrame = {
    import records.sparkSession.implicits._
    val window = Window.partitionBy(partitionCol).orderBy(col(sortCol).desc)
    records
      .withColumn("row_number", row_number().over(window))
      .where($"row_number" === 1)
      .drop("row_number")
  }

  implicit class OptionalDataFrameTransformer(df: Option[DataFrame]) {
    def joinOptionalStrict(rightDF: Option[DataFrame], joinExprs: Column): Option[DataFrame] =
      joinOptionalStrict(rightDF, Left(joinExprs))

    def joinOptionalStrict(rightDF: Option[DataFrame], usingColumns: Seq[String]): Option[DataFrame] =
      joinOptionalStrict(rightDF, Right(usingColumns))

    private def joinOptionalStrict(rightDF: Option[DataFrame], usingColumns: Either[Column, Seq[String]]): Option[DataFrame] = {
      df match {
        case Some(leftDf) if leftDf.isEmpty => None
        case None => None
        case Some(leftDf) =>
          rightDF match {
            case Some(rightDf) if rightDf.isEmpty => None
            case None => None
            case Some(rightDf) =>
              usingColumns match {
                case Left(c) => Some(leftDf.join(rightDf, c, "inner"))
                case Right(c) => Some(leftDf.join(rightDf, c, "inner"))
              }
          }
      }
    }

    def unionOptionalByNameWithEmptyCheck(otherDF: Option[DataFrame], allowMissingColumns: Boolean = false): Option[DataFrame] = {
      df match {
        case Some(thisDf) => Some(thisDf.unionOptionalByName(otherDF, allowMissingColumns))
        case None => otherDF.flatMap(_.checkEmptyDf)
      }
    }

    def unionOptionalByName(otherDf: Option[DataFrame]): Option[DataFrame] = {
      df match {
        case Some(df1) =>
          otherDf match {
            case Some(df2) => Some(df1.unionByName(df2))
            case None => Some(df1)
          }
        case None => otherDf
      }
    }

    def findUnmapped(otherDF: Option[DataFrame], mappingExprs: Column): Option[DataFrame] =
      findUnmapped(otherDF, Left(mappingExprs))

    def findUnmapped(otherDF: Option[DataFrame], usingColumns: Seq[String]): Option[DataFrame] =
      findUnmapped(otherDF, Right(usingColumns))

    private def findUnmapped(otherDF: Option[DataFrame], usingColumns: Either[Column, Seq[String]]): Option[DataFrame] = {
      usingColumns match {
        case Left(c) => df.map(_.joinOptional(otherDF, c, "leftanti"))
        case Right(c) => df.map(_.joinOptional(otherDF, c, "leftanti"))
      }
    }
  }

  implicit class RawDataFrameToTransformationAndSink(df: DataFrame) {

    def checkEmptyDf: Option[DataFrame] = if (df.isEmpty) None else Some(df)

    def unionOptionalByName(otherDF: Option[DataFrame], allowMissingColumns: Boolean = false): DataFrame = otherDF match {
      case Some(dataFrame) if dataFrame.isEmpty => df
      case Some(dataFrame) => df.unionByName(dataFrame, allowMissingColumns)
      case None => df
    }

    def joinOptional(rightDF: Option[DataFrame], joinExprs: Column, joinType: String): DataFrame =
      joinOptional(rightDF, Left(joinExprs), joinType)

    def joinOptional(rightDF: Option[DataFrame], usingColumns: Seq[String], joinType: String): DataFrame =
      joinOptional(rightDF, Right(usingColumns), joinType)

    private def joinOptional(rightDF: Option[DataFrame], usingColumns: Either[Column, Seq[String]], joinType: String): DataFrame =
      rightDF match {
        case Some(dataFrame) if dataFrame.isEmpty => df
        case Some(dataFrame) =>
          usingColumns match {
            case Left(c) => df.join(dataFrame, c, joinType)
            case Right(s) => df.join(dataFrame, s, joinType)
          }
        case None => df
      }

    def joinOptionalUsingExpr(rightDF: Option[DataFrame], expr: Column, joinType: String): DataFrame = rightDF match {
      case Some(dataFrame) if dataFrame.isEmpty => df
      case Some(dataFrame) => df.join(dataFrame, expr, joinType)
      case None => df
    }

    def exceptOptional(rightDF: Option[DataFrame]): DataFrame = rightDF match {
      case Some(dataFrame) if dataFrame.isEmpty => df
      case Some(dataFrame) => df.except(dataFrame)
      case None => df
    }

    def renameUUIDcolsForDelta(prefix: Option[String] = None): DataFrame = {
      val selCols = df.columns.map(
        k =>
          if (k.contains("uuid")) col(k).alias(prefix.getOrElse("") + k.replace("uuid", "id"))
          else col(k))

      df.select(selCols: _*)
    }

    def appendTimestampColumns(entity: String, isFact: Boolean = false): DataFrame =
      addTimestampColumns(df, entity, isFact = isFact)

    def appendEventDate: DataFrame =
      df.withColumn("eventdate", date_format(col("occurredOn"), "yyyy-MM-dd"))

    def appendFactDateColumns(entity: String, isFact: Boolean = false): DataFrame = {
      val entityPrefix = getEntityPrefix(entity)
      if (isFact) {
        df.appendEventDate
          .withColumn(s"${entityPrefix}date_dw_id", date_format(col("occurredOn"), "yyyyMMdd"))
      } else df
    }

    def appendDwCreatedTimeColumn(columnPrefix: String, timeValue: Option[String] = None): DataFrame = df.withColumn(
      s"${columnPrefix}_dw_created_time",
      to_utc_timestamp(lit(timeValue.getOrElse(currentUTCDate)), Calendar.getInstance().getTimeZone.getID)
    )

    def selectColumnsWithMapping(columnMapping: Map[String, String]): DataFrame = if (columnMapping.isEmpty) df else selectAs(df, columnMapping)

    def selectLatestRecordsByEventType(eventType: String, ids: List[String], dateColumn: String): DataFrame =
      selectLatestUpdatedRecords(df, eventType, ids, dateColumn)

    def selectLatest(ids: List[String], dateCol: String = "occurredOn"): DataFrame = selectLatestRecords(df, ids, dateCol)

    def selectLatestByRowNumber(ids: List[String], dateCol: String = "occurredOn"): DataFrame =
      selectLatestRecordsByRowNumber(df, ids, dateCol)

    def selectOneByRowNumber(ids: List[String], dateCol: String = "occurredOn", asc: Boolean = false): DataFrame =
      selectOneByRowNumberOrderByDateCols(df, ids, List(dateCol), asc)

    def appendStatusColumn(entity: String): DataFrame = addStatusColumn(df, entity)

    def transformForInsertDim(columnMapping: Map[String, String],
                              entity: String,
                              ids: List[String] = Nil,
                              eventDateColumn: String = "occurredOn",
                              isFact: Boolean = false): DataFrame =
      df.transformForInsertOrUpdate(columnMapping, entity, ids, eventDateColumn, isFact)

    @deprecated("For facts status column should not be added, use new method transformForFact")
    def transformForInsertFact(columnMapping: Map[String, String],
                               entity: String,
                               ids: List[String] = Nil,
                               eventDateColumn: String = "occurredOn"): DataFrame =
      df.transformForInsertOrUpdate(columnMapping, entity, ids, eventDateColumn, isFact = true)

    def transformForFact(columnMapping: Map[String, String],
                               entity: String,
                               ids: List[String] = Nil,
                               eventDateColumn: String = "occurredOn"): DataFrame = {
      val filteredDf = if (ids.isEmpty) df.dropDuplicates() else df.selectLatestByRowNumber(ids, eventDateColumn)

      filteredDf
        .selectColumnsWithMapping(columnMapping)
        .appendFactDateColumns(entity, isFact = true)
        .appendTimestampColumns(entity, isFact = true)
    }

    def transformForUpdateDim(columnMapping: Map[String, String],
                              entity: String,
                              ids: List[String] = Nil,
                              eventDateColumn: String = "occurredOn",
                              isFact: Boolean = false): DataFrame =
      df.transformForInsertOrUpdate(columnMapping, entity, ids, eventDateColumn, isFact)

    def transformForInsertOrUpdate(columnMapping: Map[String, String],
                                   entity: String,
                                   ids: List[String] = Nil,
                                   eventDateColumn: String = "occurredOn",
                                   isFact: Boolean = false): DataFrame = {

      val filteredDf = if (ids.isEmpty) df.dropDuplicates() else df.selectLatestByRowNumber(ids, eventDateColumn)

      filteredDf
        .appendStatusColumn(entity)
        .selectColumnsWithMapping(columnMapping)
        .appendFactDateColumns(entity, isFact)
        .appendTimestampColumns(entity, isFact)
    }

    def transformForInsertDwIdMapping(column_prefix: String,
                                      entityType: String,
                                      eventDateColumn: String = "occurredOn"): DataFrame = {

      df.withColumn(s"${column_prefix}_created_time", col(s"$eventDateColumn").cast(TimestampType))
        .appendDwCreatedTimeColumn(column_prefix)
        .withColumn(s"${column_prefix}_type", lit(s"$entityType"))
        .drop(eventDateColumn)
    }

    def transformForDelete(columnMapping: Map[String, String], entity: String, idColumns: List[String] = List("id")): DataFrame =
      df.selectLatestByRowNumber(idColumns)
        .transformForInsertOrUpdate(columnMapping, entity)
        .withColumn(s"${entity}_deleted_time", col(s"${entity}_created_time"))
        .withColumn(s"${entity}_status", lit(4))

    @deprecated
    def transformForSCD(columnMapping: Map[String, String], entity: String, ids: List[String] = Nil): DataFrame =
      df.transformForInsertOrUpdate(columnMapping, entity, ids)
        .withColumn(s"${entity}_active_until", lit(NULL).cast(TimestampType))

    @deprecated("use transformForSCDTypeII")
    def transformForInsertWithHistory(columnMapping: Map[String, String], entity: String): DataFrame =
      addColsForIWH(columnMapping, entity)(df)

    /** This method is new version of `transformForInsertWithHistory`.
     *
     * It should be compared with `transformForInsertWithHistory` and unified into single generic method
     */
    @deprecated("use transformForSCDTypeII")
    def transformForIWH2(columnMapping: Map[String, String],
                         entity: String,
                         associationType: Int,
                         attachedEvents: List[String],
                         detachedEvents: List[String],
                         groupKey: List[String],
                         orderByField: String = "occurredOn",
                         associationAttachedStatusVal: Int = 1,
                         associationDetachedStatusVal: Int = 0,
                         associationUndefinedStatusVal: Int = -1,
                         inactiveStatus: Int = Detached): DataFrame = {
      df.withColumn(s"${entity}_type", lit(associationType))
        .transformAppendAssociationCols(
          entity,
          attachedEvents,
          detachedEvents,
          groupKey,
          orderByField,
          associationAttachedStatusVal,
          associationDetachedStatusVal,
          associationUndefinedStatusVal,
          inactiveStatus
        )
        .transform(addColsForIWH(columnMapping, entity))
    }

    def transformForSCDTypeII(columnMapping: Map[String, String],
                                        entity: String,
                                        uniqueKey: List[String],
                                        orderBy: String = "occurredOn",
                                        deleteEvent: String = "",
                                        activeStatus: Int = ActiveState,
                                        historyStatus: Int = HistoryState,
                                        deleteStatus: Int = DeletedState): DataFrame = {
      val entityPrefix =  getEntityPrefix(entity)
      val dwGeneratedCols = Set(s"${entityPrefix}status", s"${entityPrefix}active_until")
      val dwGeneratedColsMap = dwGeneratedCols.map(a => a -> a)
      df.transform(addHistoryStatusForSCDTypeII(entity, uniqueKey, orderBy, deleteEvent, activeStatus, historyStatus, deleteStatus))
        .transform(addActiveUntilCol(entity, uniqueKey, orderBy))
        .transform(selectAvailableColumns(_, columnMapping ++ dwGeneratedColsMap))
        .transform(addCreatedTimestampColumns(_, entity))
    }

    def associationTypesFunction(associationTypes: Map[String, Int]): UserDefinedFunction =
      udf((key: String) => associationTypes.getOrElse(key, Undefined))

    @deprecated("use transformForSCDTypeII")
    def transformForIWH(columnMapping: Map[String, String],
                        entity: String,
                        atm: AssociationTypesMapping,
                        attachedEvents: List[String],
                        detachedEvents: List[String],
                        groupKey: List[String],
                        orderByField: String = "occurredOn",
                        associationAttachedStatusVal: Int = 1,
                        associationDetachedStatusVal: Int = 0,
                        associationUndefinedStatusVal: Int = -1): DataFrame = {

      df.withColumn(s"${entity}_type", associationTypesFunction(atm.mapping)(col(atm.colName)))
        .transformAppendAssociationCols(
          entity,
          attachedEvents,
          detachedEvents,
          groupKey,
          orderByField,
          associationAttachedStatusVal,
          associationDetachedStatusVal,
          associationUndefinedStatusVal
        )
        .transform(addColsForIWH(columnMapping, entity))
    }

    def transformAppendAssociationCols(entity: String,
                                       attachedEvents: List[String],
                                       detachedEvents: List[String],
                                       groupKey: List[String],
                                       orderByField: String = "occurredOn",
                                       associationAttachedStatusVal: Int = 1,
                                       associationDetachedStatusVal: Int = 0,
                                       associationUndefinedStatusVal: Int = -1,
                                       inactiveStatus: Int = Detached): DataFrame =
      df.transform(addStatusCol(entity, orderByField, groupKey, inactiveStatus))
        .transform(addUpdatedTimeCols(entity, orderByField, groupKey))
        .withColumn(
          s"${entity}_attach_status",
          when(col("eventType").isInCollection(attachedEvents), lit(associationAttachedStatusVal))
            .when(col("eventType").isInCollection(detachedEvents), lit(associationDetachedStatusVal))
            .otherwise(lit(associationUndefinedStatusVal))
        )

    def toParquetSink(sink: String, eventDateName: String = "occurredOn"): DataSink = {
      val eventDateDf = df
        .withColumn("eventdate", date_format(col(eventDateName), "yyyy-MM-dd"))
        .coalesce(1)
      DataSink(sink, eventDateDf, Map("partitionBy" -> "eventdate"))
    }

    def toStagingSink(sink: String)(implicit spark: SparkSession): DataSink = {
      DataSink(sink, df.coalesce(1), eventType = StagingEventType)
    }

    //ToDo consider removing event type, as in any sink it doesn't signify anything, and it is not present in data going to redshift
    def toRedshiftInsertSink(sinkName: String, eventType: String = ""): DataSink =
      DataSink(sinkName, df, eventType = eventType)

    def toRedshiftResetSink(sinkName: String,
                            eventType: String,
                            entity: String,
                            colPrefix: String): DataSink = {
      DataSink(sinkName, df, eventType = eventType, options = resetOptions(df, entity, colPrefix))
    }

    def toRedshiftUpsertSink(sinkName: String,
                             tableName: String,
                             matchConditions: String,
                             columnsToUpdate: Map[String, String],
                             columnsToInsert: Map[String, String],
                             isStaging: Boolean): DataSink = {
      val options = setUpsertOptions(tableName, matchConditions, columnsToUpdate, columnsToInsert, isStaging)
      DataSink(sinkName, df, options)
    }

    def toRedshiftUpdateSink(sinkName: String,
                             eventType: String,
                             entity: String,
                             ids: List[String] = Nil,
                             options: Map[String, String] = Map.empty,
                             isStaging: Boolean = false): DataSink = {
      val updateOptions = if (options.isEmpty) setUpdateOptions(df, entity, ids, isStaging) else options
      DataSink(sinkName, df, eventType = eventType, options = updateOptions)
    }

    def toRedshiftIWHSink(sinkName: String,
                          entity: String,
                          eventType: String = "",
                          ids: List[String] = Nil,
                          inactiveStatus: Int = Detached,
                          isStagingSink: Boolean = false,
                          isActiveUntilVersion: Boolean = false,
                          dimTableName: Option[String] = None,
                          relTableName: Option[String] = None
                         ): DataSink = {
      val options = if (isActiveUntilVersion) {
        if (isStagingSink) getOptionsForStagingSCDTypeII(df, entity, relTableName.getOrElse(s"rel_$entity"), dimTableName.getOrElse(s"dim_$entity"), ids, ActiveState, inactiveStatus)
        else getOptionsForSCDTypeII(df, entity, dimTableName.getOrElse(s"dim_$entity"), ids, ActiveState, inactiveStatus)
      } else {
        setInsertWithHistoryOptions(df, ids, entity, inactiveStatus, isStagingSink)
      }

      DataSink(sinkName, df, eventType = eventType, options = options)
    }

    def toRedshiftSCDTypeIISink(sinkName: String,
                                entity: String,
                                uniqueIds: List[String],
                                dimTableName: String,
                                relTableNameOpt: Option[String] = None
                               ): DataSink = {
      val options = relTableNameOpt
        .map(relTableName => getOptionsForStagingSCDTypeII(df, entity, relTableName, dimTableName, uniqueIds))
        .getOrElse(getOptionsForSCDTypeII(df, entity, dimTableName, uniqueIds))

      DataSink(sinkName, df, options = options)
    }

    def toInsertIfNotExistsSink(sinkName: String, tableName: String, uniqueIdsStatement: String, isStaging: Boolean = false, filterNot: List[String] = List.empty): DataSink = {
      val options = getOptionsForInsertIfNotExists(df, tableName, uniqueIdsStatement, isStaging, filterNot)
      DataSink(sinkName, df, options = options)
    }

    def toRedshiftSCDSink(sinkName: String,
                          eventType: String,
                          entity: String,
                          buildOptions: (DataFrame, String) => Map[String, String]): DataSink =
      DataSink(sinkName, df, eventType = eventType, options = buildOptions(df, entity))

    def genDwId(colName: String, maxId: Long, orderByField: String = "occurredOn"): DataFrame = {
      val schema = StructType(df.schema.fields :+ StructField(colName, LongType, nullable = true))
      val ndf = df.orderBy(col(orderByField)).rdd.zipWithIndex().map {
        case (row, idx) => Row.fromSeq(row.toSeq :+ idx)
      }
      df.sparkSession.createDataFrame(ndf, schema).withColumn(colName, col(colName) + lit(maxId))
    }

    def enrichLearningExperienceColumns(): DataFrame = {

      Map(
        "activityTemplateId" -> lit("NA"),
        "activityType" -> lit("NA"),
        "activityComponentResources" -> typedLit(Seq.empty[ActivityComponentResource]),
        "activityComponentType" -> lit("NA"),
        "abbreviation" -> lit("NA"),
        "exitTicket" -> lit(false),
        "mainComponent" -> lit(false),
        "completionNode" -> lit(false)
      ).filterNot { case (k, _) => df.columns.toList.contains(k) }
        .foldLeft(df)((a, b) => a.withColumn(b._1, b._2))
    }

    @deprecated("replaced by RawDataFrameToTransformationAndSink.toParquetSink")
    def createParquetSinks(source: String, rawDf: Option[DataFrame] = None)(implicit spark: SparkSession): List[DataSink] = {
      if (df.isEmpty) Nil else List(getParquetSink(spark, source, rawDf.getOrElse(df)))
    }

    @deprecated("replaced by RawDataFrameToTransformationAndSink api methods")
    def transformDataFrameWithEventType(
                                         events: List[String],
                                         columnMapping: Map[String, String],
                                         entity: String,
                                         idColumns: List[String] = List("uuid"),
                                         eventDateColumn: String = "occurredOn")(implicit spark: SparkSession): List[TransformedDataWithEvent] = {
      import spark.implicits._
      events.flatMap {
        case eventType if !isEmpty(df) && !isEmpty(df.filter($"eventType" === eventType)) =>
          val fltDF = df.filter($"eventType" === eventType)
          val dfWithStatus = addStatusColumn(fltDF, entity)
          getOperations(eventType) match {
            case Insert =>
              val selectedDf = selectAs(dfWithStatus, columnMapping)
              val dataFrameWithTimeColumns = addTimestampColumns(selectedDf, entity)
              List(TransformedDataWithEvent(dataFrameWithTimeColumns, eventType))
            case Update =>
              val latestUpdated = selectLatestUpdatedRecords(dfWithStatus, eventType, idColumns, eventDateColumn)
              val selectedDf = selectAs(latestUpdated, columnMapping)
              val dataFrameWithTimeColumns = addTimestampColumns(selectedDf, entity)
              List(TransformedDataWithEvent(dataFrameWithTimeColumns, eventType))
            case Delete =>
              val latestUpdated = dfWithStatus.dropDuplicates(idColumns)
              val selectedDf = selectAs(latestUpdated, columnMapping)
              val dataFrameWithTimeColumns = addTimestampColumns(selectedDf, entity)
                .withColumn(s"${entity}_deleted_time", $"${entity}_created_time")
              List(TransformedDataWithEvent(dataFrameWithTimeColumns, eventType))
            case _ => Nil
          }
        case _ => Nil
      }
    }

    @deprecated
    def addNestedColumnIfNotExists(columnName: String, castType: DataType = null): DataFrame = {
      if (df.isEmpty) df
      else {
        val cols = columnName.split('.')
        if (cols.length > 2) {
          throw new IllegalArgumentException("The addColumnIfNotExists method supports only 1 level of nesting (e.g. struct.nestedCol1)")
        }

        if (cols.length == 1) {
          if (!df.columns.contains(columnName) && castType != null) df.withColumn(columnName, lit(null).cast(castType))
          else if (!df.columns.contains(columnName)) df.withColumn(columnName, lit(null))
          else df
        }
        else {
          val structName = cols(0)
          val colName = cols(1)

          val index = df.schema.fieldIndex(structName)
          val nestedColExists = df.schema(index).dataType.asInstanceOf[StructType].fieldNames.contains(colName)
          if (!nestedColExists) {
            if (castType != null) df.withColumn(structName, struct(col(s"${structName}.*"), lit(null).as(colName)).cast(castType))
            else df.withColumn(structName, struct(col(s"${structName}.*"), lit(null).as(colName)))
          }
          else df
        }
      }
    }

    def addColIfNotExists(name: String, tp: DataType = null, value: String = null): DataFrame = {
      if (df.columns.contains(name)) df
      else if (tp == null) df.withColumn(name, lit(null))
      else df.withColumn(name, lit(value).cast(tp))
    }

    def castStructCol[T <: Product : TypeTag](colName: String): DataFrame = {
      val schema = Encoders.product[T].schema
      val nullCol = lit(null).cast(schema)
      val column = df.schema.fields.find(_.name == colName)
      val replacementCol = column.get.dataType match {
        case sbType: StringType => nullCol
        case _ => col(colName)
      }
      df.withColumn(colName, replacementCol)
    }
  }

  @deprecated
  case class TransformedDataWithEvent(dataFrame: DataFrame, event: String)

  @deprecated("replaced by RawDataFrameToTransformationAndSink.createRedshiftSink")
  implicit class TransformationDataFrameToRedshiftSink(dataWithEvent: List[TransformedDataWithEvent]) {
    def createRedshiftSinks(sinkName: String, entity: String): List[DataSink] = {
      dataWithEvent flatMap {
        case TransformedDataWithEvent(dataFrame, eventType) =>
          getOperations(eventType) match {
            case Insert => List(DataSink(sinkName, dataFrame, eventType = eventType))
            case Update =>
              val updateOptions = setUpdateOptions(dataFrame, entity)
              List(DataSink(sinkName, dataFrame, eventType = eventType, options = updateOptions))
            case Delete =>
              val updateOptions =
                setUpdateOptions(dataFrame, entity)
              List(DataSink(sinkName, dataFrame, eventType = eventType, options = updateOptions))
            case _ => Nil
          }
        case _ => Nil
      }
    }
  }

  def addMaterialCols(df: DataFrame): DataFrame = {
    val mIdDf = if (df.columns.contains(materialId)) {
      df.withColumn(materialId, when(col(materialId).isNotNull, col(materialId))
        .otherwise(col("instructionalPlanId")))
    } else {
      df.withColumn(materialId, col("instructionalPlanId"))
    }

    if (df.columns.contains(materialType)) {
      mIdDf.withColumn(materialType, when(col(materialType).isNotNull, col(materialType))
        .otherwise(lit("INSTRUCTIONAL_PLAN")))
    } else {
      mIdDf.withColumn(materialType, lit("INSTRUCTIONAL_PLAN"))
    }
  }

  def sessionStatus(prefix: String)(df: DataFrame): DataFrame =
    df.withColumn(s"${prefix}_session_state", when(col(s"${prefix}_session_state") === "started", lit(1))
      .otherwise(when(col(s"${prefix}_session_state") === "in_progress", lit(2))
        .otherwise(when(col(s"${prefix}_session_state") === "finished", lit(3))))
    )
}
