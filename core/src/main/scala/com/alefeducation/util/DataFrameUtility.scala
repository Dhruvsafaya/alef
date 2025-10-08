package com.alefeducation.util

import com.alefeducation.io.data.RedshiftIO.createMergeQuery
import com.alefeducation.schema.Schema._
import com.alefeducation.util.Constants._
//import com.amazon.redshift.util.RedshiftException
import org.apache.log4j.Logger
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._

import java.sql.SQLException
import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId, ZoneOffset, ZonedDateTime}
import java.util.{Calendar, TimeZone}
import java.util.concurrent.TimeUnit
import scala.collection.mutable

case class DateField(name: String, `type`: DataType)

object DataFrameUtility {
  val logger = Logger.getLogger(DataFrameUtility.getClass)

  val SerialisationExcMsg = "ERROR: 1023"
  val SerialisationExcCode = 1023

  implicit class DataFrameExtraMethods(df: DataFrame) {
    def transformIfNotEmpty(f: DataFrame => DataFrame): DataFrame = {
      if (df.isEmpty) df
      else df.transform(f)
    }

    def flattenDf(flatOps: Map[String, String] = Map.empty, separator: Char = '_'): DataFrame = flatten(df, flatOps, separator)
  }

  val entityTableMap: Map[String, String] = Map(
    "ic" -> "interim_checkpoint",
    "ic_lesson" -> "ic_lesson_association",
    "ic_rule" -> "interim_checkpoint_rules",
    "lo" -> "learning_objective",
    "lo_association" -> "learning_objective_association",
    "curr_subject" -> "curriculum_subject",
    "at" -> "activity_template",
    "role" -> "cx_role",
    "pool" -> "question_pool",
    "pool" -> "question_pool",
    "crma" -> "content_repository_material_association",
    "ccr" -> "course_content_repository_association",
    "cc" -> "course_curriculum_association",
    "cg" -> "course_grade_association",
    "cs" -> "course_subject_association",
    "cata" -> "course_ability_test_association",
    "cacga" -> "course_activity_container_grade_association",
    "caa" -> "course_activity_association",
    "cacd" -> "course_activity_container_domain",
    "actp" -> "academic_calendar_teaching_period",
    "caoa" -> "course_activity_outcome_association",
    "caga" -> "course_activity_grade_association",
    "saya" -> "school_academic_year_association",
    "pacing" -> "pacing_guide",
    "susra" -> "staff_user_school_role_association",
    "aat" -> "adt_attempt_threshold",
    "caraa" -> "course_additional_resource_activity_association",
    "craoa" -> "course_resource_activity_outcome_association",
    "craga" -> "course_resource_activity_grade_association",
    "ttbla" -> "teacher_test_blueprint_lesson_association"
  )

  def selectAvailableColumns(df: DataFrame, cols: Map[String, String]): DataFrame =
    df.select(
      cols.toList
        .filter(a => flatten(df, separator = '.').columns.toList.contains(a._1))
        .map { case (k, v) => col(k).as(v) }: _*)

  def selectAs(df: DataFrame, cols: Map[String, String]): DataFrame = df.select(cols.toList.map { case (k, v) => col(k).as(v) }: _*)

  def preprocessDeltaForFact(srcDf: DataFrame,
                             entity: String,
                             columnMapping: Map[String, String],
                             f: DataFrame => DataFrame = identity): DataFrame = {
    srcDf.transformIfNotEmpty { df =>
      selectAs(f(df), columnMapping)
        .withColumn("eventdate", date_format(col(s"${entity}_created_time"), "yyyy-MM-dd"))
    }
  }

  def preprocessRedshift(entity: String, columnMapping: Map[String, String], f: DataFrame => DataFrame, isFact: Boolean)(
    srcDf: DataFrame): DataFrame = {
    srcDf.transformIfNotEmpty { df =>
      val redshiftDf = selectAs(f(df), columnMapping)
      addTimestampColumns(redshiftDf, entity, isFact = isFact)
    }
  }

  val dateTimeFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneId.of("UTC"))

  def collectAsString(df: DataFrame, col: Column): String = {
    import df.sparkSession.implicits._
    df.select(col).as[String].collect().distinct.mkString("'", "', '", "'")
  }

  def collectAsList(session: SparkSession, df: DataFrame, name: Column): List[String] = {
    import session.implicits._
    df.select(name).as[String].collect().toList.distinct
  }

  def filterPublished(df: DataFrame): DataFrame = filterByCourseStatus(df, "PUBLISHED")

  def filterInReview(df: DataFrame): DataFrame = filterByCourseStatus(df, "IN_REVIEW")

  private def filterByCourseStatus(df: DataFrame, status: String): DataFrame = {
    import df.sparkSession.implicits._
    df.select($"*", explode($"_headers").as("header")).
      filter($"header.key" === "COURSE_STATUS" && regexp_replace($"header.value".cast("string"), "\"", "") === status).
      drop("header")
  }

  def process(session: SparkSession,
              payloadStream: DataFrame,
              schema: StructType,
              eventFilter: List[String],
              dateField: DateField,
              filterColumn: String = "eventType",
              includeHeaders: Boolean = false): DataFrame = {

    import session.implicits._
    val dfColumns = payloadStream.columns.toSet
    val columnNames = getColumnNames(includeHeaders)
    val columns: Seq[Column] = columnNames.filter(dfColumns).map(col)
    val df = payloadStream.select(columns: _*).withColumn("body", from_json($"body", schema))
    val dfWithBaseColumns = selectColumns(session, df, schema)

    val dfWithOccurredOnAdded = if (schema == guardiansDeletedEventSchema) {
      dfWithBaseColumns.withColumn("occurredOn", $"events.occurredOn".getItem(0)) //TODO remove when sorted with app
    } else if (schema == noMloForSkillsFoundSchema) {
      dfWithBaseColumns
        .withColumn("occurredOn",
          when($"occurredOn".isNull, lit(DefaultEpochTime).cast(LongType)).otherwise($"occurredOn")
        ) //TODO add default in case of NoMloForSkillsFound have no occurredOn
    } else dfWithBaseColumns

    val filteredDf =
      if (eventFilter.isEmpty) dfWithOccurredOnAdded else dfWithOccurredOnAdded.filter($"$filterColumn" isin (eventFilter: _*))

    val occurredOn = "occurredOn"
    if (schema == questionMutatedSchema) {
      val filteredDfWithoutNullOccurredON = filteredDf.withColumnRenamed("body", "questionBody").select("*", "questionBody.*")
        .drop("questionBody")
        .filter($"occurredOn".isNotNull)
      val dfWithTS: DataFrame = addOccurredOnColumn(session, dateField, filteredDfWithoutNullOccurredON, occurredOn)
      dfWithTS
    } else {
      val dfWithTS: DataFrame = addOccurredOnColumn(session, dateField, filteredDf, occurredOn)
      dfWithTS
    }
  }


  private def getColumnNames(includeHeaders: Boolean) = {
    if (includeHeaders) Seq("tenantId", "eventType", "body", "loadtime", "_headers") else Seq("tenantId", "eventType", "body", "loadtime")
  }

  def addOccurredOnColumn(session: SparkSession, dateField: DateField, filteredDf: Dataset[Row], occurredOn: String = "occurredOn"): DataFrame = {
    import session.implicits._
    val dfWithTS = dateField.`type` match {
      case LongType =>
        filteredDf
          .withColumn("eventDateDw", from_unixtime($"${dateField.name}" / 1000, "yyyyMMdd").as[Int])
          .withColumn(occurredOn, getUTCDateFrom($"${dateField.name}"))
          .coalesce(MinPartitions)
      case TimestampType =>
        filteredDf
          .withColumn("eventDateDw", date_format($"${dateField.name}", "yyyyMMdd").as[Int])
          .withColumn(occurredOn, regexp_replace($"${dateField.name}", "T", " "))
          .coalesce(MinPartitions)
      case _ => filteredDf
    }
    dfWithTS
  }

  def getUTCDateFrom: UserDefinedFunction =
    udf[String, String](epoch => {
      ZonedDateTime.ofInstant(Instant.ofEpochMilli(epoch.toLong), ZoneId.of(UTCTimeZone)).format(dateTimeFormatter)
    })

  def getUTCDateFromMillisOrSeconds: UserDefinedFunction =
    udf[String, String](epoch => {
      val zonedDateTime =
        if (epoch.length == 10)
          ZonedDateTime.ofInstant(Instant.ofEpochSecond(epoch.toLong), ZoneOffset.UTC)
        else if (epoch.length == 13)
          ZonedDateTime.ofInstant(Instant.ofEpochMilli(epoch.toLong), ZoneOffset.UTC)
        else
          throw new IllegalArgumentException("The time column should be in epoch milliseconds or epoch seconds only")
      zonedDateTime.format(dateTimeFormatter)
    })

  def getStringDateToTimestamp(colName: String): Column =
    to_timestamp(expr(s"substring($colName, 1, 23) || 'Z'"), "yyyy-MM-dd'T'HH:mm:ss.SSSX")


  def currentUTCDate: String = dateTimeFormatter.format(ZonedDateTime.now)

  def selectColumns(session: SparkSession, dataFrame: DataFrame, schema: StructType): DataFrame = {
    import session.implicits._

    if (schema == xapiSchema)
      dataFrame.select(
        $"tenantId",
        $"eventType",
        $"body.id".as("id"),
        $"body.object.id".as("objectId"),
        $"body.object.objectType".as("objectType"),
        $"body.object.definition.type".as("objectDefinitionType"),
        $"body.object.definition.extensions".as("objectDefinitionExtensions"),
        $"body.actor.openid".as("actorOpenId"),
        $"body.actor.objectType".as("actorObjectType"),
        $"body.actor.account".as("actorAccount"),
        $"body.verb.id".as("verbId"),
        $"body.verb.display".as("verbDisplay"),
        $"body.context.extensions".as("contextExtensions"),
        $"body.timestamp".as("timestamp"),
        $"loadtime"
      )
    else if (schema == newXAPISchema)
      dataFrame.select(
        $"tenantId",
        $"eventType",
        $"body.actor.objectType".as("actorObjectType"),
        $"body.actor.account.name".as("actorAccountName"),
        $"body.actor.account.homePage".as("actorAccountHomePage"),
        $"body.verb.id".as("verbId"),
        $"body.verb.display".as("verbDisplay"),
        $"body.object".as("object"),
        $"body.context.contextActivities.parent".as("contextActivityParent"),
        $"body.context.contextActivities.grouping".as("contextActivityGrouping"),
        $"body.context.contextActivities.category".as("contextActivityCategory"),
        $"body.context.extensions".as("contextExtensions"),
        $"body.timestamp".as("timestamp"),
        $"body.result".as("result"),
        $"loadtime"
      )
    else {
      dataFrame
        .withColumnRenamed("body", "envelope")
        .select($"*", $"envelope.*")
        .drop("envelope")
    }

  }

  def setUpsertOptions(targetTableName: String,
                       matchConditions: String,
                       columnsToUpdate: Map[String, String],
                       columnsToInsert: Map[String, String],
                       isStaging: Boolean): Map[String, String] = {
    val schema = if (isStaging) Resources.redshiftStageSchema() else Resources.redshiftSchema()
    val temporaryTable: String = s"$schema.staging_$targetTableName"

    val mergeQuery = createMergeQuery(
      targetTable = s"$schema.$targetTableName",
      sourceTable = temporaryTable,
      matchConditions = matchConditions,
      columnsToUpdate = columnsToUpdate,
      columnsToInsert = columnsToInsert
    )

    val upsertQuery =
      s"""
         |BEGIN TRANSACTION;
         |
         |$mergeQuery
         |
         |DROP TABLE $temporaryTable;
         |
         |END TRANSACTION;
         |""".stripMargin

    logger.info(s"setUpsertOptions upsertQuery: $upsertQuery")
    Map("postactions" -> upsertQuery, "dbtable" -> temporaryTable)
  }

  def setUpdateOptions(dataFrame: DataFrame, name: String, ids: List[String] = Nil, isStaging: Boolean = false): Map[String, String] = {
    val tableName = entityTableMap.getOrElse(name, name)
    val targetTable = s"dim_$tableName" // TODO: This line doesn't support having a different table name and table column name prefix
    val relTargetTable = s"rel_$tableName"
    val idCol = s"${name}_id"
    val statusCol = s"${name}_status"
    val schema = Resources.redshiftSchema()
    val stageSchema = Resources.redshiftStageSchema()
    val stagingTable = s"staging_$targetTable" //TODO: Rename this table prefix to `tmp`

    val idConditions =
      if (ids.isEmpty) s"$stagingTable.$idCol = $targetTable.$idCol"
      else ids.map(col => s"$targetTable.$col = $stagingTable.$col").mkString(" and ")
    val idRelConditions =
      if (ids.isEmpty) s"$stagingTable.$idCol = $relTargetTable.$idCol"
      else ids.map(col => s"$stagingTable.$col = $relTargetTable.$col").mkString(" and ")

    val updateCols = extractColumnsTobeUpdated(dataFrame, stagingTable)
    val updateDimQuery =
      s"""update $schema.$targetTable set $updateCols from $schema.$stagingTable
        where $idConditions and
        $stagingTable.${name}_created_time > $targetTable.${name}_created_time;
        DROP table $schema.$stagingTable
     """.stripMargin

    val updateRelQuery =
      s"""update $schema.$targetTable set $updateCols from $schema.$stagingTable
        where  $idConditions and
        $targetTable.$statusCol = 1 and
        $stagingTable.${name}_created_time > $targetTable.${name}_created_time;
        update $stageSchema.$relTargetTable set $updateCols from $schema.$stagingTable
        where  $idRelConditions and
        $relTargetTable.$statusCol = 1 and
        $stagingTable.${name}_created_time > $relTargetTable.${name}_created_time;
        DROP table $schema.$stagingTable
     """.stripMargin
    val updateQuery = if (name == "instructional_plan" || isStaging) updateRelQuery else updateDimQuery

    Map("postactions" -> updateQuery, "dbtable" -> s"$schema.$stagingTable")
  }

  def extractColumnsTobeUpdated(columns: Array[String], stagingTable: String): String = {
    columns
      .filterNot(c => c.contains("create"))
      .map(column => {
        if (column.contains("updated_time")) s"$column = $stagingTable.${column.replace("updated_time", "created_time")}"
        else s"$column = $stagingTable.$column"
      })
      .mkString(", ")
  }

  def extractColumnsTobeUpdated(dataFrame: DataFrame, stagingTable: String): String = {
    extractColumnsTobeUpdated(dataFrame.columns, stagingTable)
  }

  def getEarliestRowsInRedshift(schema: String,
                                stagingTable: String,
                                name: String,
                                uniqueKeys: List[String]) = {
    val entityPrefix = getEntityPrefix(name)
    val uniqueKeysString = uniqueKeys.mkString(", ")
    val uniqueKeysConcatStatement = uniqueKeys.reduceLeft((acc, item) => s"concat($acc, $item)")
    val getEarliestQuery =
      s"""select * from $schema.$stagingTable where concat($uniqueKeysConcatStatement, cast(${entityPrefix}created_time as varchar)) in
         |(select id
         |from (select concat($uniqueKeysConcatStatement, cast(${entityPrefix}created_time as varchar)) id,
         |dense_rank()
         |over (partition by
         |${uniqueKeysString}
         |order by ${entityPrefix}created_time) rnk
         |from $schema.$stagingTable) a
         |where a.rnk = 1);""".stripMargin
        .trim.replaceAll(" +", " ")
        .replaceAll("\\R", " ")
    logger.info(s"earliest rows query : $getEarliestQuery")
    getEarliestQuery
  }

  def getOptionsForSCDTypeII(df: DataFrame,
                             entity: String,
                             targetTable: String,
                             ids: List[String],
                             activeStatus: Int = ActiveState,
                             inactiveStatus: Int = HistoryState
                   ): Map[String, String] = {
    val schema = Resources.redshiftSchema()
    val stagingTable = s"staging_$targetTable"
    val earliestDataQuery = getEarliestRowsInRedshift(schema, stagingTable, entity, ids)
    val entityPrefix = getEntityPrefix(entity)
    val idConditions = mkIdConditions(targetTable, ids)

    val insertCols = df.columns.mkString(", ")

    val updateQuery =
      s"""
         |begin transaction;
         |CREATE TEMPORARY TABLE earliest as $earliestDataQuery;
         |update $schema.$targetTable set ${entityPrefix}status = '$inactiveStatus' from earliest where $idConditions and
         |  $targetTable.${entityPrefix}status = '$activeStatus';
         |update $schema.$targetTable set ${entityPrefix}active_until = earliest.${entityPrefix}created_time from earliest where $idConditions and
         |  $targetTable.${entityPrefix}active_until is null;
         |insert into $schema.$targetTable ($insertCols) select $insertCols from $schema.$stagingTable;
         |drop table $schema.$stagingTable;
         |end transaction""".stripMargin

    logger.info(s"getOptionsForSCDTypeII updateQuery: $updateQuery")
    Map("postactions" -> updateQuery, "dbtable" -> s"$schema.$stagingTable")
  }

  def getOptionsForStagingSCDTypeII(df: DataFrame,
                                    entity: String,
                                    relTargetTable: String,
                                    targetTable: String,
                                    ids: List[String],
                                    activeStatus: Int = ActiveState,
                                    inactiveStatus: Int = HistoryState
                             ): Map[String, String] = {

    val relSchema = Resources.redshiftStageSchema()
    val schema = Resources.redshiftSchema()
    val stagingTable = s"staging_$targetTable"
    val entityPrefix = getEntityPrefix(entity)
    val earliestDataQuery = getEarliestRowsInRedshift(schema, stagingTable, entity, ids)

    val relIdConditions = mkIdConditions(relTargetTable, ids)
    val dimIdConditions = mkIdConditions(targetTable, ids)
    val insertCols = df.columns.mkString(", ")
    val insertConditions = mkIdConditionsForInsert(relTargetTable, stagingTable, ids)

    val updateQuery =
      s"""
         |begin transaction;
         |CREATE TEMPORARY TABLE earliest as $earliestDataQuery
         |
         |update $relSchema.$relTargetTable set ${entityPrefix}status = '$inactiveStatus' from earliest where $relIdConditions and
         |  $relTargetTable.${entityPrefix}status = '$activeStatus' and
         |  $relTargetTable.${entityPrefix}created_time < earliest.${entityPrefix}created_time;
         |update $relSchema.$relTargetTable set ${entityPrefix}active_until = earliest.${entityPrefix}created_time from earliest where $relIdConditions and
         |  $relTargetTable.${entityPrefix}active_until is null and
         |  $relTargetTable.${entityPrefix}created_time < earliest.${entityPrefix}created_time;
         |
         |update $schema.$targetTable set ${entityPrefix}status = '$inactiveStatus' from earliest where $dimIdConditions and
         |  $targetTable.${entityPrefix}status = '$activeStatus' and
         |  $targetTable.${entityPrefix}created_time < earliest.${entityPrefix}created_time;
         |update $schema.$targetTable set ${entityPrefix}active_until = earliest.${entityPrefix}created_time from earliest where $dimIdConditions and
         |  $targetTable.${entityPrefix}active_until is null and
         |  $targetTable.${entityPrefix}created_time < earliest.${entityPrefix}created_time;
         |
         |insert into $relSchema.$relTargetTable ($insertCols) select $insertCols from $schema.$stagingTable
         |  where not exists (select 1 from $relSchema.$relTargetTable where $insertConditions and
         |    $relTargetTable.${entityPrefix}status = '$activeStatus' and
         |      $relTargetTable.${entityPrefix}created_time = $stagingTable.${entityPrefix}created_time);
         |
         |drop table $schema.$stagingTable;
         |end transaction""".stripMargin

    logger.info(s"getOptionsForStagingSCDTypeII updateQuery=: $updateQuery")
    Map("postactions" -> updateQuery, "dbtable" -> s"$schema.$stagingTable")
  }

  def mkIdConditions(tableName: String, ids: List[String]): String = {
    ids.map(col => s"$tableName.$col = earliest.$col").mkString(" and ")
  }

  def mkIdConditionsForInsert(tableName: String, stagingTableName: String, ids: List[String]): String = {
    ids.map(col => s"$tableName.$col = $stagingTableName.$col").mkString(" and ")
  }

  def getOptionsForInsertIfNotExists(dataFrame: DataFrame,
                                     tableName: String,
                                     uniqueIdsStatement: String,
                                     isStaging: Boolean = false,
                                     filterNot: List[String] = List.empty
                                    ): Map[String, String] = {
    val schema = if (isStaging) Resources.redshiftStageSchema() else Resources.redshiftSchema()
    val stagingTable = s"staging_${tableName}_tmp"
    val insertCols = dataFrame.columns.filterNot(filterNot.contains).mkString(", ")

    val insert =
      s"""
         |INSERT INTO $schema.$tableName ($insertCols) select $insertCols from $schema.$stagingTable t
         |     WHERE NOT EXISTS (SELECT 1 FROM $schema.$tableName WHERE $uniqueIdsStatement);
         |DROP table $schema.$stagingTable;""".stripMargin
    Map("dbtable" -> s"$schema.$stagingTable", "postactions" -> insert)
  }


  def setInsertWithHistoryOptions(df: DataFrame,
                                  ids: List[String],
                                  name: String,
                                  inactiveStatus: Int = Detached,
                                  isStagingTable: Boolean = false): Map[String, String] = {
    val tableName = entityTableMap.getOrElse(name, name)
    val targetTable = s"dim_$tableName"
    val schema = Resources.redshiftSchema()
    val stagingTable = s"staging_$targetTable"
    val earliestDataQuery = getEarliestRowsInRedshift(schema, stagingTable, name, ids)

    if (isStagingTable) {
      setInsertWithHistoryForStaging(df, ids, name, inactiveStatus, earliestDataQuery)
    } else {
      val idConditions = ids.map(col => s"$targetTable.$col = earliest.$col").mkString(" and ")

      val updateCols =
        s"${name}_status = '$inactiveStatus', " +
          s"${name}_updated_time = earliest.${name}_created_time, " +
          s"${name}_dw_updated_time = earliest.${name}_dw_created_time"

      val insertCols = df.columns.mkString(", ")

      val updateQuery =
        s"""
           |begin transaction;
           |CREATE TEMPORARY TABLE earliest as $earliestDataQuery;
           |update $schema.$targetTable set $updateCols from earliest where $idConditions and
           |  $targetTable.${name}_status <> '$inactiveStatus';
           |insert into $schema.$targetTable ($insertCols) select $insertCols from $schema.$stagingTable;
           |drop table $schema.$stagingTable;
           |end transaction""".stripMargin

      logger.info(s"setInsertWithHistory updateQuery: $updateQuery")
      Map("postactions" -> updateQuery, "dbtable" -> s"$schema.$stagingTable")
    }
  }

  def setInsertWithHistoryForStaging(df: DataFrame,
                                     ids: List[String],
                                     name: String,
                                     inactiveStatus: Int = Detached,
                                     earliestDataQuery: String): Map[String, String] = {
    val tableName = entityTableMap.getOrElse(name, name)
    val (relTargetTable, relSchema) = (s"rel_$tableName", Resources.redshiftStageSchema())
    val (targetTable, schema) = (s"dim_$tableName", Resources.redshiftSchema())

    def isUser(data: String): Boolean = List("user", "teacher", "student", "guardian").contains(data)

    def isDwIdMapping(col: String): Boolean = col.equals("content_repository")


    val stagingTable = s"staging_$targetTable"

    val updateCols =
      s"${name}_status = '$inactiveStatus', ${name}_updated_time = earliest.${name}_created_time, " +
        s"${name}_dw_updated_time = earliest.${name}_dw_created_time"

    val idConditions = ids.map(col => s"$relTargetTable.$col = earliest.$col").mkString(" and ")
    val idConditionsDim = ids
      .map {
        case col if col.endsWith("uuid") =>
          val colPrefix = col.stripSuffix("_uuid")
          val tableName = entityTableMap.getOrElse(colPrefix, colPrefix)
          if (isUser(tableName)) s"$targetTable.${name}_${tableName}_dw_id = rel_user.user_dw_id"
          else if (isDwIdMapping(colPrefix)) s"$targetTable.${name}_dw_id = rel_dw_id_mappings.dw_id"
          else s"$targetTable.${name}_${colPrefix}_dw_id = dim_$tableName.${colPrefix}_dw_id"
        case col => s"$targetTable.$col = earliest.$col"
      }
      .mkString(" and ")

    val join = ids
      .filter(_.endsWith("uuid"))
      .map { col =>
        val colPrefix = col.stripSuffix("_uuid")
        val tableName = entityTableMap.getOrElse(colPrefix, colPrefix)
        if (isUser(colPrefix))
          s"$relSchema.rel_user on rel_user.user_id = $col"
        else if (isDwIdMapping(colPrefix))
          s"$relSchema.rel_dw_id_mappings on rel_dw_id_mappings.id = $col"
        else s"$schema.dim_${tableName} on dim_$tableName.${colPrefix}_id = $col"
      }
      .mkString(" join ")

    val insertCols = df.columns.mkString(", ")
    val insertConditions = mkIdConditionsForInsert(relTargetTable, stagingTable, ids)

    val updateQuery =
      s"""
         |begin transaction;
         |CREATE TEMPORARY TABLE earliest as $earliestDataQuery;
         |
         |UPDATE $relSchema.$relTargetTable
         |SET $updateCols
         |FROM earliest
         |WHERE $idConditions AND
         | $relTargetTable.${name}_status <> '$inactiveStatus' AND
         | $relTargetTable.${name}_created_time < earliest.${name}_created_time;
         |
         |UPDATE $schema.$targetTable
         |SET $updateCols
         |FROM earliest ${if (join.nonEmpty) s"join $join" else ""}
         |WHERE $idConditionsDim AND
         | $targetTable.${name}_status <> '$inactiveStatus' AND
         | $targetTable.${name}_created_time < earliest.${name}_created_time;
         |
         |insert into $relSchema.$relTargetTable ($insertCols) select $insertCols from $schema.$stagingTable
         |  where not exists (select 1 from $relSchema.$relTargetTable where $insertConditions and
         |    $relTargetTable.${name}_status = '$ActiveState' and
         |      $relTargetTable.${name}_created_time = $stagingTable.${name}_created_time);
         |
         |drop table $schema.$stagingTable;
         |end transaction""".stripMargin

    logger.info(s"setInsertWithHistoryForStaging updateQuery: $updateQuery")
    Map("postactions" -> updateQuery, "dbtable" -> s"$schema.$stagingTable")
  }

  def setScdTypeIIPostAction(dataFrame: DataFrame, name: String): Map[String, String] = {
    val idCol = s"${name}_id"
    val relTargetTable = s"rel_$name"
    val dimTargetTable = s"dim_$name"
    val tempTable = s"temp_rel_$name"

    val relSchema = Resources.redshiftStageSchema()
    val dimSchema = Resources.redshiftSchema()

    val createdTimeCol = s"${name}_created_time"
    val dwCreatedTimeCol = s"${name}_dw_created_time"

    val insertCols = dataFrame.columns.mkString(", ")

    val updateActiveUntil =
      s"""
        begin transaction;
        update $relSchema.$relTargetTable
        set
        ${name}_status = 2,
        ${name}_active_until = temp.${name}_created_time
        from $relSchema.$relTargetTable target
                 join $relSchema.$tempTable temp
        on target.$idCol = temp.$idCol
        where temp.${name}_active_until is null
          and target.${name}_active_until is null
          and target.${name}_status = 1
          and target.$createdTimeCol <= temp.$createdTimeCol
          and target.$dwCreatedTimeCol < temp.$dwCreatedTimeCol;

        update $dimSchema.$dimTargetTable
        set
        ${name}_status = 2,
        ${name}_active_until = temp.${name}_created_time
        from $dimSchema.$dimTargetTable target join
             $relSchema.$tempTable temp
        on target.$idCol = temp.$idCol
        where temp.${name}_active_until is null
          and target.${name}_active_until is null
          and target.${name}_status = 1
          and target.$createdTimeCol <= temp.$createdTimeCol
          and target.$dwCreatedTimeCol < temp.$dwCreatedTimeCol;

       insert into $relSchema.$relTargetTable ($insertCols) select $insertCols from $relSchema.$tempTable;
       drop table $relSchema.$tempTable;

       end transaction
       """.stripMargin

    Map("dbtable" -> s"$relSchema.$tempTable", "postactions" -> updateActiveUntil)
  }

  @deprecated
  def selectLatestUpdatedRecords(dataFrame: DataFrame,
                                 eventType: String,
                                 ids: List[String] = List("uuid"),
                                 eventDateColumn: String = "occurredOn"): Dataset[Row] = {
    val partition = Window.partitionBy(ids.map(col): _*).orderBy(col(eventDateColumn).desc)
    dataFrame
      .filter(col("eventType") === eventType)
      .withColumn("rank", dense_rank().over(partition))
      .where("rank == 1")
      .drop("rank")
  }

  @deprecated
  def selectLatestRecords(dataFrame: DataFrame, ids: List[String] = List("uuid"), eventDateColumn: String = "occurredOn"): Dataset[Row] = {
    val partition = Window.partitionBy(ids.map(col): _*).orderBy(col(eventDateColumn).desc)
    dataFrame
      .withColumn("rank", dense_rank().over(partition))
      .where("rank == 1")
      .drop("rank")
  }

  def selectLatestRecordsByRowNumber(dataFrame: DataFrame,
                                     ids: List[String] = List("uuid"),
                                     eventDateColumn: String = "occurredOn"): Dataset[Row] = {
    selectLatestRecordsByRowNumberOrderByDateCols(dataFrame, ids, List(eventDateColumn))
  }

  def selectLatestRecordsByRowNumberOrderByDateCols(dataFrame: DataFrame,
                                                    ids: List[String] = List("uuid"),
                                                    eventDateCols: List[String] = List("occurredOn")): Dataset[Row] = {
    selectOneByRowNumberOrderByDateCols(dataFrame, ids, eventDateCols, asc = false)
  }

  def selectOneByRowNumberOrderByDateCols(dataFrame: DataFrame,
                                          ids: List[String] = List("uuid"),
                                          eventDateCols: List[String] = List("occurredOn"),
                                          asc: Boolean = false
                                         ): Dataset[Row] = {

    val cond: Column => Column = if (asc) _.asc else _.desc
    val partition = Window.partitionBy(ids.map(col): _*).orderBy(eventDateCols.map(col).map(cond): _*)
    dataFrame
      .withColumn("rank", row_number().over(partition))
      .where("rank == 1")
      .drop("rank")
  }

  @deprecated
  def selectLatestUpdatedWithoutEventFilter(dataFrame: DataFrame,
                                            idColumn: String = "uuid",
                                            eventDateColumn: String = "occurredOn"): Dataset[Row] = {
    if (dataFrame.isEmpty) dataFrame
    else {
      val partition = Window.partitionBy(col(idColumn)).orderBy(col(eventDateColumn).desc)
      dataFrame.withColumn("rank", dense_rank().over(partition)).where("rank == 1").drop("rank")
    }
  }

  @deprecated
  def selectLatestUpdated(dataFrame: DataFrame, eventDateColumn: String, idColumn: String*): DataFrame = {
    selectRecordsByRank(dataFrame, eventDateColumn, "rank == 1", idColumn: _*)
  }

  private def selectRecordsByRank(dataFrame: DataFrame, eventDateColumn: String, rank: String, idColumn: String*) = {
    if (dataFrame.isEmpty) dataFrame
    else {
      val partition = Window.partitionBy(idColumn.map(col): _*).orderBy(col(eventDateColumn).desc)
      dataFrame.withColumn("rank", dense_rank().over(partition)).where(rank).drop("rank")
    }
  }

  def isEmpty(dataFrame: DataFrame): Boolean = dataFrame.isEmpty

  def containsAny(s: Seq[String]): UserDefinedFunction =
    functions.udf((c: mutable.WrappedArray[String]) => c.toList.intersect(s).nonEmpty)

  private def flattenSchema(schema: StructType, prefix: String = null): Array[Column] =
    schema.fields.flatMap(f => {
      val colName = if (prefix == null) f.name else prefix + "." + f.name

      f.dataType match {
        case st: StructType => flattenSchema(st, colName)
        case _ => Array(col(colName))
      }
    })

  def flatten(dataframe: DataFrame, flatOps: Map[String, String] = Map.empty, separator: Char = '_'): DataFrame = {

    def getName(str: String, itr: Iterator[(String, String)]): String = {
      if (itr.isEmpty) str
      else {
        val (oldStr, newStr) = itr.next()
        getName(str.replace(oldStr, newStr), itr)
      }
    }.replace("jsontostructs(value).", "").replace('.', separator)

    dataframe.select(flattenSchema(dataframe.schema).map(name => col(name.toString).as(getName(name.toString, flatOps.iterator))): _*)
  }
  
  def getEntityPrefix(entityName: String) = if (entityName.equals("")) "" else s"${entityName}_"

  def addCreatedTimestampColumns(df: DataFrame,
                                 entity: String = "",
                                 eventTimeColumn: String = "occurredOn",
                                 now: Option[String] = None): Dataset[Row] = {

    val entityPrefix =  getEntityPrefix(entity)
    df.withColumn(s"${entityPrefix}created_time", to_utc_timestamp(col(s"$eventTimeColumn"), UTCTimeZone))
      .withColumn(s"${entityPrefix}dw_created_time",
        to_utc_timestamp(lit(now.getOrElse(currentUTCDate)), UTCTimeZone))
      .drop(eventTimeColumn)
  }

  def addTimestampColumns(df: DataFrame,
                          entity: String = "",
                          isFact: Boolean = false,
                          eventTimeColumn: String = "occurredOn",
                          now: Option[String] = None): Dataset[Row] = {
    val entityPrefix =  getEntityPrefix(entity)
    val data = addCreatedTimestampColumns(df, entity, eventTimeColumn, now)
    if (isFact) data
    else
      addUpdatedTimestampColumns(entityPrefix)(data)
  }

  def addUpdatedTimestampColumns(entityPrefix: String)(data: DataFrame): DataFrame = {
    data
      .select("*")
      .withColumn(s"${entityPrefix}updated_time", lit(null).cast(TimestampType))
      .withColumn(s"${entityPrefix}deleted_time", lit(null).cast(TimestampType))
      .withColumn(s"${entityPrefix}dw_updated_time", lit(null).cast(TimestampType))
  }

  def latestOccurredOnTimeUDF(timeColumns: Column*): Column =
    timeColumns.reduceLeft((col1, col2) => when(col1.gt(col2), col1).otherwise(col2))

  def setNullableStateForAllStringColumns(df: DataFrame, nullable: Boolean) = {
    StructType(df.schema.map {
      case StructField(columnName, StringType, _, metadata) => StructField(columnName, StringType, nullable = nullable, metadata)
      case StructField(columnName, columnType, isColumnNullable, metadata) =>
        StructField(columnName, columnType, isColumnNullable, metadata)
    })
  }
  def addStatusCol(entity: String, orderBy: String, uniqueKey: List[String], inactiveStatus: Int = Detached)(df: DataFrame): DataFrame = {
    val w = makeWindowSpec(uniqueKey, orderBy)
    df.withColumn(s"${entity}_status", when(dense_rank().over(w) === 1, lit(Attached)).otherwise(inactiveStatus))
  }

  def addUpdatedTimeCols(entity: String, orderBy: String, uniqueKey: List[String])(df: DataFrame): DataFrame = {
    val w = makeWindowSpec(uniqueKey, orderBy)
    df.withColumn(s"${entity}_updated_time",
      when(dense_rank().over(w) === 1, lit(null).cast(TimestampType)).otherwise(max(col(orderBy)).over(w).cast(TimestampType)))
      .withColumn(s"${entity}_dw_updated_time", lit(null).cast(TimestampType))
  }

  def addActiveUntilCol(entity: String, uniqueKey: List[String], orderBy: String)(df: DataFrame): DataFrame = {
    val w = makeWindowSpec(uniqueKey, orderBy)
    val entityPrefix =  getEntityPrefix(entity)
    df.withColumn(s"${entityPrefix}active_until",
        when(dense_rank().over(w) === 1, lit(null).cast(TimestampType)).otherwise(max(col(orderBy)).over(w).cast(TimestampType)))
  }

  @deprecated("user addHistoryStatusForSCDTypeII")
  def addHistoryStatus(entity: String,
                       uniqueKey: List[String],
                       orderBy: String,
                       activeStatus: Int = ActiveState,
                       historyStatus: Int = HistoryState
                      )(df: DataFrame): DataFrame = {
    val w = makeWindowSpec(uniqueKey, orderBy)
    df.withColumn("rank", row_number().over(w))
      .withColumn(s"${entity}_status", when(col("rank") > 1 && col(s"${entity}_status") === activeStatus, lit(historyStatus))
        .otherwise(col(s"${entity}_status")))
      .drop("rank")
  }

  def addHistoryStatusForSCDTypeII(entity: String,
                                   uniqueKey: List[String],
                                   orderBy: String,
                                   deleteEvent: String = "",
                                   activeStatus: Int = ActiveState,
                                   historyStatus: Int = HistoryState,
                                   deleteStatus: Int = DeletedState
                                  )(df: DataFrame): DataFrame = {
    val entityPrefix =  getEntityPrefix(entity)
    val w = makeWindowSpec(uniqueKey, orderBy)
    df.withColumn(s"${entityPrefix}status",
      when(dense_rank().over(w) === 1 && col("eventType") === deleteEvent, lit(deleteStatus))
        .when(dense_rank().over(w) === 1 && col("eventType") =!= deleteEvent, lit(activeStatus))
        .otherwise(historyStatus))
  }

  def addUpdatedTimeColsForAssociation(entity: String, orderBy: String, uniqueKey: List[String])(df: DataFrame): DataFrame = {
    val w = makeWindowSpec(uniqueKey, orderBy)
    df.withColumn(s"${entity}_updated_time", getUtcTimeStamp(lag(orderBy, 1).over(w)))
      .withColumn(s"${entity}_dw_updated_time", lit(null).cast(TimestampType))
  }

  def makeWindowSpec(uniqueKey: List[String], orderBy: String): WindowSpec = {
    Window.partitionBy(uniqueKey.map(col): _*).orderBy(desc(orderBy))
  }

  def getUtcTimeStamp(column: Column): Column = to_utc_timestamp(column, Calendar.getInstance().getTimeZone.getID)

  def resetOptions(dataFrame: DataFrame, name: String, colPrefix: String): Map[String, String] = {
    import dataFrame.sparkSession.implicits._

    val entityTableMap = Map("assignment_instance" -> "assignment_instance_student",
      "ais_instance" -> "assignment_instance")

    val tableName = entityTableMap.getOrElse(name, name)
    val (relTargetTable, relSchema) = (s"rel_$tableName", Resources.redshiftStageSchema())
    val (targetTable, schema) = (s"dim_$tableName", Resources.redshiftSchema())
    val idConditionsForRel = s"${colPrefix}_id in (${collectAsString(dataFrame, $"${colPrefix}_id")})"

    val dimTableName = entityTableMap.getOrElse(colPrefix, colPrefix)
    val idConditionsDim = s"${colPrefix}_dw_id in (select ${dimTableName}_dw_id from $schema.dim_$dimTableName where ${dimTableName}_id in (${collectAsString(dataFrame, $"${colPrefix}_id")}))"

    val deleteQuery =
      s"""
         |begin transaction;
         |delete from $relSchema.$relTargetTable where $idConditionsForRel;
         |
         |delete from $schema.$targetTable
         |  where $idConditionsDim;
         |end transaction""".stripMargin

    Map("preactions" -> deleteQuery, "dbtable" -> s"$relSchema.$relTargetTable")
  }

  def handleSqlException(fn: => Unit, attempts: Int): Unit = {
    try {
      fn
    } catch {
      case e: SQLException =>
        if (isSerialisationExc(e) || e.getErrorCode == SerialisationExcCode) {
          handleExc(fn, attempts, e)
        } else {
          throw e
        }
      case e: SQLException =>
        val cause = e.getCause
        if (isSerialisationExc(cause) || e.getErrorCode == SerialisationExcCode) {
          handleExc(fn, attempts, e)
        } else {
          throw e
        }
      case e: Throwable =>
        logger.error(e.getMessage, e)
        throw e
    }
  }

  private def isSerialisationExc(e: Throwable): Boolean =
    e != null && e.getMessage != null && e.getMessage.contains(SerialisationExcMsg)

  private def handleExc(fn: => Unit, attempts: Int, e: SQLException): Unit = {
    if (attempts <= 0) {
      throw e
    }
    val nextTryIn = Resources.getSqlExceptionTimeout()
    val remainingAttempts = attempts - 1
    logger.warn(s"Caught Serializable isolation violation exception; making another attempt in $nextTryIn seconds. Remaining attempts: $remainingAttempts.")
    TimeUnit.SECONDS.sleep(nextTryIn)
    handleSqlException(fn, remainingAttempts)
  }
}
