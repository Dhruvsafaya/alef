package com.alefeducation.dimensions

import com.alefeducation.bigdata.Sink
import com.alefeducation.bigdata.batch.delta.{Alias, Builder, Delta, DeltaOperation, DeltaUpsert, DeltaSink, DeltaUpdate}
import com.alefeducation.service.{DataSink, SparkBatchService}
import com.alefeducation.util.Constants._
import com.alefeducation.util.DataFrameUtility.{entityTableMap, extractColumnsTobeUpdated, selectLatestRecords}
import com.alefeducation.util.Helpers._
import com.alefeducation.util.{DataFrameUtility, Resources, SparkSessionUtils}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class LearningPathDimension(override implicit val session: SparkSession) extends SparkBatchService {

  import session.implicits._
  import com.alefeducation.util.BatchTransformerUtility._
  import LearningPathDimension.DeltaSinkName
  import LearningPathDimension.Utility

  override val name: String = LearningPathDimensionName

  override def transform(): List[Sink] = {
    val learningPathOptional = readOptional(ParquetLearningPathSource, session)
    val withCompositeIdsDF = learningPathOptional.map(
      _.withColumn(
        "composite_id",
        when($"classId".isNotNull, concat(col("id"), col("schoolId"), col("classId")))
          .otherwise(concat(col("id"), col("schoolId"), col("schoolSubjectId")))
      ).withColumn("uuid", $"composite_id"))

    val createdLearningPathDf = withCompositeIdsDF.filterDataFrame(AdminLearningPath)
    val updatedLearningPathDf = withCompositeIdsDF.filterDataFrame(AdminLearningPathUpdated)
    val deletedLearningPathDf = withCompositeIdsDF.filterDataFrame(AdminLearningPathDeleted)

    val outputCreatedDF =
      createdLearningPathDf.map(_.transformForInsertDim(LearningPathDimensionCols, LearningPathEntity, ids = List("composite_id")).cache())
    val updateCreatedDF = updatedLearningPathDf.map(
      _.transformForUpdateDim(LearningPathDimensionCols, LearningPathEntity, List("composite_id")).cache())
    val deleteCreatedDF =
      deletedLearningPathDf.map(_.transformForDelete(LearningPathDimensionCols, LearningPathEntity, List("composite_id")).cache())

    val parquetSink = learningPathOptional.map(_.toParquetSink(ParquetLearningPathSource))
    val redshiftSinks = buildRedshiftSink(outputCreatedDF, updateCreatedDF, deleteCreatedDF)
    val deltaSinks = buildDeltaSink(outputCreatedDF, updateCreatedDF, deleteCreatedDF)
    (parquetSink ++ redshiftSinks ++ deltaSinks).toList
  }

  def setUpdateOptions(dataFrame: DataFrame, name: String): Map[String, String] = {
    val tableName = entityTableMap.getOrElse(name, name)
    val targetTable = s"dim_$tableName"
    val idCol = s"${name}_id"
    val schema = Resources.redshiftSchema()
    val stagingTable = s"staging_$targetTable"

    val idConditions = s"$stagingTable.$idCol = $targetTable.$idCol"
    val insertCols = dataFrame.columns.mkString(", ")
    val selectCols = dataFrame.columns.mkString(s"$stagingTable.", s", $stagingTable.", "")
    val updateCols = extractColumnsTobeUpdated(dataFrame, stagingTable)
    val updateDimQuery =
      s"""
        update $schema.$targetTable set
        ${name}_updated_time = $stagingTable.${name}_created_time,
        ${name}_dw_updated_time = $stagingTable.${name}_dw_created_time,
        ${name}_deleted_time = $stagingTable.${name}_created_time,
        ${name}_status = 4
        from $schema.$stagingTable
        where $targetTable.${name}_class_id = $stagingTable.${name}_class_id and
        $targetTable.${name}_uuid != $stagingTable.${name}_uuid and
        $stagingTable.${name}_class_id is not null and
        $targetTable.${name}_status = 1;

        update $schema.$targetTable set $updateCols from $schema.$stagingTable
        where $idConditions and
        $stagingTable.${name}_created_time > $targetTable.${name}_created_time and
        $targetTable.${name}_status = 1;

        delete from $schema.$stagingTable using $schema.$targetTable where
        $idConditions and
        $stagingTable.${name}_created_time > $targetTable.${name}_created_time and
        $targetTable.${name}_status = 1;

        insert into $schema.$targetTable ($insertCols)
        (select distinct $selectCols from $schema.$stagingTable);
        DROP table $schema.$stagingTable
     """.stripMargin

    Map("postactions" -> updateDimQuery, "dbtable" -> s"$schema.$stagingTable")
  }

  private def buildRedshiftSink(outputCreatedDF: Option[DataFrame],
                                updateCreatedDF: Option[DataFrame],
                                deleteCreatedDF: Option[DataFrame]) = {
    val redshiftCreatedSink =
      outputCreatedDF.map(_.toRedshiftInsertSink(RedshiftLearningPathSink, AdminLearningPath))

    val redshiftUpdatedSink =
      updateCreatedDF.recordsWithSubjectAndlatestClass.map { df =>
        df.toRedshiftUpdateSink(RedshiftLearningPathSink,
                                AdminLearningPathUpdated,
                                LearningPathEntity,
                                options = setUpdateOptions(df, LearningPathEntity))
      }

    val redshiftDeletedSink: Option[DataSink] =
      deleteCreatedDF.map(_.toRedshiftUpdateSink(RedshiftLearningPathSink, AdminLearningPathDeleted, LearningPathEntity))
    redshiftCreatedSink ++ redshiftUpdatedSink ++ redshiftDeletedSink
  }

  private def buildDeltaSink(outputCreatedDF: Option[DataFrame], updateCreatedDF: Option[DataFrame], deleteCreatedDF: Option[DataFrame]) = {
    import Delta.Transformations._
    import Builder.Transformations
    val deltaCreatedSink: Option[DeltaSink] =
      outputCreatedDF.flatMap(
        _.withEventDateColumn(false)
          .toCreate()
          .map(_.toSink(DeltaSinkName, eventType = AdminLearningPath)))

    val deltaUpdateClassesSink: Option[DeltaSink] = updateCreatedDF.recordsWithSubjectAndlatestClass
      .flatMap { df =>
        df.filter($"${LearningPathEntity}_class_id".isNotNull)
          .withEventDateColumn(false)
          .toDelete(
            matchConditions =
              s"${Alias.Delta}.${LearningPathEntity}_class_id = ${Alias.Events}.${LearningPathEntity}_class_id and " +
                s"${Alias.Delta}.${LearningPathEntity}_uuid != ${Alias.Events}.${LearningPathEntity}_uuid and " +
                s"${Alias.Delta}.${LearningPathEntity}_status = 1")
          .map(_.toSink(DeltaSinkName, LearningPathEntity))
      }

    val upsertMatchConditions = s"${Alias.Delta}.${LearningPathEntity}_id = ${Alias.Events}.${LearningPathEntity}_id" +
      s" AND ${Alias.Delta}.${LearningPathEntity}_status = 1"

    val deltaUpsertSink: Option[DeltaSink] = updateCreatedDF.recordsWithSubjectAndlatestClass
      .flatMap { df =>
        df.withEventDateColumn(false)
          .toUpsert(
            partitionBy = Nil,
            matchConditions = DeltaOperation.buildMatchColumns(upsertMatchConditions, LearningPathEntity),
            columnsToUpdate = DeltaUpdate.buildUpdateColumns(Nil, LearningPathEntity, df),
            columnsToInsert = DeltaUpsert.buildInsertColumns(df),
          )
          .map(_.toSink(DeltaSinkName))
      }

    val deltaDeletedSink: Option[DeltaSink] = deleteCreatedDF
      .flatMap(
        _.withEventDateColumn(false)
          .toDelete()
          .map(_.toSink(DeltaSinkName, LearningPathEntity, AdminLearningPathDeleted)))

    deltaCreatedSink ++ deltaUpdateClassesSink ++ deltaUpsertSink ++ deltaDeletedSink
  }
}

object LearningPathDimension {

  val DeltaSinkName = "delta-learning-path-sink"

  implicit class Utility(df: Option[DataFrame]) {
    def recordsWithSubjectAndlatestClass = {
      df match {
        case Some(x) =>
          val selected = selectLatestRecords(
            x.filter(col(s"${LearningPathEntity}_class_id").isNotNull),
            List(s"${LearningPathEntity}_class_id"),
            s"${LearningPathEntity}_created_time"
          )
          val dfWithSubject = x.filter(col(s"${LearningPathEntity}_class_id").isNull)
          val dfWithSubjectAndLatestClass = selected.unionByName(dfWithSubject)
          if (dfWithSubjectAndLatestClass.isEmpty) None else Some(dfWithSubjectAndLatestClass)
        case _ => None
      }
    }

    def filterDataFrame(eventType: String) = {
      df match {
        case Some(x) =>
          val filtered = x.filter(col("eventType") === eventType)
          if (filtered.isEmpty) None else Some(filtered)
        case _ => None
      }
    }
  }

  def apply(implicit session: SparkSession): LearningPathDimension = new LearningPathDimension

  def main(args: Array[String]): Unit = {
    LearningPathDimension(SparkSessionUtils.getSession(LearningPathDimensionName)).run
  }

}
