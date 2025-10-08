package com.alefeducation.dimensions.interim_checkpoint

import com.alefeducation.bigdata.Sink
import com.alefeducation.bigdata.batch.delta.{Alias, Delta}
import com.alefeducation.io.data.RedshiftIO.TempTableAlias
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object InterimCheckpointTransformer {
  import InterimCheckpointDimension._

  val redshiftIcIds = List("ic_id")
  val idId = "interimCheckpointId"
  val icIds = List(idId)
  val targetTableName = "dim_interim_checkpoint"

  private val deltaMatchCondition =
    s"""
       |${Alias.Delta}.${InterimCheckpointPrefix}_id = ${Alias.Events}.${InterimCheckpointPrefix}_id
     """.stripMargin

  private val upsertMatchCondition =
    s"$targetTableName.${InterimCheckpointPrefix}_id = $TempTableAlias.${InterimCheckpointPrefix}_id"

  private def columnsToInsert(tempTable: String): Map[String, String] = Map(
      s"${InterimCheckpointPrefix}_id" -> s"$tempTable.${InterimCheckpointPrefix}_id",
      s"${InterimCheckpointPrefix}_title" -> s"$tempTable.${InterimCheckpointPrefix}_title",
      s"${InterimCheckpointPrefix}_language" -> s"$tempTable.${InterimCheckpointPrefix}_language",
      s"${InterimCheckpointPrefix}_status" -> s"$tempTable.${InterimCheckpointPrefix}_status",
      s"${InterimCheckpointPrefix}_material_type" -> s"$tempTable.${InterimCheckpointPrefix}_material_type",
      s"${InterimCheckpointPrefix}_created_time" -> s"$tempTable.${InterimCheckpointPrefix}_created_time",
      s"${InterimCheckpointPrefix}_dw_created_time" -> s"$tempTable.${InterimCheckpointPrefix}_dw_created_time",
      s"${InterimCheckpointPrefix}_updated_time" -> s"$tempTable.${InterimCheckpointPrefix}_updated_time",
      s"${InterimCheckpointPrefix}_dw_updated_time" -> s"$tempTable.${InterimCheckpointPrefix}_dw_updated_time",
    )

  private def columnsToUpdate(tempTable: String): Map[String, String] = Map(
    s"${InterimCheckpointPrefix}_title" -> s"$tempTable.${InterimCheckpointPrefix}_title",
    s"${InterimCheckpointPrefix}_language" -> s"$tempTable.${InterimCheckpointPrefix}_language",
    s"${InterimCheckpointPrefix}_status" -> s"$tempTable.${InterimCheckpointPrefix}_status",
    s"${InterimCheckpointPrefix}_material_type" -> s"$tempTable.${InterimCheckpointPrefix}_material_type",
    s"${InterimCheckpointPrefix}_updated_time" -> s"$tempTable.${InterimCheckpointPrefix}_created_time",
    s"${InterimCheckpointPrefix}_dw_updated_time" -> s"$tempTable.${InterimCheckpointPrefix}_dw_created_time",
  )

  def transformForCreate(createdDF: Option[DataFrame], updatedDF: Option[DataFrame], deletedDF: Option[DataFrame], interimCheckPointDeletedDf: Option[DataFrame])
                        (implicit session: SparkSession): List[Sink] = {
    import Delta.Transformations._
    import com.alefeducation.util.BatchTransformerUtility._

    // Base Interim Checkpoint extracted
    val transformedDf = createdDF.map(addMaterialType)
      .map(_.transformForInsertDim(interimCheckpointCols, InterimCheckpointPrefix, ids = icIds))
    val transformedUpdatedDf = updatedDF.map(addMaterialType)
      .map(_.transformForUpdateDim(interimCheckpointCols, InterimCheckpointPrefix, ids = icIds))
    val transformedDeletedDf = combineOptionalDfs(transformForDelete(deletedDF), transformForDelete(interimCheckPointDeletedDf))

    val deltaCreatedSink =
      transformedDf.flatMap(_.toUpsert(
          matchConditions = deltaMatchCondition,
          partitionBy = Nil,
          columnsToUpdate = columnsToUpdate(Alias.Events),
          columnsToInsert = columnsToInsert(Alias.Events),
        )
        .map(_.toSink(DeltaInterimCheckpointSink))
      )
    val deltaUpdatedSink =
      transformedUpdatedDf.flatMap(_.toUpdate(matchConditions = deltaMatchCondition)
        .map(_.toSink(DeltaInterimCheckpointSink, InterimCheckpointPrefix)))
    val deltaDeletedSink =
      transformedDeletedDf.flatMap(_.toDelete().map(_.toSink(DeltaInterimCheckpointDeleteSink, InterimCheckpointPrefix)))

    val redshiftCreatedSink = transformedDf.map(_.toRedshiftUpsertSink(
      sinkName = RedshiftInterimCheckpointSink,
      tableName = targetTableName,
      matchConditions = upsertMatchCondition,
      columnsToUpdate = columnsToUpdate(TempTableAlias),
      columnsToInsert = columnsToInsert(TempTableAlias),
      isStaging = false
    ))

    val redshiftUpdatedSink = transformedUpdatedDf
      .map(_.toRedshiftUpdateSink(RedshiftInterimCheckpointSink, InterimCheckpointRulesUpdatedEvent, InterimCheckpointPrefix, ids = redshiftIcIds))
    val redshiftDeletedSink = transformedDeletedDf.map(
      _.toRedshiftUpdateSink(RedshiftInterimCheckpointDeletedSink, PublishedCheckpointDeletedEvent, InterimCheckpointPrefix, ids = redshiftIcIds))

    val parquetCreatedSink = createdDF.map(_.toParquetSink(ParquetInterimCheckpointSink))
    val parquetUpdatedSink = updatedDF.map(_.toParquetSink(ParquetInterimCheckpointUpdatedSink))
    val parquetDeletedSink = deletedDF.map(_.toParquetSink(ParquetPublishedCheckpointDeletedSink))
    val parquetInterimCheckPointSink = interimCheckPointDeletedDf.map(_.toParquetSink(ParquetInterimCheckpointDeletedSink))
    (deltaCreatedSink ++ deltaUpdatedSink ++ deltaDeletedSink ++
      redshiftCreatedSink ++ redshiftUpdatedSink ++ redshiftDeletedSink ++
      parquetCreatedSink ++ parquetUpdatedSink ++ parquetDeletedSink ++ parquetInterimCheckPointSink).toList
  }

  private def transformForDelete(deletedDF: Option[DataFrame]): Option[DataFrame] = {
    import com.alefeducation.util.BatchTransformerUtility._
    deletedDF
      .map(_.transformForDelete(interimCheckpointDeleteCols, InterimCheckpointPrefix))
  }

  private def addMaterialType(df: DataFrame): DataFrame = {
    if (df.columns.contains(MaterialType))
      df.withColumn(IcMaterialType, when(col(MaterialType).isNotNull, col(MaterialType)).otherwise(lit(InstructionPlan)))
    else
      df.withColumn(IcMaterialType, lit(InstructionPlan))
  }

}
