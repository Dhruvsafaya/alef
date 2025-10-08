package com.alefeducation.bigdata.batch.delta

import com.alefeducation.bigdata.batch._
import com.alefeducation.bigdata.batch.delta.operations.{DeltaSCD, DeltaSCDSink}
import com.alefeducation.util.Constants.{ActiveState, HistoryState}
import org.apache.spark.sql.DataFrame

case class Transform(entity: String,
                     isFact: Boolean = false,
                     withoutColumns: List[String] = Nil,
                     mappingsOverride: Map[String, String] = Map(),
                     uniqueIdColumns: List[String] = Nil)

trait DeltaOperation {

  def applyTransformations(df: DataFrame, transform: Transform): DataFrame

}

object DeltaOperation {

  def buildMatchColumns(matchConditions: String = "", entity: String): String = {
    val res =
      if (matchConditions.isEmpty) s"${Alias.Delta}.${entity}_id = ${Alias.Events}.${entity}_id"
      else matchConditions

    res + s" and ${Alias.Delta}.${entity}_created_time < ${Alias.Events}.${entity}_created_time"
  }

}

object Delta {

  object Transformations {

    private[delta] def applyTransformations[T <: DeltaOperation](df: DataFrame,
                                                                 operation: T,
                                                                 transform: Option[Transform]): Option[DataFrame] = {
      if (df.isEmpty) None
      else {
        val outputDF = transform match {
          case None => df
          case Some(t) => operation.applyTransformations(df, t)
        }
        Some(outputDF)
      }
    }

    implicit class DeltaDataFrameTransformationsOptional(optionalDf: Option[DataFrame]) {
      def toSCD(matchConditions: String = "", uniqueIdColumns: List[String] = Nil): Option[SCDContext] = {
        optionalDf.map(df => SCDContext(df, matchConditions = matchConditions, uniqueIdColumns = uniqueIdColumns))
      }
    }

    implicit class DeltaDataFrameTransformations(df: DataFrame) {

      def toWrite: Option[WriteContext] = {
        if (df.isEmpty) None
        else Some(WriteContext(df))
      }

      def toCreate(transform: Option[Transform] = None, isFact: Boolean = false): Option[CreateContext] =
        applyTransformations(df, DeltaCreate, transform).map { dataFrame =>
          CreateContext(df = dataFrame, isFact = isFact)
        }

      def toUpdate(transform: Option[Transform] = None,
                   matchConditions: String = "",
                   updateColumns: List[String] = Nil): Option[UpdateContext] =
        applyTransformations(df, DeltaUpdate, transform).map { dataFrame =>
          UpdateContext(df = dataFrame, matchConditions = matchConditions, updateColumns = updateColumns)
        }

      def toUpsert(matchConditions: String,
                   partitionBy: Seq[String],
                   columnsToUpdate: Map[String, String],
                   columnsToInsert: Map[String, String],
                   updateConditions: Option[String] = None): Option[UpsertContext] = {
        if (df.isEmpty)
          return None

        Some(
          UpsertContext(
            df,
            partitionBy,
            matchConditions,
            columnsToUpdate = columnsToUpdate,
            columnsToInsert = columnsToInsert,
            updateConditions = updateConditions
          )
        )
      }

      def toDelete(transform: Option[Transform] = None, matchConditions: String = ""): Option[DeleteContext] =
        applyTransformations(df, DeltaDelete, transform).map { dataFrame =>
          DeleteContext(df = dataFrame, matchConditions = matchConditions)
        }

      @deprecated(message = "use toSCDContext to avoid unnecessary isEmpty call")
      def toSCD(matchConditions: String = "", uniqueIdColumns: List[String] = Nil): Option[SCDContext] = {
        if (df.rdd.isEmpty) None
        else Some(SCDContext(df = df, matchConditions = matchConditions, uniqueIdColumns = uniqueIdColumns))
      }

      @deprecated("use toSCDTypeII")
      def toSCDContext(matchConditions: String = "", uniqueIdColumns: List[String] = Nil): SCDContext = {
        SCDContext(df = df, matchConditions = matchConditions, uniqueIdColumns = uniqueIdColumns)
      }

      @deprecated(message = "use toIWHContext to avoid unnecessary isEmpty call")
      def toIWH(matchConditions: String = "", uniqueIdColumns: List[String] = Nil, inactiveStatus: Int = 4, activeState: Int = ActiveState): Option[IWHContext] = {
        if (df.rdd.isEmpty) None
        else
          Some(IWHContext(df = df, matchConditions = matchConditions, uniqueIdColumns = uniqueIdColumns, activeState = activeState, inactiveStatus = inactiveStatus))
      }

      @deprecated("use toSCDTypeII")
      def toIWHContext(matchConditions: String = "", uniqueIdColumns: List[String] = Nil, inactiveStatus: Int = 4, activeState: Int = ActiveState): IWHContext = {
        IWHContext(df = df, matchConditions = matchConditions, uniqueIdColumns = uniqueIdColumns, activeState = activeState, inactiveStatus = inactiveStatus)
      }

      /**
       * DO NOT make `matchConditions` and `uniqueIdColumns` as default value for avoid future bugs
       * @param matchConditions
       * @param uniqueIdColumns
       * @param activeState
       * @param historyStatus
       * @return
       */
      def toSCDTypeII(matchConditions: String,
                      uniqueIdColumns: List[String],
                      activeState: Int = ActiveState,
                      historyStatus: Int = HistoryState): SCDTypeIIContext = {
        SCDTypeIIContext(
          df = df,
          matchConditions = matchConditions,
          uniqueIdColumns = uniqueIdColumns,
          activeState = activeState,
          inactiveStatus = historyStatus
        )
      }

      def toInsertIfNotExists(matchConditions: String, filterNot: List[String] = List.empty): InsertIfNotExistsContext = {
        InsertIfNotExistsContext(df, matchConditions, filterNot)
      }

      @deprecated(message = "use toResetContext to avoid unnecessary isEmpty call")
      def toReset(matchConditions: String = "", uniqueIdColumns: List[String] = Nil): Option[ResetContext] = {
        if (df.rdd.isEmpty) None
        else
          Some(ResetContext(df = df, matchConditions = matchConditions, uniqueIdColumns = uniqueIdColumns))
      }

      def toResetContext(matchConditions: String = "", uniqueIdColumns: List[String] = Nil): ResetContext = {
        ResetContext(df = df, matchConditions = matchConditions, uniqueIdColumns = uniqueIdColumns)
      }
    }

    implicit class DeltaWriteContextTransformations(context: WriteContext) {

      def toSink(name: String, writerOptions: Map[String, String] = Map()): DeltaWriteSink =
        new DeltaWriteSink(context.df.sparkSession, name, context.df, context.df, writerOptions)

    }

    implicit class DeltaCreateContextTransformations(context: CreateContext) {

      def toSink(name: String,
                 writerOptions: Map[String, String] = Map(),
                 eventType: String = "",
                 partitionBy: String = Columns.EventDate
                ): DeltaCreateSink = {
        val outputWriterOptions = if (context.isFact) {
          writerOptions ++ Map(BatchWriter.Options.PartitionBy -> partitionBy)
        } else {
          writerOptions
        }
        new DeltaCreateSink(context.df.sparkSession, name, context.df, context.df, outputWriterOptions, eventType)
      }

    }

    implicit class DeltaUpdateContextTransformations(context: UpdateContext) {

      def toSink(name: String, entity: String, eventType: String = ""): DeltaUpdateSink = {
        val matchColumns = DeltaOperation.buildMatchColumns(context.matchConditions, entity)
        val updateColumns = DeltaUpdate.buildUpdateColumns(context.updateColumns, entity, context.df)
        new DeltaUpdateSink(context.df.sparkSession, name, context.df, context.df, matchColumns, updateColumns, eventType)
      }

    }

    implicit class DeltaUpsertContextTransformations(context: UpsertContext) {

      def toSink(name: String): DeltaUpsertSink = {
        new DeltaUpsertSink(
          spark = context.df.sparkSession,
          name = name,
          df = context.df,
          input = context.df,
          partitionBy = context.partitionBy,
          matchConditions = context.matchConditions,
          columnsToUpdate = context.columnsToUpdate,
          columnsToInsert = context.columnsToInsert,
          updateConditions = context.updateConditions
        )
      }

    }

    implicit class DeltaDeleteContextTransformations(context: DeleteContext) {

      def toSink(name: String, entity: String, eventType: String = ""): DeltaDeleteSink = {
        val matchColumns = DeltaOperation.buildMatchColumns(context.matchConditions, entity)
        val updateColumns = DeltaDelete.buildUpdateColumns(entity)
        new DeltaDeleteSink(context.df.sparkSession, name, context.df, context.df, matchColumns, updateColumns, eventType)
      }

    }

    @deprecated("use SCDTypeII")
    implicit class DeltaSCDContextTransformations(context: SCDContext) {

      def toSink(name: String, entity: String, create: Boolean = true, eventType: String = ""): DeltaSCDSink = {
        val matchColumns = DeltaSCD.buildMatchColumns(entity, context.matchConditions)
        val updateColumns = DeltaSCD.buildUpdateColumns(entity)
        new DeltaSCDSink(context.df.sparkSession,
          name,
          context.df,
          context.df,
          matchColumns,
          updateColumns,
          entity,
          context.uniqueIdColumns,
          create,
          eventType)
      }

    }

    @deprecated("use SCDTypeII")
    implicit class DeltaIWHContextTransformations(context: IWHContext) {

      def toSink(name: String, entity: String, eventType: String = "", isActiveUntilVersion: Boolean = false): DeltaIwhSink = {
        val matchColumns = DeltaIWH.buildMatchColumns(entity, context.matchConditions, context.activeState, context.inactiveStatus, isActiveUntilVersion)
        val updateColumns = DeltaIWH.buildUpdateColumns(entity, context.inactiveStatus, isActiveUntilVersion)
        val insertConditionColumns = DeltaIWH.buildInsertConditions(entity, context.matchConditions, context.activeState)
        new DeltaIwhSink(context.df.sparkSession,
          name,
          context.df,
          context.df,
          matchColumns,
          updateColumns,
          context.uniqueIdColumns,
          insertConditionColumns,
          eventType)
      }
    }

    implicit class DeltaSCDTypeIIContextTransformations(context: SCDTypeIIContext) {

      def toSink(name: String, entity: String): DeltaSCDTypeIISink = {
        val matchColumns = DeltaSCDTypeII.buildMatchConditions(entity, context.matchConditions, context.activeState)
        val insertConditionColumns = DeltaSCDTypeII.buildInsertConditions(entity, context.matchConditions, context.activeState)
        val updateColumns = DeltaSCDTypeII.buildUpdateColumns(entity, context.inactiveStatus)
        new DeltaSCDTypeIISink(name, context.df, matchColumns, updateColumns, context.uniqueIdColumns, insertConditionColumns)
      }

    }

    implicit class DeltaResetContextTransformations(context: ResetContext) {
      def toSink(name: String, entity: String, eventType: String = ""): DeltaResetSink = {
        val matchColumns = DeltaReset.buildMatchColumns(entity, context.matchConditions)
        new DeltaResetSink(context.df.sparkSession,
          name,
          context.df,
          context.df,
          matchColumns,
          Map(),
          context.uniqueIdColumns,
          eventType)
      }
    }

    implicit class DeltaInsertIfNotExistsContextTransformations(context: InsertIfNotExistsContext) {
      def toSink(name: String, entity: String): DeltaInsertIfNotExistsSink = {
        val matchConditions = DeltaInsertIfNotExists.buildMatchConditions(entity, context.matchConditions)
        new DeltaInsertIfNotExistsSink(
          name,
          context.df,
          matchConditions,
          filterNot = context.filterNot
        )
      }
    }

  }

}
