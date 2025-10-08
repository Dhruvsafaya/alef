package com.alefeducation.bigdata.batch.delta

import com.alefeducation.bigdata.batch.{Create, Delete, InsertIfNotExists, Iwh, Reset, SCDTypeII, Scd, Update, Upsert, Write}
import org.apache.spark.sql.DataFrame

sealed trait OperationContext

case class WriteContext(df: DataFrame) extends OperationContext with Write

case class CreateContext(df: DataFrame, isFact: Boolean) extends OperationContext with Create

case class UpdateContext(df: DataFrame, matchConditions: String, updateColumns: List[String]) extends OperationContext with Update

case class UpsertContext(df: DataFrame,
                         partitionBy: Seq[String],
                         matchConditions: String,
                         columnsToUpdate: Map[String, String],
                         columnsToInsert: Map[String, String],
                         updateConditions: Option[String]) extends OperationContext with Upsert

case class DeleteContext(df: DataFrame, matchConditions: String) extends OperationContext with Delete

@deprecated("user SCDTypeIIContext")
case class SCDContext(df: DataFrame, matchConditions: String, uniqueIdColumns: List[String]) extends OperationContext with Scd

@deprecated("user SCDTypeIIContext")
case class IWHContext(df: DataFrame, matchConditions: String, uniqueIdColumns: List[String], activeState: Int, inactiveStatus: Int) extends OperationContext with Iwh

case class SCDTypeIIContext(df: DataFrame, matchConditions: String, uniqueIdColumns: List[String], activeState: Int, inactiveStatus: Int) extends OperationContext with SCDTypeII

case class ResetContext(df: DataFrame, matchConditions: String, uniqueIdColumns: List[String]) extends OperationContext with Reset

case class InsertIfNotExistsContext(df: DataFrame, matchConditions: String, filterNot: List[String] = List.empty) extends OperationContext with InsertIfNotExists
