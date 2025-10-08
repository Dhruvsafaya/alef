package com.alefeducation.dimensions.team

import com.alefeducation.bigdata.Sink
import com.alefeducation.bigdata.batch.delta.{Alias, Delta}
import com.alefeducation.service.SparkBatchService
import com.alefeducation.util.DataFrameUtility.collectAsString
import com.alefeducation.util.{BatchTransformerUtility, Resources, SparkSessionUtils}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{explode_outer, size}

import scala.collection.immutable.{ListMap, Map}

class TeamStudentAssociationDimension(override implicit val session: SparkSession) extends SparkBatchService {

  import session.implicits._
  import TeamStudentAssociationDimension._
  import com.alefeducation.util.BatchTransformerUtility._

  override val name: String = JobName

  override def transform(): List[Sink] = {
    import BatchTransformerUtility._
    import Delta.Transformations._

    val updatedDF = readOptional(ParquetSource, session).map(_.cache())

    val latestUpdatedDF = updatedDF.map(_.selectLatestRecordsByEventType(EventType, List("teamId"), "occurredOn").cache())
    val latestAssociatedDF =
      latestUpdatedDF.map(
        _.filter(size($"students") > 0)
          .transform(explodeStudents)
          .transformForSCD(ColumnMapping.Associated, EntityName)
          .transform(dropDeletedTime))
    val deltaAssociatedSCDSink = latestAssociatedDF.flatMap(
      _.renameUUIDcolsForDelta(prefix = Some(s"${EntityName}_"))
        .toSCD(
          uniqueIdColumns = List(s"${EntityName}_team_id", s"${EntityName}_student_id"),
          matchConditions =
            s"${Alias.Delta}.${EntityName}_team_id = ${Alias.Events}.${EntityName}_team_id and ${Alias.Delta}.${EntityName}_student_id = ${Alias.Events}.${EntityName}_student_id"
        )
        .map(_.toSink(DeltaSinkAssociated, EntityName)))
    val latestUnassociatedDF =
      latestUpdatedDF.map(
        _.filter(size($"students") === 0)
          .transformForSCD(ColumnMapping.Unassociated, EntityName)
          .transform(dropDeletedTime)
          .cache())
    val deltaUnassociatedSCDSink = latestUnassociatedDF.flatMap(
      _.renameUUIDcolsForDelta(prefix = Some(s"${EntityName}_"))
        .toSCD(uniqueIdColumns = List(s"${EntityName}_team_id"),
               matchConditions = s"${Alias.Delta}.${EntityName}_team_id = ${Alias.Events}.${EntityName}_team_id")
        .map(_.toSink(DeltaSinkUnassociated, EntityName, create = false)))

    val transformedUpdatedDF = latestUpdatedDF.map(
      _.transform(explodeStudents).transformForSCD(ColumnMapping.Updated, EntityName).transform(dropDeletedTime).cache())
    val redshiftUpdatedSCDSink =
      transformedUpdatedDF.map(_.toRedshiftSCDSink(RedshiftSink, EventType, EntityName, setScdTypeIIUpdate))

    val parquetUpdatedSink = updatedDF.map(_.toParquetSink(ParquetSink).copy(eventType = EventType))

    (deltaAssociatedSCDSink ++ deltaUnassociatedSCDSink ++ redshiftUpdatedSCDSink ++ parquetUpdatedSink).toList
  }

  private def explodeStudents(df: DataFrame): DataFrame = df.withColumn("studentId", explode_outer($"students")).drop("students")

  private def dropDeletedTime(df: DataFrame): DataFrame = df.drop($"${EntityName}_deleted_time")

  private def setScdTypeIIUpdate(dataFrame: DataFrame, name: String): Map[String, String] = {
    val targetTableDim = s"dim_$name"
    val targetTableRel = s"rel_$name"
    val idColRel = s"team_uuid"
    val idColDim = s"${name}_team_dw_id"
    val dimSchema = Resources.redshiftSchema()
    val relSchema = Resources.redshiftStageSchema()
    val stagingTable = s"staging_$targetTableDim"
    val insertCols = dataFrame.columns.mkString(", ")
    val updateStatusAndActiveUntil =
      s"""
       begin transaction;
       update $relSchema.$targetTableRel set ${name}_status = 2
       from $relSchema.$targetTableRel target
       join $dimSchema.$stagingTable staging on staging.$idColRel = target.$idColRel
       where target.${name}_created_time < staging.${name}_created_time
       and target.${name}_status = 1 and target.${name}_active_until is null;

       update $relSchema.$targetTableRel set ${name}_active_until = staging.${name}_created_time
       from $relSchema.$targetTableRel target
       join $dimSchema.$stagingTable staging on staging.$idColRel = target.$idColRel
       where target.${name}_created_time < staging.${name}_created_time
       and target.${name}_active_until is null;

       update $dimSchema.$targetTableDim set ${name}_status = 2
       from $dimSchema.$targetTableDim target
       join $dimSchema.dim_team team on team.team_dw_id = target.$idColDim
       join $dimSchema.$stagingTable staging on team.team_id = staging.$idColRel
       where target.${name}_created_time < staging.${name}_created_time and
       target.${name}_status = 1 and target.${name}_active_until is null;

       update $dimSchema.$targetTableDim set ${name}_active_until = staging.${name}_created_time
       from $dimSchema.$targetTableDim target
       join $dimSchema.dim_team team on team.team_dw_id = target.$idColDim
       join $dimSchema.$stagingTable staging on team.team_id = staging.$idColRel
       where target.${name}_created_time < staging.${name}_created_time
       and target.${name}_active_until is null;

       insert into $relSchema.$targetTableRel ($insertCols)
       (select $insertCols from $dimSchema.$stagingTable WHERE $stagingTable.student_uuid IS NOT NULL);
       DROP table $dimSchema.$stagingTable;
       end transaction
     """.stripMargin
    Map("dbtable" -> s"$dimSchema.$stagingTable", "postactions" -> updateStatusAndActiveUntil)
  }

}

object TeamStudentAssociationDimension {

  val JobName: String = "team-student-association-dimension"

  val EntityName: String = "team_student_association"

  val EventType: String = "TeamMembersUpdatedEvent"

  val DeltaSinkAssociated = "delta-team-members-associated-sink"

  val DeltaSinkUnassociated = "delta-team-members-unassociated-sink"

  val RedshiftSink: String = "redshift-team-members-updated-sink"

  val ParquetSource = "parquet-team-members-updated-source"

  val ParquetSink = "parquet-team-members-updated-sink"

  object ColumnMapping {
    val Common: Map[String, String] = ListMap(
      "teamId" -> s"team_uuid",
      "occurredOn" -> "occurredOn"
    )

    val Updated: Map[String, String] = ListMap(
      "studentId" -> s"student_uuid",
      s"${EntityName}_status" -> s"${EntityName}_status"
    ) ++ Common

    val Associated: Map[String, String] = Updated

    val Unassociated: Map[String, String] = Common

  }

  def apply(implicit session: SparkSession): TeamStudentAssociationDimension = new TeamStudentAssociationDimension

  def main(args: Array[String]): Unit = TeamStudentAssociationDimension(SparkSessionUtils.getSession(JobName)).run

}
