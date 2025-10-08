package com.alefeducation.dimensions.team

import com.alefeducation.bigdata.Sink
import com.alefeducation.bigdata.batch.delta.{Builder, Delta, DeltaSink}
import com.alefeducation.service.{DataSink, SparkBatchService}
import com.alefeducation.util.{BatchTransformerUtility, SparkSessionUtils}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.immutable.ListMap

class TeamDimension(override implicit val session: SparkSession) extends SparkBatchService {

  import TeamDimension._

  override val name: String = JobName

  override def transform(): List[Sink] = {
    import BatchTransformerUtility._
    import Builder.Transformations
    import Delta.Transformations._
    import session.implicits._

    val mutatedDF = readOptional(Parquet.Source.Mutated, session).map(_.cache())
    val createdDF = mutatedDF.map(_.filter($"eventType" === EventType.Created).cache())
    val updatedDF = mutatedDF.map(_.filter($"eventType" === EventType.Updated).cache())
    val deletedDF = readOptional(Parquet.Source.Deleted, session).map(_.cache())

    val outputCreatedDF = createdDF.map(_.transformForInsertDim(ColumnMapping.Mutated, EntityName, ids = List("teamId")).cache())
    val outputUpdatedDF = updatedDF.map(_.transformForUpdateDim(ColumnMapping.Mutated, EntityName, List("teamId")).cache())
    val outputDeletedDF = deletedDF.map(_.transformForDelete(ColumnMapping.Deleted, EntityName, List("teamId")).cache())

    val deltaCreatedSink: Option[DeltaSink] =
      outputCreatedDF.flatMap(_.withEventDateColumn(false).toCreate().map(_.toSink(DeltaSink, eventType = EventType.Created)))
    val deltaUpdatedSink: Option[DeltaSink] = outputUpdatedDF
      .flatMap(_.withEventDateColumn(false).toUpdate().map(_.toSink(DeltaSink, EntityName, EventType.Updated)))
    val deltaDeletedSink: Option[DeltaSink] = outputDeletedDF
      .flatMap(_.withEventDateColumn(false).toDelete().map(_.toSink(DeltaSink, EntityName, EventType.Deleted)))

    val redshiftCreatedSink =
      outputCreatedDF.map(_.transform(transformRedshiftMutatedDF).toRedshiftInsertSink(RedshiftSink, EventType.Created))
    val redshiftUpdatedSink =
      outputUpdatedDF.map(_.transform(transformRedshiftMutatedDF).toRedshiftUpdateSink(RedshiftSink, EventType.Updated, EntityName))
    val redshiftDeletedSink: Option[DataSink] = outputDeletedDF.map(_.toRedshiftUpdateSink(RedshiftSink, EventType.Deleted, EntityName))

    val parquetCreatedSink = createdDF.map(_.toParquetSink(Parquet.Sink.Mutated).copy(eventType = EventType.Created))
    val parquetUpdatedSink = updatedDF.map(_.toParquetSink(Parquet.Sink.Mutated).copy(eventType = EventType.Updated))
    val parquetDeletedSink = deletedDF.map(_.toParquetSink(Parquet.Sink.Deleted))

    val deltaSinks = deltaCreatedSink ++ deltaUpdatedSink ++ deltaDeletedSink
    val redshiftSinks = redshiftCreatedSink ++ redshiftUpdatedSink ++ redshiftDeletedSink
    val parquetSinks = parquetCreatedSink ++ parquetUpdatedSink ++ parquetDeletedSink

    (deltaSinks ++ redshiftSinks ++ parquetSinks).toList
  }

  private def transformRedshiftMutatedDF(df: DataFrame) = df.drop(s"${EntityName}_description")

}

object TeamDimension {

  val JobName: String = "team-dimension"
  val EntityName: String = "team"

  object EventType {
    val Created: String = "TeamCreatedEvent"
    val Updated: String = "TeamUpdatedEvent"
    val Deleted: String = "TeamDeletedEvent"
  }

  object ColumnMapping {
    val Mutated: Map[String, String] = ListMap(
      "teamId" -> s"${EntityName}_id",
      "name" -> s"${EntityName}_name",
      "description" -> s"${EntityName}_description",
      "classId" -> s"${EntityName}_class_id",
      "teacherId" -> s"${EntityName}_teacher_id",
      s"${EntityName}_status" -> s"${EntityName}_status",
      "occurredOn" -> "occurredOn"
    )

    val Deleted: Map[String, String] = ListMap(
      "teamId" -> s"${EntityName}_id",
      s"${EntityName}_status" -> s"${EntityName}_status",
      "occurredOn" -> "occurredOn"
    )
  }

  val DeltaSink: String = "delta-team-sink"
  val RedshiftSink: String = "redshift-team-sink"

  object Parquet {
    object Source {
      val Mutated: String = "parquet-team-mutated-source"
      val Deleted: String = "parquet-team-deleted-source"
    }

    object Sink {
      val Mutated: String = "parquet-team-mutated-sink"
      val Deleted: String = "parquet-team-deleted-sink"
    }
  }

  def apply(implicit session: SparkSession): TeamDimension = new TeamDimension

  def main(args: Array[String]): Unit = TeamDimension(SparkSessionUtils.getSession(JobName)).run

}
