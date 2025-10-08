package com.alefeducation.dimensions

import com.alefeducation.bigdata.Sink
import com.alefeducation.bigdata.batch.{DeltaSink, Action}
import com.alefeducation.service.SparkBatchService
import com.alefeducation.util.BatchUtility.transformDataFrame
import com.alefeducation.util.Constants._
import com.alefeducation.util.Helpers._
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql._

class SubjectDimensionTransformer(override val name: String, override implicit val session: SparkSession) extends SparkBatchService {

  private val dimensionColMapping = Map(
    "uuid" -> "subject_id",
    "name" -> "subject_name",
    "online" -> "subject_online",
    "genSubject" -> "subject_gen_subject",
    "subject_status" -> "subject_status",
    "schoolGradeUuid" -> "grade_id",
    "occurredOn" -> "occurredOn"
  )

  private val supportedEventTypes = List(
    (Action.CREATE, AdminSubjectCreated),
    (Action.UPDATE, AdminSubjectUpdated),
    (Action.DELETE, AdminSubjectDeleted)
  )

  override def transform(): List[Sink] = {
    val rawDataFrame = read(SubjectParquetSource, session)

    transformDataFrame(
      session,
      rawDataFrame,
      supportedEventTypes.map(_._2),
      dimensionColMapping,
      SubjectEntity,
      RedshiftSubjectSink,
      SubjectParquetSource
    ) ++
      supportedEventTypes.flatMap(eventInfo => {
        DeltaSink.getDeltaSink(
          sinkName = SubjectDimension.DeltaSinkName,
          dataFrame = rawDataFrame,
          entity = SubjectEntity,
          colMapping = dimensionColMapping,
          eventType = eventInfo._2,
          action = eventInfo._1
        )
      })
  }

}

object SubjectDimension {

  val DeltaSinkName = "delta-subject-sink"

  def main(args: Array[String]): Unit = {
    new SubjectDimensionTransformer(SubjectDimensionName, SparkSessionUtils.getSession(SubjectDimensionName)).run
  }
}
