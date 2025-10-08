package com.alefeducation

import com.alefeducation.schema.Schema._
import com.alefeducation.service.{DataSink, SparkStreamingService}
import com.alefeducation.util.Constants._
import com.alefeducation.util.DataFrameUtility.process
import com.alefeducation.util.Resources.getPayloadStream
import com.alefeducation.util.{Constants, DateField, SparkSessionUtils}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.TimestampType

class PathwayProgressRawEvents(override val name: String, override val session: SparkSession) extends SparkStreamingService() {

    override def transform(): List[DataSink] = {
      val kafkaStream = read(PathwayProgressRawEvents.source, session)
      val payloadStream = getPayloadStream(session, kafkaStream)
      val dateField = DateField("occurredOn", TimestampType)
      val progressEvents = List(
        DataSink("learning-pathway-activity-completed-sink",
          process(session, payloadStream, pathwayActivityCompletedSchema, List(PathwayActivityCompletedEvent), dateField)),
        DataSink("learning-level-completed-sink",
          process(session, payloadStream, levelCompletedSchema, List(LevelCompletedEvent), dateField)),
        DataSink("learning-levels-recommended-sink",
          process(session, payloadStream, levelsRecommendedSchema, List(LevelsRecommendedEvent), dateField)),
        DataSink("learning-student-domain-grade-update-sink",
          process(session, payloadStream, studentDomainGradeChangedSchema, List(Constants.StudentDomainGradeChangedEvent), dateField)),
        DataSink("learning-student-manual-placement-sink",
          process(session, payloadStream, studentManualPlacementSchema, List(Constants.StudentManualPlacementEvent), dateField)),
        DataSink("learning-teacher-activities-assigned-sink",
          process(session, payloadStream, PathwayActivitiesAssignedSchema, List(Constants.PathwayActivitiesAssignedEvent), dateField)),
        DataSink("learning-teacher-activity-unassigned-sink",
          process(session, payloadStream, PathwaysActivityUnAssignedSchema, List(Constants.PathwaysActivityUnAssignedEvent), dateField)),
        DataSink("learning-placement-completed-sink",
          process(session, payloadStream, placementCompletionSchema, List(placementCompletionEvent), dateField)),
        DataSink("learning-placement-test-completed-sink",
          process(session, payloadStream, placementTestCompletedSchema, List(placementTestCompletedEvent), dateField)),
        DataSink("learning-additional-resources-assigned-sink",
          process(session, payloadStream, AdditionalResourcesAssignedSchema, List(Constants.AdditionalResourcesAssignedEvent), dateField)),
        DataSink("learning-additional-resources-unassigned-sink",
          process(session, payloadStream, AdditionalResourceUnAssignedSchema, List(Constants.AdditionalResourceUnAssignedEvent), dateField)),
      )
      progressEvents
    }
}

object PathwayProgressRawEvents {

  val name = "pathway-progress"
  val source = "pathway-progress-source"

  def main(args: Array[String]): Unit = {
    new PathwayProgressRawEvents(name, SparkSessionUtils.getSession(name)).run
  }
}
