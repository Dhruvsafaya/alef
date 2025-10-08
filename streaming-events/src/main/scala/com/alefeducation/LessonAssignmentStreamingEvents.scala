package com.alefeducation

import com.alefeducation.bigdata.Sink
import com.alefeducation.schema.Schema.{lessonClassAssignedUnAssignedSchema, studentLessonAssignmentSchema}
import com.alefeducation.service.{DataSink, SparkStreamingService}
import com.alefeducation.util.Constants.{ClassLessonAssignedEvent, ClassLessonUnAssignedEvent, LearnerLessonAssignedEvent, LearnerLessonUnAssignedEvent}
import com.alefeducation.util.DataFrameUtility.process
import com.alefeducation.util.Resources.getPayloadStream
import com.alefeducation.util.{DateField, SparkSessionUtils}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.LongType

class LessonAssignmentStreamingEvents(override val session: SparkSession) extends SparkStreamingService {

  override val name: String = LessonAssignmentStreamingEvents.name

  override def transform(): List[Sink] = {
    val kafkaStream = read("lesson-assignment-source", session)
    val payloadStream = getPayloadStream(session, kafkaStream)
    val dateOccurredOnEpoch = DateField("occurredOn", LongType)

    val sinks = List(
      DataSink(
        "student-lesson-assignment-sink",
        process(session,
          payloadStream,
          studentLessonAssignmentSchema,
          List(LearnerLessonAssignedEvent, LearnerLessonUnAssignedEvent),
          dateOccurredOnEpoch)
      ),
      DataSink(
        "class-lesson-assignment-sink",
        process(session,
          payloadStream,
          lessonClassAssignedUnAssignedSchema,
          List(ClassLessonAssignedEvent, ClassLessonUnAssignedEvent),
          dateOccurredOnEpoch)
      )
    )

    sinks
  }
}

object LessonAssignmentStreamingEvents {

  private val name = "lesson-assignment"

  def main(args: Array[String]): Unit = {
    new LessonAssignmentStreamingEvents(SparkSessionUtils.getSession(name)).run
  }
}
