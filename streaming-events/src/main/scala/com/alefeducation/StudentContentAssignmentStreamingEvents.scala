package com.alefeducation

import com.alefeducation.bigdata.Sink
import com.alefeducation.schema.Schema.{contentClassAssignedUnAssignedSchema, studentContentAssignmentSchema, teamDeletedSchema, teamMembersUpdatedSchema, teamMutatedSchema}
import com.alefeducation.service.{DataSink, SparkStreamingService}
import com.alefeducation.util.Constants.{ClassContentAssignedEvent, ClassContentUnAssignedEvent, StudentContentAssignedEvent, StudentContentUnAssignedEvent}
import com.alefeducation.util.DataFrameUtility.process
import com.alefeducation.util.Resources.getPayloadStream
import com.alefeducation.util.{DateField, SparkSessionUtils}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.LongType

class StudentContentAssignmentStreamingEvents(override val session: SparkSession) extends SparkStreamingService {

  override val name: String = StudentContentAssignmentStreamingEvents.name

  override def transform(): List[Sink] = {
    val kafkaStream = read("student-content-assignment-source", session)
    val payloadStream = getPayloadStream(session, kafkaStream)
    val dateOccurredOnEpoch = DateField("occurredOn", LongType)

    val sinks = List(
      DataSink(
        "student-content-assignment-sink",
        process(session,
          payloadStream,
          studentContentAssignmentSchema,
          List(StudentContentAssignedEvent, StudentContentUnAssignedEvent),
          dateOccurredOnEpoch)
      ),
      DataSink(
        "class-content-assignment-sink",
        process(session,
          payloadStream,
          contentClassAssignedUnAssignedSchema,
          List(ClassContentAssignedEvent, ClassContentUnAssignedEvent),
          dateOccurredOnEpoch)
      )
    )

    val teamSinks = List(
      DataSink(
        "team-mutated-sink",
        process(session, payloadStream, teamMutatedSchema, List("TeamCreatedEvent", "TeamUpdatedEvent"), dateOccurredOnEpoch)
      ),
      DataSink(
        "team-deleted-sink",
        process(session, payloadStream, teamDeletedSchema, List("TeamDeletedEvent"), dateOccurredOnEpoch)
      ),
      DataSink(
        "team-members-updated-sink",
        process(session, payloadStream, teamMembersUpdatedSchema, List("TeamMembersUpdatedEvent"), dateOccurredOnEpoch)
      )
    )

    sinks ++ teamSinks
  }

}

object StudentContentAssignmentStreamingEvents {

  private val name = "student-content-assignment"

  def main(args: Array[String]): Unit = {
    new StudentContentAssignmentStreamingEvents(SparkSessionUtils.getSession(name)).run
  }
}
