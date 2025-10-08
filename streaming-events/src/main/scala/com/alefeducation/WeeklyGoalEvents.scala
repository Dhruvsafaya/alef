package com.alefeducation

import com.alefeducation.schema.Schema._
import com.alefeducation.service.{DataSink, SparkStreamingService}
import com.alefeducation.util.Constants._
import com.alefeducation.util.DataFrameUtility.process
import com.alefeducation.util.{DateField, SparkSessionUtils}
import com.alefeducation.util.Resources.getPayloadStream
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.LongType

class WeeklyGoalEvents(override val session: SparkSession) extends SparkStreamingService {

  override val name: String = WeeklyGoalEvents.name

  override def transform(): List[DataSink] = {
    val kafkaStream = read("raw-events-source", session)
    val payloadStream = getPayloadStream(session, kafkaStream)
    val dateOccurredOnEpoch = DateField("occurredOn", LongType)

    val sinks = List(
      DataSink(
        "student-weekly-goal-sink",
        process(session,
                payloadStream,
                weeklyGoalSchema,
                List(WeeklyGoalCreatedEvent, WeeklyGoalCompletedEvent, WeeklyGoalExpiredEvent), dateOccurredOnEpoch)
      ),
      DataSink(
        "student-weekly-goal-activity-sink",
        process(session,
                payloadStream,
                weeklyGoalProgressEventSchema,
                List(WeeklyGoalProgressEvent), dateOccurredOnEpoch)
      ),
      DataSink(
        "student-weekly-goal-star-sink",
        process(session,
          payloadStream,
          weeklyGoalStarSchema,
          List(WeeklyGoalStarEarnedEvent), dateOccurredOnEpoch)
      ),
    )

    sinks
  }
}

object WeeklyGoalEvents {

  private val name = "weekly-goal-events"

  def main(args: Array[String]): Unit = {
    new WeeklyGoalEvents(SparkSessionUtils.getSession(name)).run
  }
}



