package com.alefeducation

import com.alefeducation.schema.Schema._
import com.alefeducation.service.{DataSink, SparkStreamingService}
import com.alefeducation.util.DataFrameUtility.process
import com.alefeducation.util.{DateField, SparkSessionUtils}
import com.alefeducation.util.Resources.getPayloadStream
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.LongType

class TeacherTestRawEvents(override val session: SparkSession) extends SparkStreamingService {

  import TeacherTestRawEvents._

  override val name: String = TeacherTestRawEvents.name

  override def transform(): List[DataSink] = {
    val kafkaStream = read(ParquetTeacherTestFeatureSource, session)
    val payloadStream = getPayloadStream(session, kafkaStream)
    val dateOccurredOnTs = DateField("occurredOn", LongType)

    val sinks = List(
      DataSink(
        "test-blueprint-created-sink",
        process(session,
                payloadStream,
                testBlueprintCreatedIntegrationEventSchema,
                List(TestBlueprintCreatedIntegrationEvent),
                dateOccurredOnTs)
      ),
      DataSink(
        "test-blueprint-context-updated-sink",
        process(session,
                payloadStream,
                testBlueprintContextUpdatedIntegrationEventSchema,
                List(TestBlueprintContextUpdatedIntegrationEvent),
                dateOccurredOnTs)
      ),
      DataSink(
        "test-blueprint-guidance-updated-sink",
        process(session,
                payloadStream,
                testBlueprintGuidanceUpdatedIntegrationEventSchema,
                List(TestBlueprintGuidanceUpdatedIntegrationEvent),
                dateOccurredOnTs)
      ),
      DataSink(
        "test-blueprint-made-ready-sink",
        process(session,
                payloadStream,
                testBlueprintMadeReadyIntegrationEventSchema,
                List(TestBlueprintMadeReadyIntegrationEvent),
                dateOccurredOnTs)
      ),
      DataSink(
        "test-blueprint-published-sink",
        process(session,
                payloadStream,
                testBlueprintPublishedIntegrationEventSchema,
                List(TestBlueprintPublishedIntegrationEvent),
                dateOccurredOnTs)
      ),
        DataSink(
            "test-blueprint-discarded-sink",
            process(session,
                    payloadStream,
                    testBlueprintDiscardedIntegrationEventSchema,
                    List(TestBlueprintDiscardedIntegrationEvent),
                    dateOccurredOnTs)
        ),
      DataSink(
        "test-blueprint-archived-sink",
        process(session,
          payloadStream,
          testBlueprintArchivedIntegrationEventSchema,
          List(TestBlueprintArchivedIntegrationEvent),
          dateOccurredOnTs)
      ),
      DataSink("test-created-sink",
               process(session, payloadStream, testCreatedIntegrationEventSchema, List(TestCreatedIntegrationEvent), dateOccurredOnTs)),
      DataSink(
        "test-context-updated-sink",
        process(session,
                payloadStream,
                testContextUpdatedIntegrationEventSchema,
                List(TestContextUpdatedIntegrationEvent),
                dateOccurredOnTs)
      ),
      DataSink("test-item-replaced-sink",
               process(session, payloadStream, itemReplacedIntegrationEventSchema, List(ItemReplacedIntegrationEvent), dateOccurredOnTs)),
      DataSink("test-item-reordered-sink",
               process(session, payloadStream, itemReorderedIntegrationEventSchema, List(ItemReorderedIntegrationEvent), dateOccurredOnTs)),
      DataSink(
        "test-items-regenerated-sink",
        process(session,
                payloadStream,
                testItemsRegeneratedIntegrationEventSchema,
                List(TestItemsRegeneratedIntegrationEvent),
                dateOccurredOnTs)
      ),
      DataSink("test-published-sink",
               process(session, payloadStream, testPublishedIntegrationEventSchema, List(TestPublishedIntegrationEvent), dateOccurredOnTs)),
      DataSink("test-discarded-sink",
               process(session, payloadStream, testDiscardedIntegrationEventSchema, List(TestDiscardedIntegrationEvent), dateOccurredOnTs)),
      DataSink("test-archived-sink",
        process(session, payloadStream, testArchivedIntegrationEventSchema, List(TestArchivedIntegrationEvent), dateOccurredOnTs)),

      DataSink(
        "test-delivery-created-sink",
        process(session,
                payloadStream,
                testDeliveryCreatedIntegrationEventSchema,
                List(TestDeliveryCreatedIntegrationEvent),
                dateOccurredOnTs)
      ),
      DataSink(
        "test-delivery-archived-sink",
        process(session,
          payloadStream,
          testDeliveryArchivedIntegrationEventSchema,
          List(TestDeliveryArchivedIntegrationEvent),
          dateOccurredOnTs)
      ),
      DataSink(
        "test-delivery-started-sink",
        process(session,
                payloadStream,
                testDeliveryStartedIntegrationEventSchema,
                List(TestDeliveryStartedIntegrationEvent),
                dateOccurredOnTs)
      ),
      DataSink(
        "test-delivery-candidate-updated-sink",
        process(session,
                payloadStream,
                testDeliveryCandidateUpdatedIntegrationEventSchema,
                List(TestDeliveryCandidateUpdatedIntegrationEvent),
                dateOccurredOnTs)
      ),
      DataSink(
        "test-candidate-session-recorder-made-in-progress-sink",
        process(
          session,
          payloadStream,
          candidateSessionRecorderMadeInProgressIntegrationEventSchema,
          List(CandidateSessionRecorderMadeInProgressIntegrationEvent),
          dateOccurredOnTs
        )
      ),
      DataSink(
        "test-candidate-session-recorder-made-in-completed-sink",
        process(
          session,
          payloadStream,
          candidateSessionRecorderMadeCompletedIntegrationEventSchema,
          List(CandidateSessionRecorderMadeCompletedIntegrationEvent),
          dateOccurredOnTs
        )
      ),
      DataSink(
        "test-candidate-session-recorder-archived-sink",
        process(
          session,
          payloadStream,
          candidateSessionRecorderArchivedIntegrationEventSchema,
          List(CandidateSessionRecorderArchivedIntegrationEvent),
          dateOccurredOnTs
        )
      )
    )
    sinks
  }
}

object TeacherTestRawEvents {
  private val name = "teacher-test-integration-events"
  final val ParquetTeacherTestFeatureSource = "teacher-test-integration-events-source"
  final val TestBlueprintCreatedIntegrationEvent = "TestBlueprintCreatedIntegrationEvent"
  final val TestBlueprintContextUpdatedIntegrationEvent = "TestBlueprintContextUpdatedIntegrationEvent"
  final val TestBlueprintGuidanceUpdatedIntegrationEvent = "TestBlueprintGuidanceUpdatedIntegrationEvent"
  final val TestBlueprintMadeReadyIntegrationEvent = "TestBlueprintMadeReadyIntegrationEvent"
  final val TestBlueprintPublishedIntegrationEvent = "TestBlueprintPublishedIntegrationEvent"
  final val TestBlueprintDiscardedIntegrationEvent = "TestBlueprintDiscardedIntegrationEvent"
  final val TestBlueprintArchivedIntegrationEvent = "TestBlueprintArchivedIntegrationEvent"
  final val TestCreatedIntegrationEvent = "TestCreatedIntegrationEvent"
  final val TestContextUpdatedIntegrationEvent = "TestContextUpdatedIntegrationEvent"
  final val ItemReplacedIntegrationEvent = "ItemReplacedIntegrationEvent"
  final val ItemReorderedIntegrationEvent = "ItemReorderedIntegrationEvent"
  final val TestItemsRegeneratedIntegrationEvent = "TestItemsRegeneratedIntegrationEvent"
  final val TestPublishedIntegrationEvent = "TestPublishedIntegrationEvent"
  final val TestDiscardedIntegrationEvent = "TestDiscardedIntegrationEvent"
  final val TestArchivedIntegrationEvent = "TestArchivedIntegrationEvent"
  final val TestDeliveryCreatedIntegrationEvent = "TestDeliveryCreatedIntegrationEvent"
  final val TestDeliveryArchivedIntegrationEvent = "TestDeliveryArchivedIntegrationEvent"
  final val TestDeliveryStartedIntegrationEvent = "TestDeliveryStartedIntegrationEvent"
  final val TestDeliveryCandidateUpdatedIntegrationEvent = "TestDeliveryCandidateUpdatedIntegrationEvent"
  final val CandidateSessionRecorderMadeInProgressIntegrationEvent = "CandidateSessionRecorderMadeInProgressIntegrationEvent"
  final val CandidateSessionRecorderMadeCompletedIntegrationEvent = "CandidateSessionRecorderMadeCompletedIntegrationEvent"
  final val CandidateSessionRecorderArchivedIntegrationEvent = "CandidateSessionRecorderArchivedIntegrationEvent"

  def main(args: Array[String]): Unit = {
    new TeacherTestRawEvents(SparkSessionUtils.getSession(name)).run
  }
}
