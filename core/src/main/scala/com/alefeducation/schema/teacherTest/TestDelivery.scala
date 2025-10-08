package com.alefeducation.schema.teacherTest

case class TestDeliveryCreatedIntegrationEvent(
    id: String,
    testId: String,
    contextFrameClassId: String,
    contextDomainId: String,
    candidates: List[String],
    deliverySettings: DeliverySettings,
    status: String,
    allPossibleCandidates: List[String],
    occurredOn: Long,
    aggregateIdentifier: String
)

case class TestDeliveryStartedIntegrationEvent(
    id: String,
    contextFrameClassId: String,
    contextDomainId: String,
    candidates: List[String],
    deliverySettings: DeliverySettings,
    occurredOn: Long,
    status: String,
    aggregateIdentifier: String
)

case class TestDeliveryArchivedIntegrationEvent(
   id: String,
   updatedBy: String,
   updatedAt: String,
   status: String,
   occurredOn: Long,
   aggregateIdentifier: String,
 )

case class TestDeliveryCandidateUpdatedIntegrationEvent(
    id: String,
    candidates: List[String],
    testId: String,
    contextFrameClassId: String,
    deliverySettings: DeliverySettings,
    occurredOn: Long,
    aggregateIdentifier: String
)

case class DeliverySettings(
    title: String,
    startTime: String,
    endTime: String,
    allowLateSubmission: Boolean,
    stars: Int,
    randomized: Boolean
)