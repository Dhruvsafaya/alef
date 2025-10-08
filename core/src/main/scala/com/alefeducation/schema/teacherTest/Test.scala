package com.alefeducation.schema.teacherTest

case class TestCreatedIntegrationEvent(
    testId: String,
    testBlueprintId: String,
    items: List[Item],
    contextFrameClassId: String,
    contextFrameTitle: String,
    contextFrameDescription: String,
    contextDomainId: String,
    status: String,
    createdBy: String,
    createdAt: String,
    occurredOn: Long,
    aggregateIdentifier: String
)

case class TestContextUpdatedIntegrationEvent(
    testId: String,
    updatedBy: String,
    updatedAt: String,
    contextDomainId: String,
    contextFrameAreItemsShuffled: Boolean,
    occurredOn: Long,
    aggregateIdentifier: String
)

case class ItemReplacedIntegrationEvent(
    testId: String,
    items: List[Item],
    updatedBy: String,
    updatedAt: String,
    occurredOn: Long,
    aggregateIdentifier: String
)

case class ItemReorderedIntegrationEvent(
    testId: String,
    items: List[Item],
    updatedBy: String,
    updatedAt: String,
    occurredOn: Long,
    aggregateIdentifier: String
)

case class TestItemsRegeneratedIntegrationEvent(
    testId: String,
    items: List[Item],
    updatedAt: String,
    occurredOn: Long,
    aggregateIdentifier: String
)

case class TestPublishedIntegrationEvent(
    testId: String,
    testBlueprintId: String,
    items: List[Item],
    contextFrameClassId: String,
    contextFrameTitle: String,
    contextFrameDescription: String,
    contextDomainId: String,
    updatedBy: String,
    updatedAt: String,
    publishedBy: String,
    publishedAt: String,
    status: String,
    occurredOn: Long,
    aggregateIdentifier: String
)

case class TestDiscardedIntegrationEvent(
    testId: String,
    updatedBy: String,
    updatedAt: String,
    status: String,
    occurredOn: Long,
    aggregateIdentifier: String
)

case class TestArchivedIntegrationEvent(
    testId: String,
    updatedBy: String,
    updatedAt: String,
    status: String,
    occurredOn: Long,
    aggregateIdentifier: String,
)

case class Item(
    id: String,
    `type`: String
)
