package com.alefeducation.schema.teacherTest

case class GuidanceClass(id: String, materialType: String, materialId: String)

case class GuidanceLesson(id: String, classes: List[GuidanceClass])

case class Version(
    major: Int = 0,
    minor: Int = 0,
    revision: Int = 0
)

case class TestBlueprintCreatedIntegrationEvent(
    id: String,
    contextFrameClassId: String,
    contextFrameTitle: String,
    contextDomainId: String,
    contextFrameDescription: String,
    guidanceVariableLessons: List[GuidanceLesson],
    noOfQuestions: Int,
    guidanceType: String,
    guidanceVersion: Version,
    createdBy: String,
    createdAt: String,
    status: String,
    aggregateIdentifier: String,
    occurredOn: Long
)

case class TestBlueprintContextUpdatedIntegrationEvent(
    id: String,
    contextFrameClassId: String,
    contextFrameTitle: String,
    contextDomainId: String,
    updatedBy: String,
    updatedAt: String,
    status: String,
    aggregateIdentifier: String,
    occurredOn: Long
)

case class TestBlueprintGuidanceUpdatedIntegrationEvent(
    id: String,
    guidanceVariableLessons: List[GuidanceLesson],
    noOfQuestions: Int,
    guidanceType: String,
    guidanceVersion: Version,
    aggregateIdentifier: String,
    occurredOn: Long
)

case class TestBlueprintMadeReadyIntegrationEvent(
    id: String,
    contextFrameClassId: String,
    contextFrameTitle: String,
    contextDomainId: String,
    contextFrameDescription: String,
    guidanceVariableLessons: List[GuidanceLesson],
    noOfQuestions: Int,
    guidanceType: String,
    guidanceVersion: Version,
    updatedBy: String,
    updatedAt: String,
    status: String,
    aggregateIdentifier: String,
    occurredOn: Long
)

case class TestBlueprintPublishedIntegrationEvent(
    id: String,
    contextFrameClassId: String,
    contextFrameTitle: String,
    contextDomainId: String,
    guidanceVariableLessons: List[GuidanceLesson],
    noOfQuestions: Int,
    guidanceType: String,
    guidanceVersion: Version,
    updatedBy: String,
    updatedAt: String,
    createdBy: String,
    createdAt: String,
    publishedBy: String,
    publishedAt: String,
    status: String,
    aggregateIdentifier: String,
    occurredOn: Long
)

case class TestBlueprintDiscardedIntegrationEvent(
    id: String,
    updatedBy: String,
    updatedAt: String,
    status: String,
    aggregateIdentifier: String,
    occurredOn: Long
)

case class TestBlueprintArchivedIntegrationEvent(
                                                 id: String,
                                                 updatedBy: String,
                                                 updatedAt: String,
                                                 status: String,
                                                 occurredOn: Long,
                                                 aggregateIdentifier: String,
                                               )
