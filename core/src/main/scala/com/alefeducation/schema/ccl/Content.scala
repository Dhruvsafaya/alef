package com.alefeducation.schema.ccl

case class ContentCreatedEvent(contentId: Long,
                               title: String,
                               description: String,
                               tags: String,
                               organisation: String,
                               fileName: String,
                               fileContentType: String,
                               fileSize: Long,
                               fileUpdatedAt: Long,
                               conditionOfUse: String,
                               knowledgeDimension: String,
                               difficultyLevel: String,
                               language: String,
                               lexicalLevel: String,
                               mediaType: String,
                               status: String,
                               contentLocation: String,
                               authoredDate: String,
                               createdAt: Long,
                               createdBy: Long,
                               contentLearningResourceTypes: List[String],
                               cognitiveDimensions: List[String],
                               copyrights: List[String],
                               occurredOn: Long)

case class ContentUpdatedEvent(contentId: Long,
                               title: String,
                               description: String,
                               tags: String,
                               organisation: String,
                               fileName: String,
                               fileContentType: String,
                               fileSize: Long,
                               fileUpdatedAt: Long,
                               conditionOfUse: String,
                               knowledgeDimension: String,
                               difficultyLevel: String,
                               language: String,
                               lexicalLevel: String,
                               mediaType: String,
                               status: String,
                               contentLocation: String,
                               authoredDate: String,
                               updatedAt: Long,
                               contentLearningResourceTypes: List[String],
                               cognitiveDimensions: List[String],
                               copyrights: List[String],
                               occurredOn: Long)

case class ContentDeletedEvent(contentId: Long, occurredOn: Long)

case class ContentPublishedEvent(
    contentId: Long,
    publishedDate: String,
    publisherId: Long,
    occurredOn: Long
)

case class ContentOutcomeAttachEvent(
    contentId: Long,
    outcomeId: String,
    occurredOn: Long
)

case class ContentSkillAttachedDetachedEvent(
    contentId: Long,
    skillId: String,
    occurredOn: Long
)
