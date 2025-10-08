package com.alefeducation.schema.ccl

case class Translation(languageCode: String, value: String)

case class OutcomeCreated(outcomeId: String,
                          outcomeType: String,
                          name: String,
                          description: String,
                          boardId: Long,
                          gradeId: Long,
                          subjectId: Long,
                          parentId: String,
                          translations: List[Translation],
                          occurredOn: Long)

case class OutcomeUpdatedEvent(outcomeId: String,
                               parentId: String,
                               name: String,
                               description: String,
                               translations: List[Translation],
                               occurredOn: Long)

case class OutcomeDeletedEvent(outcomeId: String, occurredOn: Long)

case class OutcomeSkillAttachEvent(outcomeId: String, skillId: String, occurredOn: Long)

case class OutcomeCategoryAttachEvent(outcomeId: String, categoryId: String, occurredOn: Long)
