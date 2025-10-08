package com.alefeducation.schema.ccl

import java.util.UUID

case class QuestionPoolMutatedEvent(
                             triggeredBy: String,
                             name: String,
                             questionCodePrefix: String,
                             status: String,
                             createdBy: String,
                             updatedBy: String,
                             createdAt: Long,
                             updatedAt: Long,
                             occurredOn: Long,
                             poolId: String
                           )

case class QuestionsPoolAssociationEvent(
                                      triggeredBy: String,
                                      questions: List[QuestionCode],
                                      occurredOn: Long,
                                      poolId: String
                                    )

case class QuestionCode(code: String)
