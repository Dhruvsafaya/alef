package com.alefeducation.schema.ccl

case class QuestionModifiedEvent(body: QuestionBody)

case class QuestionBody(questionId: String,
                        code: String,
                        version: Int,
                        `type`: String,
                        variant: String,
                        language: String,
                        body: String,
                        validation: String,
                        stage: String,
                        maxScore: Double,
                        metadata: String,
                        createdBy: String,
                        createdAt: Long,
                        updatedBy: String,
                        updatedAt: Long,
                        occurredOn: Long,
                        triggeredBy: String)
