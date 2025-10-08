package com.alefeducation.schema.activity

final case class ActivityTemplateEvent(templateUuid: String,
                                 name: String,
                                 description: String,
                                 status: String,
                                 activityType: String,
                                 dateCreated: String,
                                 publisherName: String,
                                 publisherId: Long,
                                 publishedDate: String,
                                 mainComponents: List[Component],
                                 supportingComponents: List[Component],
                                 sideComponents: List[Component],
                                 occurredOn: Long)


final case class QuestionAttempts(attempts: Long, hints: Boolean)

final case class PerformanceConditions(max: Long, min: Long, releaseComponent: String)

final case class Component(abbreviation: String,
                           alwaysEnabled: Boolean,
                           assessmentsAttempts: Long,
                           completionNode: Boolean,
                           exitTicket: Boolean,
                           icon: String,
                           id: String,
                           maxRepeat: Long,
                           name: String,
                           order: Long,
                           passingScore: Long,
                           performanceConditions: List[PerformanceConditions],
                           questionAttempts: QuestionAttempts,
                           releaseCondition: String,
                           releaseConditions: List[String],
                           `type`: String)