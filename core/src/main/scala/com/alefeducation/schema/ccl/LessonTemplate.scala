package com.alefeducation.schema.ccl

case class LessonTemplateCreated(id: Long,
                                 frameworkId: Long,
                                 title: String,
                                 organisation: String,
                                 description: String,
                                 steps: List[Step],
                                 createdAt: Long,
                                 occurredOn: Long)

case class LessonTemplateUpdated(id: Long,
                                 frameworkId: Long,
                                 title: String,
                                 organisation: String,
                                 description: String,
                                 steps: List[Step],
                                 updatedAt: Long,
                                 occurredOn: Long)

case class Step(id: Long, displayName: String, stepId: String, abbreviation: String)
