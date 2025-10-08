package com.alefeducation.schema.activity

case class ActivityOutcomeEvent(lessonId: Long,
                                activityUuid: String,
                                outcomeId: String,
                                occurredOn: Long)