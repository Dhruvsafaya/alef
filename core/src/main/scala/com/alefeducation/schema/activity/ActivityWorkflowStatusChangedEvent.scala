package com.alefeducation.schema.activity

final case class ActivityWorkflowStatusChangedEvent(lessonId: Long,
                                                    activityUuid: String,
                                                    code: String,
                                                    academicYear: String,
                                                    status: String,
                                                    publishedBefore: Boolean,
                                                    occurredOn: Long)
