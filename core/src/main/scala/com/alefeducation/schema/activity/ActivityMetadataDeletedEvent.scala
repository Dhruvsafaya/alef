package com.alefeducation.schema.activity

final case class ActivityMetadataDeletedEvent(lessonId: Long,
                                              activityUuid: String,
                                              code: String,
                                              academicYear: String,
                                              occurredOn: Long)