package com.alefeducation.schema.guardian

case class TeacherFeedbackSentEvent(feedbackThreadId: String,
                                    feedbackType: String,
                                    responseEnabled: Boolean,
                                    guardianIds: List[String],
                                    teacherId: String,
                                    studentId: String,
                                    classId: String,
                                    occurredOn: Long)
case class TeacherMessageSentEvent(messageId: String, feedbackThreadId: String, isFirstOfThread: Boolean, occurredOn: Long)
case class GuardianFeedbackReadEvent(guardianId: String, feedbackThreadId: String, occurredOn: Long)
case class GuardianMessageSentEvent(messageId: String, feedbackThreadId: String, guardianId: String, occurredOn: Long)
case class TeacherMessageDeletedEvent(messageId: String, occurredOn: Long)
case class TeacherFeedbackDeletedEvent(feedbackThreadId: String, occurredOn: Long)
