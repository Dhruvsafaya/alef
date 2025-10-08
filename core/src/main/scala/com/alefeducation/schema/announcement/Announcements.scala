package com.alefeducation.schema.announcement

final case class TeacherAnnouncement(announcementId: String,
                                     teacherId: String,
                                     classIds: List[String],
                                     studentIds: List[String],
                                     hasAttachment: Boolean,
                                     announcementType: String,
                                     occurredOn: Long)

final case class PrincipalAnnouncement(announcementId: String,
                                       principalId: String,
                                       schoolId: String,
                                       gradeIds: List[String],
                                       hasAttachment: Boolean,
                                       announcementType: String,
                                       occurredOn: Long,
                                       classIds: List[String],
                                       studentIds: List[String]
                                      )

final case class SuperintendentAnnouncement(announcementId: String,
                                            superintendentId: String,
                                            schoolIds: List[String],
                                            gradeIds: List[String],
                                            hasAttachment: Boolean,
                                            announcementType: String,
                                            occurredOn: Long)

final case class AnnouncementDeleted(announcementId: String,
                                     occurredOn: Long)
