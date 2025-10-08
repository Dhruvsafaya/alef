package com.alefeducation.schema.lps

case class ActivityProgressStatus(
                                   activityId: String,
                                   progressStatus: String,
                                   activityType: String
                                 )

case class CoreAdditionalResourcesAssignedEvent(
                                             id: String, // mongoDB id of the corresponding document
                                             learnerId: String,
                                             courseId: String,
                                             classId: String,
                                             courseType: String,
                                             academicYearTag: String,
                                             activities: Set[String],
                                             dueDateStart: String,
                                             dueDateEnd: String,
                                             assignedBy: String,
                                             assignedOn: String,
                                             occurredOn: String,
                                             uuid: String, // unique message identifier
                                             resourceInfo: List[ActivityProgressStatus]
                                           )
