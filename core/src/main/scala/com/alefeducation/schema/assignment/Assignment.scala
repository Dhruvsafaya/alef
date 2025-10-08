package com.alefeducation.schema.assignment

case class AssignmentMutatedEvent(id: String,
                                  `type`: String,
                                  title: String,
                                  description: String,
                                  maxScore: Double,
                                  attachment: Attachment,
                                  isGradeable: Boolean,
                                  schoolId: String,
                                  language: String,
                                  status: String,
                                  attachmentRequired: Boolean,
                                  commentRequired: Boolean,
                                  createdBy: String,
                                  updatedBy: String,
                                  createdOn: Long,
                                  updatedOn: Long,
                                  publishedOn: Long,
                                  metadata: MetaData,
                                  occurredOn: Long)

case class Attachment(fileId: String, fileName: String, path: String)

case class MetaData(keywords: List[String],
                    resourceType: String,
                    summativeAssessment: Boolean,
                    difficultyLevel: String,
                    cognitiveDimensions: List[String],
                    knowledgeDimensions: String,
                    lexileLevel: String,
                    copyrights: List[String],
                    conditionsOfUse: List[String],
                    formatType: String,
                    author: String,
                    authoredDate: String,
                    curriculumOutcomes: List[CurriculumOutcomes],
                    skillIds: List[String],
                    language: String)

case class CurriculumOutcomes(`type`: String,
                              id: String,
                              name: String,
                              description: String,
                              curriculum: String,
                              grade: String,
                              subject: String)

case class AssignmentInstanceMutatedEvent(id: String,
                                          assignmentId: String,
                                          dueOn: Long,
                                          allowLateSubmission: Boolean,
                                          teacherId: String,
                                          `type`: String,
                                          k12Grade: Int,
                                          schoolGradeId: String,
                                          classId: String,
                                          subjectId: String,
                                          sectionId: String,
                                          groupId: String,
                                          levelId: String,
                                          mloId: String,
                                          trimesterId: String,
                                          instructionalPlanId: String,
                                          createdOn: Long,
                                          updatedOn: Long,
                                          startOn: Long,
                                          students: List[StudentActive],
                                          occurredOn: Long,
                                          teachingPeriodId: String)

case class StudentActive(id: String, active: Boolean)

case class AssignmentDeletedEvent(id: String, occurredOn: Long)

case class AssignmentInstanceDeletedEvent(id: String, assignmentId: String, occurredOn: Long)

case class AssignmentSubmittedEvent(id: String,
                                    assignmentInstanceId: String,
                                    assignmentId: String,
                                    referrerId: String,
                                    `type`: String,
                                    studentId: String,
                                    teacherId: String,
                                    status: String,
                                    studentAttachment: Attachment,
                                    comment: String,
                                    createdOn: Long,
                                    updatedOn: Long,
                                    submittedOn: Long,
                                    returnedOn: Long,
                                    gradedOn: Long,
                                    evaluatedOn: Long,
                                    teacherResponse: TeacherResponse,
                                    resubmissionCount: Long,
                                    occurredOn: Long)

case class AssignmentInstanceStudentsUpdatedSchema(id: String, students: List[StudentActive], occurredOn: Long)

case class TeacherResponse(comment: String, score: Double, attachment: Attachment)

case class AssignmentResubmissionRequestedEvent(assignmentId: String,
                                                studentId: String,
                                                assignmentInstanceId: String,
                                                classId: String,
                                                mloId: String,
                                                message: String,
                                                teacherId: String,
                                                resubmissionCount: Long,
                                                occurredOn: Long)
