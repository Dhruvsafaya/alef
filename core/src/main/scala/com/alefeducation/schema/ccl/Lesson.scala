package com.alefeducation.schema.ccl

case class LessonOutcomeAttachEvent(lessonId: Long, lessonUuid: String, outcomeId: String, occurredOn: Long)

case class LessonSkillLinkEvent(lessonId: Long, lessonUuid: String, skillId: String, derived: Boolean, occurredOn: Long)

case class LessonContentAttachEvent(lessonId: Long, lessonUuid: String, contentId: Long, stepId: Long, stepUuid: String, occurredOn: Long)

case class LessonMutatedEvent(lessonId: Long,
                              lessonUuid: String,
                              code: String,
                              status: String,
                              description: String,
                              title: String,
                              boardId: Long,
                              gradeId: Long,
                              subjectId: Long,
                              skillable: Boolean,
                              mloFrameworkId: Int,
                              thumbnailFileName: String,
                              thumbnailContentType: String,
                              thumbnailFileSize: Long,
                              thumbnailLocation: String,
                              thumbnailUpdatedAt: Long,
                              userId: Long,
                              mloTemplateId: Long,
                              academicYear: String,
                              academicYearId: Long,
                              assessmentTool: String,
                              lessonType: String,
                              createdAt: Long,
                              updatedAt: Long,
                              themeIds: List[Long],
                              duration: Int,
                              lessonRoles: List[String],
                              occurredOn: Long,
                              organisation: String)

case class LessonDeletedEvent(lessonId: Long, lessonUuid: String, code: String, academicYear: String, occurredOn: Long)

case class LessonPublishedEvent(lessonId: Long,
                                lessonUuid: String,
                                publisherId: Long,
                                publishedDate: String,
                                publishedBefore: Boolean,
                                occurredOn: Long,
                                maxStars: Int,
                                lessonJSON: LessonJSON)

case class LessonJSON(code: String,
                      title: String,
                      description: String,
                      academic_year: String,
                      education_board: LessonEntity,
                      subject: LessonEntity,
                      grade: LessonEntity,
                      contents: List[LessonContent],
                      lessonType: String,
                      thumbnail: String,
                      link: String,
                      `type`: String)

case class LessonEntity(id: Long, name: String)

case class LessonContent(id: String, title: String, abbreviation: String, `type`: String, url: String, description: String)

case class LessonWorkflowStatusChangedEvent(lessonId: Long,
                                            lessonUuid: String,
                                            code: String,
                                            academicYear: String,
                                            status: String,
                                            publishedBefore: Boolean,
                                            occurredOn: Long)

case class LessonAssignmentAssociationEvent(lessonId: Long,
                                            lessonUuid: String,
                                            stepId: Long,
                                            stepUuid: String,
                                            assignmentId: String,
                                            occurredOn: Long)

case class LessonAssessmentRuleAssociationEvent(lessonId: Long,
                                                lessonUuid: String,
                                                stepId: Long,
                                                stepUuid: String,
                                                rule: AssessmentRule,
                                                occurredOn: Long)

case class AssessmentRule(id: String,
                          poolId: String,
                          poolName: String,
                          difficultyLevel: String,
                          resourceType: String,
                          questions: Int)