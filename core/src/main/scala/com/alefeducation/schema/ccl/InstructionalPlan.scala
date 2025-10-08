package com.alefeducation.schema.ccl

case class InstructionalPlanPublished(id: String,
                                      name: String,
                                      code: String,
                                      academicYearName: String,
                                      curriculumId: Long,
                                      subjectId: Long,
                                      gradeId: Long,
                                      academicYearId: Int,
                                      organisationId: String,
                                      status: String,
                                      createdOn: String,
                                      items: List[Item],
                                      occurredOn: Long,
                                      contentRepositoryId: String)

case class InstructionalPlanRePublished(id: String,
                                        name: String,
                                        code: String,
                                        academicYearName: String,
                                        curriculumId: Long,
                                        subjectId: Long,
                                        gradeId: Long,
                                        academicYearId: Int,
                                        organisationId: String,
                                        status: String,
                                        createdOn: String,
                                        items: List[Item],
                                        occurredOn: Long,
                                        contentRepositoryId: String)

case class InstructionalPlanDeleted(id: String, items: List[String], occurredOn: Long)

case class Item(itemType: String, checkpointUuid: String, order: Long, weekId: String, lessonId: Long, lessonUuid: String, optional: Boolean, instructorLed: Boolean, defaultLocked: Boolean)
