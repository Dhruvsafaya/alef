package com.alefeducation.schema.admin

case class AcademicYearMutatedEventSchema(occurredOn: Long,
                                          uuid: String,
                                          `type`: String,
                                          status: String,
                                          startDate: Long,
                                          endDate: Long,
                                          schoolId: String,
                                          organization: String,
                                          createdBy: String,
                                          updatedBy: String,
                                          createdOn: Long,
                                          updatedOn: Long)