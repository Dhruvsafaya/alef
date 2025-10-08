package com.alefeducation.schema.ccl

case class TermCreated(id: String,
                       number: Int,
                       curriculumId: Long,
                       academicYearId: Int,
                       organisationId: String,
                       startDate: String,
                       endDate: String,
                       weeks: List[Week],
                       createdOn: String,
                       occurredOn: Long,
                       contentRepositoryId: String)

case class Week(id: String, number: Int, startDate: String)
