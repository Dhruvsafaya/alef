package com.alefeducation.schema.certificate

case class CertificateAwardedEvent(certificateId: String,
                       studentId: String,
                       academicYearId: String,
                       academicYear: String,
                       gradeId: String,
                       classId: String,
                       awardedBy: String,
                       category: String,
                       purpose: String,
                       language: String,
                       occurredOn: Long)