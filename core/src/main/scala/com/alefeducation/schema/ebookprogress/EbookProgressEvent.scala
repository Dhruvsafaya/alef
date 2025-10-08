package com.alefeducation.schema.ebookprogress

case class EbookProgressEvent(
    id: String,
    action: String,
    studentId: String,
    lessonId: String,
    contentId: String,
    contentHash: String,
    sessionId: String,
    experienceId: String,
    schoolId: String,
    gradeId: String,
    classId: String,
    materialType: String,
    academicYearTag: String,
    ebookMetaData: EbookMetaData,
    progressData: ProgressData,
    bookmark: Bookmark,
    highlight: Highlight,
    occurredOn: Long
)

case class EbookMetaData(
    title: String,
    totalPages: Int,
    hasAudio: Boolean
)

case class ProgressData(
    location: String,
    isLastPage: Boolean,
    status: String,
    timeSpent: Float
)

case class Bookmark(
    location: String
)

case class Highlight(
    location: String
)
