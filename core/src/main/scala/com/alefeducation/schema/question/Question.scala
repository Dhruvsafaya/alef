package com.alefeducation.schema.question

import java.sql.Timestamp

case class CurriculumOutcomes(
                               id: String,
                               name: String,
                               description: String,
                               curriculum: String,
                               grade: String,
                               subject: String
                             )

case class Question(
                     keywords: Seq[String],
                     resourceType: String,
                     summativeAssessment: Boolean,
                     difficultyLevel: String,
                     cognitiveDimensions: Seq[String],
                     knowledgeDimensions: String,
                     lexileLevel: String,
                     lexileLevels: List[String],
                     copyrights: Seq[String],
                     conditionsOfUse: Seq[String],
                     formatType: String,
                     author: String,
                     authoredDate: Timestamp,
                     curriculumOutcomes: Seq[CurriculumOutcomes],
                     skillId: String,
                     cefrLevel: String,
                     proficiency: String
                   )