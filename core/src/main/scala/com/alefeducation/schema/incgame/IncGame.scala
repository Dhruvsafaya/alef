package com.alefeducation.schema.incgame

case class IncGameEvent(id: String,
                        gameId: String,
                        occurredOn: String,
                        schoolId: String,
                        classId: String,
                        sectionId: String,
                        mloId: String,
                        title: String,
                        teacherId: String,
                        subjectId: String,
                        schoolGradeId: String,
                        genSubject: String,
                        k12Grade: Int,
                        groupId: String,
                        learningPathId: String,
                        questions: List[IncGameQuestion],
                        instructionalPlanId: String,
                        materialId: String,
                        materialType: String,
                        isFormativeAssessment: Boolean,
                        lessonComponentId: String)

case class IncGameSessionEvent(id: String,
                               sessionId: String,
                               occurredOn: String,
                               gameId: String,
                               gameTitle: String,
                               players: List[String],
                               joinedPlayers: List[String],
                               questions: List[IncGameQuestion],
                               joinedPlayersDetails: List[IncGameJoinedPlayerDetail],
                               startedOn: String,
                               startedBy: String,
                               updatedAt: String,
                               status: String,
                               isFormativeAssessment: Boolean,
                               lessonComponentId: String)

case class IncGameOutcomeEvent(id: String,
                               outcomeId: String,
                               occurredOn: String,
                               sessionId: String,
                               gameId: String,
                               playerId: String,
                               mloId: String,
                               assessmentId: String,
                               scoreBreakdown: List[IncGameScoreBreakdown],
                               score: Double,
                               status: String,
                               createdAt: String,
                               updatedAt: String,
                               isFormativeAssessment: Boolean,
                               lessonComponentId: String)

case class IncGameScoreBreakdown(code: String, score: Double, timeSpent: Int, createdAt: String, updatedAt: String, questionStatus: String)

case class IncGameQuestion(code: String, time: Int)
case class IncGameJoinedPlayerDetail(id: String, status: String)
