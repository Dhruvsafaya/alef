package com.alefeducation.schema.teacherTest

case class CandidateSessionRecorderMadeInProgressIntegrationEvent(
    id: String,
    candidateId: String,
    deliveryId: String,
    assessmentId: String,
    createdAt: String,
    status: String,
    occurredOn: Long,
    aggregateIdentifier: String
)

case class CandidateSessionRecorderMadeCompletedIntegrationEvent(
    id: String,
    candidateId: String,
    deliveryId: String,
    assessmentId: String,
    score: Float,
    awardedStars: Int,
    createdAt: String,
    updatedAt: String,
    status: String,
    occurredOn: Long,
    aggregateIdentifier: String
)

case class CandidateSessionRecorderArchivedIntegrationEvent(
    id: String,
    candidateId: String,
    deliveryId: String,
    assessmentId: String,
    updatedAt: String,
    createdAt: String,
    status: String,
    occurredOn: Long,
    aggregateIdentifier: String
)

case class AssessmentItem(
    questionId: String,
    questionCode: String,
    submissions: List[Submission],
    createdAt: String,
    updatedAt: String
)

case class Submission(
    attemptNumber: Int,
    questionVersion: Int,
    answer: Answer,
    result: Result,
    timeSpent: Double,
    hintUsed: Boolean,
    timestamp: String
)

case class Answer(
    `type`: String,
    selectedBlankChoices: List[SelectedBlankChoice],
    answerItems: List[AnswerItem],
    choiceIds: List[Int]
)

case class SelectedBlankChoice(
    blankId: Int,
    choiceId: Int
)

case class AnswerItem(
    blankId: Int,
    choiceId: Int,
    answer: String
)

case class Result(
    correct: Boolean,
    validResponse: ValidResponse,
    score: Float,
    answerResponse: AnswerResponse
)

case class ValidResponse(
    `type`: String,
    validBlankChoices: List[SelectedBlankChoice],
    answerMapping: List[AnswerMapping],
    choiceIds: List[Int]
)

case class AnswerResponse(
    correctAnswers: CorrectAnswers,
    wrongAnswers: List[WrongAnswers],
    unattendedCorrectAnswers: List[UnattendedCorrectAnswers]
)

case class UnattendedCorrectAnswers(
    `type`: String,
    selectedBlankChoices: List[SelectedBlankChoice],
    answerItems: List[AnswerItem],
    choiceIds: List[Int]
)

case class CorrectAnswers(
    `type`: String,
    selectedBlankChoices: List[SelectedBlankChoice],
    answerItems: List[AnswerItem],
    choiceIds: List[Int]
)

case class WrongAnswers(
    `type`: String,
    selectedBlankChoices: List[SelectedBlankChoice],
    answerItems: List[AnswerItem],
    choiceIds: List[Int]
)

case class AnswerMapping(
    blankId: Int,
    choiceId: Int,
    correctAnswer: String,
    alternateAnswers: List[String]
)
