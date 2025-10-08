package com.alefeducation.schema.adt

import org.scalatest.funspec.AnyFunSpec

class NextQuestionTest extends AnyFunSpec {
  describe("ADT events") {
    it("should create NextQuestion") {
      val nextQuestion = NextQuestion("1",
        occurredOn = 1456792,
        learningSessionId = "",
        studentId = "",
        questionPoolId = "",
        response = "",
        proficiency = 730923,
        standardError = 1,
        nextQuestion = "",
        timeSpent = 1,
        currentQuestion = "",
        intestProgress = 5,
        curriculumSubjectId =  1223,
        curriculumSubjectName = "",
        breakdown = "",
        language = "EN_GB",
        attempt = 1,
        grade = 1,
        gradeId = "",
        academicYear = 1,
        academicYearId = "",
        academicTerm = 1,
        classSubjectName = "math",
        skill = "listening"
      )

      assert(nextQuestion.attempt === 1)
    }
    
  }
}
