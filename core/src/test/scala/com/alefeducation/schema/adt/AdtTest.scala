package com.alefeducation.schema.adt

import org.scalatest.funspec.AnyFunSpec

class AdtTest extends AnyFunSpec {
  describe("ADT events") {

    it("should create studentReport") {
      val studentReport = StudentReport(
        "1",
        occurredOn = 1456792,
        learningSessionId = "",
        studentId = "",
        questionPoolId = "",
        curriculumSubjectId =  1223,
        curriculumSubjectName = "",
        attempt = 1,
        finalGrade = 1,
        finalScore = 1,
        forecastScore = 2,
        finalResult = "",
        finalUncertainty = 1,
        finalCategory = "",
        framework = "",
        totalTimespent = 9,
        academicYear = 9,
        academicTerm = 8,
        testId = "",
        breakdown = "",
        finalStandardError = 1,
        language = "",
        schoolId = "",
        finalProficiency = 1,
        grade = 1,
        gradeId = "",
        academicYearId = "",
        secondaryResult = "",
        classSubjectName = "",
        skill = "READING"
       )

      assert(studentReport.attempt === 1)
      assert(studentReport.finalGrade === 1)
    }
    
  }
}
