package com.alefeducation.schema.newxapi

import java.sql.Timestamp

case class NewXAPIStatement(actor: Actor,
                            verb: Verb,
                            `object`: String,
                            result: Result,
                            context: Context,
                            timestamp: String)

case class Actor(objectType: String, account: Account)

case class Account(name: String, homePage: String)

case class Verb(id: String, display: String)

case class XAPIGenericObject(objectType: String)

case class AgentObject(objectType: String, account: Account)

case class ActivityObject(id: String, objectType: String, definition: Definition)

case class Result(score: Score)

case class Score(scaled: Double, raw: Double, min: Double, max: Double)

case class Definition(`type`: String, name: Map[String, String], description: Map[String, String])

case class Context(contextActivities: ContextActivities, extensions: ContextExtensions)

case class Id(id: String)

case class ContextActivities(parent: List[Map[String, String]], grouping: List[Map[String, String]], category: List[Id])

case class ContextExtensions(`http://alefeducation.com`: String)

// Teacher Activity Related Extensions
case class TeacherExtensions(role: String,
                             outsideOfSchool: Boolean,
                             userAgent: String,
                             tenant: TeacherDetails,
                             timestampLocal: String)

case class TeacherDetails(id: String, school: School)

case class School(id: String, grade: Grade, section: Section, subject: Subject)

case class Grade(id: String)

case class Section(id: String)

case class Subject(id: String, name: String)


// Guardian Activity Related Extensions
case class GuardianExtensions(role: String,
                              outsideOfSchool: Boolean,
                              device: String,
                              userAgent: String,
                              student: Student,
                              tenant: GuardianDetails,
                              timestampLocal: String)

case class GuardianDetails(id: String, school: GuardianSchool)

case class GuardianSchool(id: String)

case class Student(id: String)

// Student Activity Related Extensions
case class StudentExtensions(role: String,
                             attempt: Int,
                             experienceId: String,
                             learningSessionId: String,
                             outsideOfSchool: Boolean,
                             isCompletionNode: Boolean,
                             isFlexibleLesson: Boolean,
                             lessonPosition: String,
                             fromTime: BigDecimal,
                             toTime: BigDecimal,
                             userAgent: String,
                             tenant: StudentDetails,
                             timestampLocal: String)

case class StudentDetails(id: String, school: StudentSchool)

case class StudentSchool(id: String, academicYearId: String, grade: Grade, section: Section, subject: Subject, academicCalendar: AcademicCalendar)

case class AcademicCalendar(id: String, currentPeriod: CurrentPeriod)

case class CurrentPeriod(teachingPeriodId: String, teachingPeriodTitle: String)
