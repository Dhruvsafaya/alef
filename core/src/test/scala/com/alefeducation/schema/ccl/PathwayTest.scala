package com.alefeducation.schema.ccl

import org.scalatest.funspec.AnyFunSpec

class PathwayTest extends AnyFunSpec {
  describe("Pathway events") {
    it("should create ActivityPlannedInCourse") {
      val activityPlannedInCourseSchema = ActivityPlannedInCourseSchema(
        activityId = ActivityId("a1",  1),
          "c1",
        "ct1",
        "lvl1",
        1,
        "2.0",
        CourseSettings("locked", true, true),
        "IN_REVIEW",
        "LEVEL",
        1234579,
        true,
        Nil,
        Metadata(
          tags = List( Tag("LIST", List("1"), List(ListTagAttributes("1", "grey", null)), "LIST" ))
        )
      )

      assert(activityPlannedInCourseSchema.courseId === "c1")
    }

    it("should create ActivityUnPlannedInCourse") {
      val activityUnPlannedInCourseSchema = ActivityUnPlannedInCourseSchema(
        activityId = ActivityId("a1",  1),
        "c1",
        "ct1",
        "lvl1",
        1,
        "2.0",
        "IN_REVIEW",
        "LEVEL",
        1234579,
        true,
        true,
        Nil,
        Metadata(
          tags = List(Tag("LIST", List("1"), List(ListTagAttributes("1", "grey", null)), "LIST"))
        )
      )

      assert(activityUnPlannedInCourseSchema.courseId === "c1")
    }

    it("should create ADTPlannedInCourseSchema") {
      val adtPlannedInCourse = ADTPlannedInCourseSchema(
        activityId = ActivityId("a1",  1),
        "c1",
        "ct1",
        5,
        PathwaySettings("locked", true, true),
        "lvl1",
        1,
        "2.0",
        "IN_REVIEW",
        "LEVEL",
        1234579
      )

      assert(adtPlannedInCourse.courseId === "c1")
    }

    it("should create ADTUnPlannedInCourseSchema") {
      val adtUnPlannedInCourse = ADTUnPlannedInCourseSchema(
        activityId = ActivityId("a1",  1),
        "c1",
        "ct1",
        "lvl1",
        1,
        "2.0",
        "IN_REVIEW",
        "LEVEL",
        1234579
      )

      assert(adtUnPlannedInCourse.courseId === "c1")
    }
  }
}
