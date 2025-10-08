package com.alefeducation

import com.alefeducation.bigdata.commons.testutils.SparkSuite
import org.scalatest.matchers.should.Matchers

class AllRawEventsRoleSpec extends SparkSuite with Matchers {

  trait Setup {
    implicit val transformer: AllRawEvents = new AllRawEvents(spark)
  }

  val expectedColumns: Set[String] = Set(
    "eventType",
    "eventDateDw",
    "occurredOn",
    "name",
    "loadtime",
    "id",
    "tenantId",
    "permissions",
    "role"
  )

  test("handle RoleCreatedEvent events") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = "raw-events-source",
          value = """
                    |[
                    |   {
                    |      "key":"key1",
                    |      "value":{
                    |         "headers":{
                    |            "eventType":"RoleCreatedEvent",
                    |            "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"
                    |         },
                    |         "body":{
                    |            "role":{
                    |               "id":"fdd07b44-a094-4fb6-aadb-cbcfc8df09f7",
                    |               "name":"CreateEvents",
                    |               "description":"CreateEvents",
                    |               "permissions":[
                    |
                    |               ],
                    |               "predefined":false,
                    |               "granularPermissions":[
                    |                  "read:curriculum_editor",
                    |                  "read:aat.workspace",
                    |                  "accessCclModule:aat.workspace",
                    |                  "create:aat.workspace",
                    |                  "update:aat.workspace",
                    |                  "assign:aat.workspace",
                    |                  "delete:aat.workspace",
                    |                  "readAll:aat.workspace",
                    |                  "read:aat.pool",
                    |                  "read:aat.question",
                    |                  "accessCclModule:aat.question",
                    |                  "read:curriculum",
                    |                  "create:aat.question",
                    |                  "update:aat.question",
                    |                  "delete:aat.question",
                    |                  "publish:aat.question",
                    |                  "accessCclModule:aat.pool",
                    |                  "create:aat.pool",
                    |                  "update:aat.pool",
                    |                  "delete:aat.pool"
                    |               ],
                    |               "created":"2024-07-10T09:01:56.683555",
                    |               "updated":"2024-07-10T09:01:56.683555",
                    |               "organization":{
                    |                  "name":"8 Test 2 a2103251",
                    |                  "code":"a2103251"
                    |               },
                    |               "roleType":"CUSTOM",
                    |               "categoryId":"100f9d55-9248-485d-b679-11a04253b629",
                    |               "permissionToCategories":[
                    |
                    |               ],
                    |               "isCCLRole":true
                    |            },
                    |            "occurredOn":1720602116763,
                    |            "id":"fdd07b44-a094-4fb6-aadb-cbcfc8df09f7",
                    |            "name":"CreateEvents",
                    |            "permissions":[
                    |               "read:curriculum_editor",
                    |               "read:aat.workspace",
                    |               "accessCclModule:aat.workspace",
                    |               "create:aat.workspace",
                    |               "update:aat.workspace",
                    |               "assign:aat.workspace",
                    |               "delete:aat.workspace",
                    |               "readAll:aat.workspace",
                    |               "read:aat.pool",
                    |               "read:aat.question",
                    |               "accessCclModule:aat.question",
                    |               "read:curriculum",
                    |               "create:aat.question",
                    |               "update:aat.question",
                    |               "delete:aat.question",
                    |               "publish:aat.question",
                    |               "accessCclModule:aat.pool",
                    |               "create:aat.pool",
                    |               "update:aat.pool",
                    |               "delete:aat.pool"
                    |            ]
                    |         }
                    |      },
                    |      "timestamp":"2020-02-20 16:23:46.609"
                    |   }
                    |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val dfOpt = sinks.find(_.name == "role-sink").map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedColumns)

          dfOpt.map(_.collect().length) shouldBe Some(1)
          dfOpt.foreach { df =>
            val fst = df.first()
            fst.getAs[String]("tenantId") shouldBe "93e4949d-7eff-4707-9201-dac917a5e013"
            fst.getAs[String]("eventType") shouldBe "RoleCreatedEvent"
            fst.getAs[String]("id") shouldBe "fdd07b44-a094-4fb6-aadb-cbcfc8df09f7"
            fst.getAs[String]("name") shouldBe "CreateEvents"
          }
        }
      )
    }
  }

  test("handle RoleUpdatedEvent events") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = "raw-events-source",
          value = """
                    |[
                    |   {
                    |      "key":"key1",
                    |      "value":{
                    |         "headers":{
                    |            "eventType":"RoleUpdatedEvent",
                    |            "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"
                    |         },
                    |         "body":{
                    |            "role":{
                    |               "id":"fdd07b44-a094-4fb6-aadb-cbcfc8df09f7",
                    |               "name":"UpdateEvents",
                    |               "description":"UpdateEvents2",
                    |               "permissions":[
                    |
                    |               ],
                    |               "predefined":false,
                    |               "granularPermissions":[
                    |                  "read:curriculum_editor",
                    |                  "read:aat.workspace",
                    |                  "accessCclModule:aat.workspace",
                    |                  "create:aat.workspace",
                    |                  "update:aat.workspace",
                    |                  "assign:aat.workspace",
                    |                  "delete:aat.workspace",
                    |                  "readAll:aat.workspace",
                    |                  "read:aat.pool",
                    |                  "accessCclModule:aat.pool",
                    |                  "read:aat.question",
                    |                  "accessCclModule:aat.question",
                    |                  "read:curriculum",
                    |                  "create:aat.question",
                    |                  "update:aat.question",
                    |                  "delete:aat.question",
                    |                  "publish:aat.question",
                    |                  "create:aat.pool",
                    |                  "update:aat.pool",
                    |                  "delete:aat.pool"
                    |               ],
                    |               "created":"2024-07-10T09:01:56.683",
                    |               "updated":"2024-07-10T09:02:14.933783",
                    |               "organization":{
                    |                  "name":"8 Test 2 a2103251",
                    |                  "code":"a2103251"
                    |               },
                    |               "roleType":"CUSTOM",
                    |               "categoryId":"100f9d55-9248-485d-b679-11a04253b629",
                    |               "permissionToCategories":[
                    |
                    |               ],
                    |               "isCCLRole":true
                    |            },
                    |            "occurredOn":1720602134941,
                    |            "id":"fdd07b44-a094-4fb6-aadb-cbcfc8df09f7",
                    |            "name":"UpdatedEvents",
                    |            "permissions":[
                    |               "read:curriculum_editor",
                    |               "read:aat.workspace",
                    |               "accessCclModule:aat.workspace",
                    |               "create:aat.workspace",
                    |               "update:aat.workspace",
                    |               "assign:aat.workspace",
                    |               "delete:aat.workspace",
                    |               "readAll:aat.workspace",
                    |               "read:aat.pool",
                    |               "accessCclModule:aat.pool",
                    |               "read:aat.question",
                    |               "accessCclModule:aat.question",
                    |               "read:curriculum",
                    |               "create:aat.question",
                    |               "update:aat.question",
                    |               "delete:aat.question",
                    |               "publish:aat.question",
                    |               "create:aat.pool",
                    |               "update:aat.pool",
                    |               "delete:aat.pool"
                    |            ]
                    |         }
                    |      },
                    |      "timestamp":"2020-02-20 16:23:46.609"
                    |   }
                    |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val dfOpt = sinks.find(_.name == "role-sink").map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedColumns)

          dfOpt.map(_.collect().length) shouldBe Some(1)
          dfOpt.foreach { df =>
            val fst = df.first()
            fst.getAs[String]("tenantId") shouldBe "93e4949d-7eff-4707-9201-dac917a5e013"
            fst.getAs[String]("eventType") shouldBe "RoleUpdatedEvent"
            fst.getAs[String]("id") shouldBe "fdd07b44-a094-4fb6-aadb-cbcfc8df09f7"
            fst.getAs[String]("name") shouldBe "UpdatedEvents"
          }
        }
      )
    }
  }

  test("handle RoleDeletedEvent events") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = "raw-events-source",
          value = """
                    |[
                    |   {
                    |      "key":"key1",
                    |      "value":{
                    |         "headers":{
                    |            "eventType":"RoleDeletedEvent",
                    |            "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"
                    |         },
                    |         "body":{
                    |            "role":{
                    |               "id":"034b73d6-ee10-4e93-9d3e-ea5c1dad2503",
                    |               "name":"DeletedEvents",
                    |               "description":"DeletedEvents",
                    |               "permissions":[
                    |
                    |               ],
                    |               "predefined":false,
                    |               "granularPermissions":[
                    |                  "read:curriculum_editor",
                    |                  "read:aat.workspace",
                    |                  "accessCclModule:aat.workspace",
                    |                  "create:aat.workspace",
                    |                  "update:aat.workspace",
                    |                  "assign:aat.workspace",
                    |                  "delete:aat.workspace",
                    |                  "readAll:aat.workspace",
                    |                  "read:aat.pool",
                    |                  "read:aat.question",
                    |                  "accessCclModule:aat.question",
                    |                  "read:curriculum",
                    |                  "create:aat.question",
                    |                  "update:aat.question",
                    |                  "delete:aat.question",
                    |                  "publish:aat.question",
                    |                  "accessCclModule:aat.pool",
                    |                  "create:aat.pool",
                    |                  "update:aat.pool",
                    |                  "delete:aat.pool"
                    |               ],
                    |               "created":"2024-07-10T09:02:53.881",
                    |               "updated":"2024-07-10T09:02:53.881",
                    |               "organization":{
                    |                  "name":"8 Test 2 a2103251",
                    |                  "code":"a2103251"
                    |               },
                    |               "roleType":"CUSTOM",
                    |               "categoryId":"100f9d55-9248-485d-b679-11a04253b629",
                    |               "permissionToCategories":[
                    |
                    |               ],
                    |               "isCCLRole":true
                    |            },
                    |            "occurredOn":1720602177894,
                    |            "id":"034b73d6-ee10-4e93-9d3e-ea5c1dad2503"
                    |         }
                    |      },
                    |      "timestamp":"2020-02-20 16:23:46.609"
                    |   }
                    |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val dfOpt = sinks.find(_.name == "role-sink").map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedColumns)

          dfOpt.map(_.collect().length) shouldBe Some(1)
          dfOpt.foreach { df =>
            val fst = df.first()
            fst.getAs[String]("tenantId") shouldBe "93e4949d-7eff-4707-9201-dac917a5e013"
            fst.getAs[String]("eventType") shouldBe "RoleDeletedEvent"
            fst.getAs[String]("id") shouldBe "034b73d6-ee10-4e93-9d3e-ea5c1dad2503"
            fst.getAs[String]("name") shouldBe null
          }
        }
      )
    }
  }
}
