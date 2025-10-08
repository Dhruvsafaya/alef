package com.alefeducation

import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.utils.StreamingUtils._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.scalatest.matchers.should.Matchers

class StreamingUtilsTest extends SparkSuite with Matchers {

  test("Test parseJsonColWithReadJson") {
    ///Arrange
    val session = spark
    import session.implicits._

    val jsonStr =
      """
          |[
            |{"value":{"headers":"{\"eventType\":\"StudentCreatedEvent\",\"tenantId\":\"93e4949d-7eff-4707-9201-dac917a5e013\"}","body":"{\"createdOn\":1604836429264,\"uuid\":\"3847858c-a71f-4fb0-a37a-5800f6864e0a\",\"username\":\"stu_xucet\",\"grade\":\"7\",\"gradeId\":\"a3216e9e-5cf0-44fd-8e7c-2a67b6a1d3d9\",\"sectionId\":\"b0ec877c-4388-42f3-87ae-b39f53662f2e\",\"schoolId\":\"a0fa04b1-90ba-46d3-8b64-32443e499928\",\"tags\":null,\"specialNeeds\":[]}"},"topic":"ninjas.alef.school.studentMutated","headers":{"eventType":"StudentCreatedEvent","tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"}},
            |{"value":{"headers":"{\"eventType\":\"StudentCreatedEvent\",\"tenantId\":\"93e4949d-7eff-4707-9201-dac917a5e013\"}","body":"{\"createdOn\":1604836712993,\"uuid\":\"51849e27-202f-46e2-bbed-7f86492b6108\",\"username\":\"stu_wofof\",\"grade\":\"7\",\"gradeId\":\"a3216e9e-5cf0-44fd-8e7c-2a67b6a1d3d9\",\"sectionId\":\"b0ec877c-4388-42f3-87ae-b39f53662f2e\",\"schoolId\":\"a0fa04b1-90ba-46d3-8b64-32443e499928\",\"tags\":null,\"specialNeeds\":[]}"},"topic":"ninjas.alef.school.studentMutated","headers":{"eventType":"StudentCreatedEvent","tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"}},
            |{"value":{"headers":"{\"eventType\":\"StudentCreatedEvent\",\"tenantId\":\"93e4949d-7eff-4707-9201-dac917a5e013\"}","body":"{\"createdOn\":1604836875980,\"uuid\":\"1827f74b-4b3c-4589-825d-81814c62216f\",\"username\":\"stu_suqow\",\"grade\":\"7\",\"gradeId\":\"a3216e9e-5cf0-44fd-8e7c-2a67b6a1d3d9\",\"sectionId\":\"b0ec877c-4388-42f3-87ae-b39f53662f2e\",\"schoolId\":\"a0fa04b1-90ba-46d3-8b64-32443e499928\",\"tags\":null,\"specialNeeds\":[]}"},"topic":"ninjas.alef.school.studentMutated","headers":{"eventType":"StudentCreatedEvent","tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"}},
            |{"value":{"headers":"{\"eventType\":\"StudentCreatedEvent\",\"tenantId\":\"93e4949d-7eff-4707-9201-dac917a5e013\"}","body":"{\"createdOn\":1604836981084,\"uuid\":\"08f79d2b-1f43-4626-8e81-c7866d8eb5ed\",\"username\":\"stu_toqan\",\"grade\":\"7\",\"gradeId\":\"a3216e9e-5cf0-44fd-8e7c-2a67b6a1d3d9\",\"sectionId\":\"b0ec877c-4388-42f3-87ae-b39f53662f2e\",\"schoolId\":\"a0fa04b1-90ba-46d3-8b64-32443e499928\",\"tags\":null,\"specialNeeds\":[]}"},"topic":"ninjas.alef.school.studentMutated","headers":{"eventType":"StudentCreatedEvent","tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"}},
            |{"value":{"headers":"{\"eventType\":\"StudentCreatedEvent\",\"tenantId\":\"93e4949d-7eff-4707-9201-dac917a5e013\"}","body":"{\"createdOn\":1604837109519,\"uuid\":\"cc98eca8-4e73-41c7-97a5-77fd08732930\",\"username\":\"stu_bihor\",\"grade\":\"7\",\"gradeId\":\"a3216e9e-5cf0-44fd-8e7c-2a67b6a1d3d9\",\"sectionId\":\"b0ec877c-4388-42f3-87ae-b39f53662f2e\",\"schoolId\":\"a0fa04b1-90ba-46d3-8b64-32443e499928\",\"tags\":null,\"specialNeeds\":[]}"},"topic":"ninjas.alef.school.studentMutated","headers":{"eventType":"StudentCreatedEvent","tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"}},
            |{"value":{"headers":"{\"eventType\":\"StudentCreatedEvent\",\"tenantId\":\"93e4949d-7eff-4707-9201-dac917a5e013\"}","body":"{\"createdOn\":1604837216235,\"uuid\":\"003c2c87-453a-4068-b44d-4a526e28ad16\",\"username\":\"stu_giyox\",\"grade\":\"7\",\"gradeId\":\"a3216e9e-5cf0-44fd-8e7c-2a67b6a1d3d9\",\"sectionId\":\"b0ec877c-4388-42f3-87ae-b39f53662f2e\",\"schoolId\":\"a0fa04b1-90ba-46d3-8b64-32443e499928\",\"tags\":null,\"specialNeeds\":[]}"},"topic":"ninjas.alef.school.studentMutated","headers":{"eventType":"StudentCreatedEvent","tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"}},
            |{"value":{"headers":"{\"eventType\":\"StudentCreatedEvent\",\"tenantId\":\"93e4949d-7eff-4707-9201-dac917a5e013\"}","body":"{\"createdOn\":1604837306115,\"uuid\":\"ad6443c6-e1cf-4625-a7b6-571cefc7d0f3\",\"username\":\"stu_pugih\",\"grade\":\"7\",\"gradeId\":\"a3216e9e-5cf0-44fd-8e7c-2a67b6a1d3d9\",\"sectionId\":\"b0ec877c-4388-42f3-87ae-b39f53662f2e\",\"schoolId\":\"a0fa04b1-90ba-46d3-8b64-32443e499928\",\"tags\":null,\"specialNeeds\":[]}"},"topic":"ninjas.alef.school.studentMutated","headers":{"eventType":"StudentCreatedEvent","tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"}}
          |]
          |""".stripMargin

    val df: DataFrame = spark.read.json(Seq(jsonStr).toDS)

    // Act
    val parsedDF = df.parseJsonColWithReadJson("value.body", "body")

    val expectedSchema = StructType.fromDDL(
      "`body` STRUCT<`createdOn`: BIGINT, `grade`: STRING, `gradeId`: STRING, `schoolId`: STRING, `sectionId`: STRING, `specialNeeds`: ARRAY<STRING>, `tags`: STRING, `username`: STRING, `uuid`: STRING>")

    // Assert
    parsedDF.select("body").schema shouldBe expectedSchema

  }

  test("Test parseJsonCol") {
    ///Arrange
    val session = spark
    import session.implicits._

    val jsonStr =
      """
        |[
        |{"value":{"headers":"{\"eventType\":\"StudentCreatedEvent\",\"tenantId\":\"93e4949d-7eff-4707-9201-dac917a5e013\"}","body":"{\"createdOn\":1604836429264,\"uuid\":\"3847858c-a71f-4fb0-a37a-5800f6864e0a\",\"username\":\"stu_xucet\",\"grade\":\"7\",\"gradeId\":\"a3216e9e-5cf0-44fd-8e7c-2a67b6a1d3d9\",\"sectionId\":\"b0ec877c-4388-42f3-87ae-b39f53662f2e\",\"schoolId\":\"a0fa04b1-90ba-46d3-8b64-32443e499928\",\"tags\":null,\"specialNeeds\":[]}"},"topic":"ninjas.alef.school.studentMutated","headers":{"eventType":"StudentCreatedEvent","tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"}},
        |{"value":{"headers":"{\"eventType\":\"StudentCreatedEvent\",\"tenantId\":\"93e4949d-7eff-4707-9201-dac917a5e013\"}","body":"{\"createdOn\":1604836712993,\"uuid\":\"51849e27-202f-46e2-bbed-7f86492b6108\",\"username\":\"stu_wofof\",\"grade\":\"7\",\"gradeId\":\"a3216e9e-5cf0-44fd-8e7c-2a67b6a1d3d9\",\"sectionId\":\"b0ec877c-4388-42f3-87ae-b39f53662f2e\",\"schoolId\":\"a0fa04b1-90ba-46d3-8b64-32443e499928\",\"tags\":null,\"specialNeeds\":[]}"},"topic":"ninjas.alef.school.studentMutated","headers":{"eventType":"StudentCreatedEvent","tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"}},
        |{"value":{"headers":"{\"eventType\":\"StudentCreatedEvent\",\"tenantId\":\"93e4949d-7eff-4707-9201-dac917a5e013\"}","body":"{\"createdOn\":1604836875980,\"uuid\":\"1827f74b-4b3c-4589-825d-81814c62216f\",\"username\":\"stu_suqow\",\"grade\":\"7\",\"gradeId\":\"a3216e9e-5cf0-44fd-8e7c-2a67b6a1d3d9\",\"sectionId\":\"b0ec877c-4388-42f3-87ae-b39f53662f2e\",\"schoolId\":\"a0fa04b1-90ba-46d3-8b64-32443e499928\",\"tags\":null,\"specialNeeds\":[]}"},"topic":"ninjas.alef.school.studentMutated","headers":{"eventType":"StudentCreatedEvent","tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"}},
        |{"value":{"headers":"{\"eventType\":\"StudentCreatedEvent\",\"tenantId\":\"93e4949d-7eff-4707-9201-dac917a5e013\"}","body":"{\"createdOn\":1604836981084,\"uuid\":\"08f79d2b-1f43-4626-8e81-c7866d8eb5ed\",\"username\":\"stu_toqan\",\"grade\":\"7\",\"gradeId\":\"a3216e9e-5cf0-44fd-8e7c-2a67b6a1d3d9\",\"sectionId\":\"b0ec877c-4388-42f3-87ae-b39f53662f2e\",\"schoolId\":\"a0fa04b1-90ba-46d3-8b64-32443e499928\",\"tags\":null,\"specialNeeds\":[]}"},"topic":"ninjas.alef.school.studentMutated","headers":{"eventType":"StudentCreatedEvent","tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"}},
        |{"value":{"headers":"{\"eventType\":\"StudentCreatedEvent\",\"tenantId\":\"93e4949d-7eff-4707-9201-dac917a5e013\"}","body":"{\"createdOn\":1604837109519,\"uuid\":\"cc98eca8-4e73-41c7-97a5-77fd08732930\",\"username\":\"stu_bihor\",\"grade\":\"7\",\"gradeId\":\"a3216e9e-5cf0-44fd-8e7c-2a67b6a1d3d9\",\"sectionId\":\"b0ec877c-4388-42f3-87ae-b39f53662f2e\",\"schoolId\":\"a0fa04b1-90ba-46d3-8b64-32443e499928\",\"tags\":null,\"specialNeeds\":[]}"},"topic":"ninjas.alef.school.studentMutated","headers":{"eventType":"StudentCreatedEvent","tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"}},
        |{"value":{"headers":"{\"eventType\":\"StudentCreatedEvent\",\"tenantId\":\"93e4949d-7eff-4707-9201-dac917a5e013\"}","body":"{\"createdOn\":1604837216235,\"uuid\":\"003c2c87-453a-4068-b44d-4a526e28ad16\",\"username\":\"stu_giyox\",\"grade\":\"7\",\"gradeId\":\"a3216e9e-5cf0-44fd-8e7c-2a67b6a1d3d9\",\"sectionId\":\"b0ec877c-4388-42f3-87ae-b39f53662f2e\",\"schoolId\":\"a0fa04b1-90ba-46d3-8b64-32443e499928\",\"tags\":null,\"specialNeeds\":[]}"},"topic":"ninjas.alef.school.studentMutated","headers":{"eventType":"StudentCreatedEvent","tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"}},
        |{"value":{"headers":"{\"eventType\":\"StudentCreatedEvent\",\"tenantId\":\"93e4949d-7eff-4707-9201-dac917a5e013\"}","body":"{\"createdOn\":1604837306115,\"uuid\":\"ad6443c6-e1cf-4625-a7b6-571cefc7d0f3\",\"username\":\"stu_pugih\",\"grade\":\"7\",\"gradeId\":\"a3216e9e-5cf0-44fd-8e7c-2a67b6a1d3d9\",\"sectionId\":\"b0ec877c-4388-42f3-87ae-b39f53662f2e\",\"schoolId\":\"a0fa04b1-90ba-46d3-8b64-32443e499928\",\"tags\":null,\"specialNeeds\":[]}"},"topic":"ninjas.alef.school.studentMutated","headers":{"eventType":"StudentCreatedEvent","tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"}}
        |]
        |""".stripMargin

    val df: DataFrame = spark.read.json(Seq(jsonStr).toDS)

    // Act
//    val parsedDF = df.parseJsonCol("value.body", "body")

    val expectedSchema = StructType.fromDDL(
      "`body` STRUCT<`createdOn`: BIGINT, `grade`: STRING, `gradeId`: STRING, `schoolId`: STRING, `sectionId`: STRING, `specialNeeds`: ARRAY<STRING>, `tags`: STRING, `username`: STRING, `uuid`: STRING>")

    // Assert
//    parsedDF.select("body").schema shouldBe expectedSchema

  }

}
