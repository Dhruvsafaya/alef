package com.alefeducation.bigdata.batch.delta

import com.alefeducation.bigdata.batch.delta.DeltaSCDTypeII.writeData
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.util.Resources
import com.alefeducation.util.StringUtilities.replaceSpecChars
import org.apache.spark.SparkConf
import org.scalatest.matchers.must.Matchers
import org.scalatest.{Inside, OptionValues}

class DeltaSCDTypeIITests extends SparkSuite with Matchers with Inside with OptionValues {

  override def sparkConf: SparkConf = {
    super.sparkConf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
  }

  test("buildInsertConditions should create condition for insert statement") {

    val res = DeltaSCDTypeII.buildInsertConditions("tag", "delta.tag_id = events.tag_id")

    replaceSpecChars(res) mustBe replaceSpecChars(
      s"""
         |delta.tag_id = events.tag_id
         | AND
         |delta.tag_status = 1
         |
         | AND
         |delta.tag_created_time = events.tag_created_time
         |""".stripMargin)
  }

  test("buildInsertConditions should create condition for insert statement when entity is empty") {

    val res = DeltaSCDTypeII.buildInsertConditions("", "delta.id = events.id")

    replaceSpecChars(res) mustBe replaceSpecChars(
      s"""
         |delta.id = events.id
         | AND
         |delta.status = 1
         |
         | AND
         |delta.created_time = events.created_time
         |""".stripMargin)
  }

  test("getPrimaryStatement should create statement with id") {
    val res = DeltaSCDTypeII.getPrimaryStatement("tag", "delta.tag_second_id = events.tag_second_id", 1)

    replaceSpecChars(res) mustBe replaceSpecChars(
      s"""
         |delta.tag_second_id = events.tag_second_id
         | AND
         |delta.tag_status = 1
         |""".stripMargin)
  }

  test("getPrimaryStatement should create statement with default id") {
    val res = DeltaSCDTypeII.getPrimaryStatement("tag", "", 1)

    replaceSpecChars(res) mustBe replaceSpecChars(
      s"""
         |delta.tag_id = events.tag_id
         | AND
         |delta.tag_status = 1
         |""".stripMargin)
  }

  test("getPrimaryConditions should create statement with default id") {
    val res = DeltaSCDTypeII.getPrimaryConditions("tag", "")

    replaceSpecChars(res) mustBe replaceSpecChars(
      s"""
         |delta.tag_id = events.tag_id
         |""".stripMargin)
  }

  test("buildMatchConditions should create match conditions") {

    val res = DeltaSCDTypeII.buildMatchConditions("tag", "delta.tag_id = events.tag_id and delta.tag_second_id = events.tag_second_id")

    replaceSpecChars(res) mustBe replaceSpecChars(
      s"""
         |delta.tag_id = events.tag_id and delta.tag_second_id = events.tag_second_id
         | AND
         |delta.tag_status = 1
         |
         | AND
         |delta.tag_created_time < events.tag_created_time
         |""".stripMargin
    )
  }

  test("writeData should update data in delta") {

    val s = spark

    import s.sqlContext.implicits._
    // === Step 1: Create a test Delta table ===
    val tempPath = "tmp/delta-table-test"
    val initialData = Seq(
      (1, "Alice", 100),
      (2, "Bob", 200)
    ).toDF("id", "name", "score")

    initialData.write.format("delta").mode("overwrite").save(tempPath)

    // === Step 2: Prepare incoming DataFrame ===
    val newData = Seq(
      (1, "Alice", 150),
      (3, "Charlie", 300)
    ).toDF("id", "name", "score")

    // === Step 3: Define match condition & update fields ===
    val matchConditions = "Delta.id = Events.id"
    val updateFields = Map("score" -> "Events.score")
    val uniqueIdColumns = List("id")
    val insertConditionColumns = "Delta.id IS NULL" // Insert if ID does not exist

    // === Step 4: Run writeData function ===
    writeData(spark, newData, Resources.Resource("resource", Map("path" -> tempPath)),
      matchConditions, updateFields, uniqueIdColumns, insertConditionColumns)

    // === Step 5: Read & Verify Delta Table ===
    val resultDF = spark.read.format("delta").load(tempPath)
    val expectedData = Seq(
      (1, "Alice", 150),
      (2, "Bob", 200),
      (3, "Charlie", 300)
    ).toDF("id", "name", "score")

    assert(resultDF.collect().toSet == expectedData.collect().toSet)
  }
}
