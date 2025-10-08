package com.alefeducation.bigdata.batch.delta

import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.util.Resources
import com.alefeducation.util.StringUtilities.replaceSpecChars
import org.apache.spark.SparkConf
import org.scalatest.matchers.must.Matchers

class DeltaInsertIfNotExistsTests extends SparkSuite with Matchers {

  override def sparkConf: SparkConf = {
    super.sparkConf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
  }

  test("buildMatchConditions should create match conditions") {
    val res = DeltaInsertIfNotExists.buildMatchConditions("tag", "delta.tag_id = events.tag_id")

    replaceSpecChars(res) mustBe replaceSpecChars(
      s"""
         |delta.tag_id = events.tag_id
         |""".stripMargin)
  }

  test("writeData should update data only if it is not exists") {
    val s = spark

    import s.sqlContext.implicits._
    // === Step 1: Create a test Delta table ===
    val tempPath = "tmp/delta-table-test-0001"
    val initialData = Seq(
      (1, "Alice", 100, "0"),
      (2, "Bob", 200, "1")
    ).toDF("id", "name", "score", "outdated_col")

    initialData.write.format("delta").mode("overwrite").save(tempPath)

    // === Step 2: Prepare incoming DataFrame ===
    val newData = Seq(
      (3, "Charlie", 300), // New row (should be inserted)
      (4, "David", 400)  ,  // New row (should be inserted)
      (2, "Bob", 200), // should be ignored because already present,
      (1, "Alice", 100) // should be ignored because already present,
    ).toDF("id", "name", "score")

    val matchConditions = "delta.id = events.id"

    // === Step 3: Run `write` method ===
    new DeltaInsertIfNotExistsSink("sinkName", newData, matchConditions)
      .write(Resources.Resource("resource", Map("path" -> tempPath)))

    // === Step 4: Read & Verify Delta Table ===
    val resultDF = spark.read.format("delta").load(tempPath)

    val expectedData = Seq(
      (1, "Alice", 100, "0"),  // Existing data (should remain unchanged)
      (2, "Bob", 200, "1"),    // Existing data (should remain unchanged)
      (3, "Charlie", 300, null), // New data (should be inserted)
      (4, "David", 400, null)    // New data (should be inserted)
    ).toDF("id", "name", "score", "outdated_col")

    assert(resultDF.collect().toSet == expectedData.collect().toSet)
  }

  test("Delta.Transformations.DeltaDataFrameTransformations.toInsertIfNotExists :: do not transform") {
    import Delta.Transformations.DeltaDataFrameTransformations
    val s = spark
    import s.sqlContext.implicits._

    val df = List(Tuple1("someValue")).toDF("someColumn")

    val expectedMatchConditions = "a = b"
    val expectedFilterNot = List("d", "c")

    val context = df.toInsertIfNotExists(
      matchConditions = expectedMatchConditions,
      filterNot = expectedFilterNot
    )

    val expectedInsertIfNotExistsContext = InsertIfNotExistsContext(
      df = df,
      matchConditions = expectedMatchConditions,
      filterNot = expectedFilterNot
    )
    context mustBe expectedInsertIfNotExistsContext
  }
}
