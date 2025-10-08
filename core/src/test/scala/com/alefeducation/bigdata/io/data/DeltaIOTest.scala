package com.alefeducation.bigdata.io.data

import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.io.data.Delta
import io.delta.tables.DeltaTable
import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.matchers.should.Matchers

import java.nio.file.{Files, Paths}

class DeltaIOTest extends SparkSuite with Matchers {

  override def sparkConf: SparkConf = {
    super.sparkConf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
  }

  test("isTableExists should return false if table does not exist") {
    val result = Delta.isTableExists(spark, "/tmp/non_existent_table")
    result shouldBe false
  }

  test("isTableExists should return true if table exists") {
    val path = "/tmp/delta_test_table"
    deleteFolderIfExists(path)
    val s = spark
    import s.sqlContext.implicits._
    // Create an empty Delta table
    val df = Seq((1, "test")).toDF("id", "name")
    df.write.format("delta").save(path)

    val result = Delta.isTableExists(spark, path)
    result shouldBe true
  }

  test("createEmptyTable should create an empty Delta table") {
    val path = "/tmp/delta_empty_table"
    deleteFolderIfExists(path)

    val schema = StructType(Seq(
      StructField("id", IntegerType, false),
      StructField("name", StringType, true)
    ))

    val df = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)

    Delta.createEmptyTable(df, path, Seq.empty)

    val table = DeltaTable.forPath(spark, path)
    table.toDF.count() shouldBe 0
  }

  def deleteFolderIfExists(path: String): Unit = {
    val directory = Paths.get(path)
    if (Files.exists(directory)) {
      import scala.reflect.io.Directory
      new Directory(directory.toFile).deleteRecursively()
    }
  }

  test("getDeltaTable should return a DeltaTable object") {
    val s = spark
    import s.sqlContext.implicits._
    val path = "/tmp/delta_table"
    deleteFolderIfExists(path)
    val df = Seq((1, "test")).toDF("id", "name")
    df.write.format("delta").save(path)

    val deltaTable = Delta.getDeltaTable(spark, path)
    deltaTable shouldBe a[DeltaTable]
  }

  test("upsert should merge records into a Delta table") {
    val s = spark
    import s.sqlContext.implicits._
    val path = "/tmp/delta_upsert"
    deleteFolderIfExists(path)

    // Create initial Delta table
    val initialDF = Seq((1, "Alice"), (2, "Bob")).toDF("id", "name")
    initialDF.write.format("delta").save(path)

    val deltaTable = DeltaTable.forPath(spark, path)

    // New data with an update and an insert
    val newDF = Seq((1, "Alice Updated"), (3, "Charlie")).toDF("id", "name")

    Delta.upsert(
      deltaTable = deltaTable,
      dataFrame = newDF,
      matchConditions = "delta.id = events.id",
      columnsToUpdate = Map("name" -> "events.name"),
      columnsToInsert = Map("id" -> "events.id", "name" -> "events.name"),
      updateConditions = None
    )

    val result = DeltaTable.forPath(spark, path).toDF.collect()

    result should contain allElementsOf Seq(
      Row(1, "Alice Updated"),
      Row(2, "Bob"),
      Row(3, "Charlie")
    )
  }

  test("update should modify records in a Delta table") {
    val s = spark
    import s.sqlContext.implicits._
    val path = "/tmp/delta_update"
    deleteFolderIfExists(path)

    // Create initial Delta table
    val initialDF = Seq((1, "Alice"), (2, "Bob")).toDF("id", "name")
    initialDF.write.format("delta").save(path)

    val deltaTable = DeltaTable.forPath(spark, path)

    // Update operation
    Delta.update(
      deltaTable = deltaTable,
      matchConditions = "id = 1",
      columnsToUpdate = Map("name" -> "'Alice Updated'")
    )

    val result = DeltaTable.forPath(spark, path).toDF.collect()

    result should contain allElementsOf Seq(
      Row(1, "Alice Updated"),
      Row(2, "Bob")
    )
  }
}
