package com.alefeducation.util

import com.alefeducation.bigdata.commons.testutils.{SparkTest, TestBaseSpec}
import com.alefeducation.util.DataFrameEqualityUtils.{createDfFromJson, assertSmallDatasetEquality => assertDF}
import com.alefeducation.util.Resources.getConfigList
import com.typesafe.config.Config
import org.apache.spark.sql.functions.{col, explode, explode_outer, from_json, lit, schema_of_json}
import org.apache.spark.sql.types.{DoubleType, IntegerType, TimestampType}

class CustomTransformationsUtilityTest extends TestBaseSpec with SparkTest {

  import com.alefeducation.util.generic_transform.CustomTransformationsUtility._

  describe("test addColIfNotExists transformation") {
    val customTransformations: Seq[Config] = getConfigList("batch-test-config", "transformations")

    val json =
      """
        |[
        |{"col-1":"one","col-array":[1,2],"col-epoch":1582026527000,"occurredOn":"2023-10-03 15:21:00","id":1},
        |{"col-1":"two","col-array":[1,2],"col-epoch":1582026527000,"occurredOn":"2023-10-05 01:00:00","id":2},
        |{"col-1":"three","col-array":[],"col-epoch":1582026527000,"occurredOn":"2023-10-06 17:22:00","id":3}
        |]
      """.stripMargin

    val df = createDfFromJson(spark, json)
    val dfWith = df
      .withColumn("col-array", explode_outer(col("col-array")))
      .withColumn("col-double", lit(null).cast(DoubleType))
      .withColumn("col-int", lit(null).cast(IntegerType))
      .withColumn("occurredOn", col("occurredOn").cast(TimestampType))
      .withColumn("col-epoch", lit("2020-02-18 11:48:47.0").cast(TimestampType))
      .withColumnRenamed("col-epoch", "col-epoch-renamed")

    val resDf = applyCustomTransformations(customTransformations, df)

    assert(resDf.count() === 5)
    assertDF("entity", resDf, dfWith)
  }

  describe("test jsonStringToArrayStruct transformation") {
    val customTransformations: Seq[Config] = getConfigList("batch-test-config-for-jsonstring", "transformations")

    val json =
      """
        |[
        |{"col-1":"two","col-array":"[{\"id\":\"id1\",\"versionId\":\"v2id\"}]"}
        |]
      """.stripMargin

    val df = createDfFromJson(spark, json)
    val dfWith = df
      .withColumn("col-array",  from_json(col("col-array"), schema_of_json(lit("""[{"id":"id1","versionId":"v2id"}]"""))))

    val resDf = applyCustomTransformations(customTransformations, df)

    assert(resDf.count() === 1)
    assertDF("entity", resDf, dfWith)
  }
}
