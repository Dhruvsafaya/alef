package com.alefeducation.base

import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.service.DataSink
import com.alefeducation.util.Resources.Resource
import org.apache.spark.sql.SparkSession

class SparkBatchServiceWriterSpec extends SparkSuite {

  val service = new SparkBatchServiceWriter {
    override val name: String = "service"
    override val session: SparkSession = spark
    override val dateOffsetColumnPrefix: Option[String] = None

    override def prehook: Unit = {}

    override def posthook: Unit = {}
  }

  test("should load resource from config") {
    val resource = service.getSinkResource(DataSink("resource1", spark.emptyDataFrame))

    assert(resource === Some(Resource("resource1", Map("one" -> "one1", "two" -> "two2"))))
  }
}
