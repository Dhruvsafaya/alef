package com.alefeducation.dimensions.class_.redshift

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.dimensions.BaseDimensionSpec
import com.alefeducation.dimensions.class_.transform.ClassModifiedTransform.classModifiedTransformSink
import com.alefeducation.dimensions.class_.transform.ClassUserTransform.classUserTransformSink
import com.alefeducation.models.ClassModel.DimClass
import com.alefeducation.service.DataSink
import com.alefeducation.util.DataFrameUtility.setScdTypeIIPostAction
import org.apache.spark.sql.SparkSession
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

class ClassRedshiftSpec extends SparkSuite with BaseDimensionSpec {

  val sprk: SparkSession = spark
  val service: SparkBatchService = mock[SparkBatchService]

  test("Should transform class dw id mapping to save into Redshift") {
    val sprk = spark
    import sprk.implicits._

    val inputValue = """{"eventType": "ClassCreatedEvent"}"""
    val inputDF = spark.read.json(Seq(inputValue).toDS())

    val transformer = new ClassDwIdMappingRedshift(sprk, service)
    when(service.readUniqueOptional(ClassDwIdMappingRedshift.classDwIdMappingSourceName, sprk, uniqueColNames = Seq("id", "entity_type")))
      .thenReturn(Some(inputDF))

    val sink = transformer.transform().get.asInstanceOf[DataSink]

    val expectedPostaction =
      """
        |BEGIN TRANSACTION;
        |
        |
        |MERGE INTO rs_stage_schema.rel_dw_id_mappings
        |USING rs_stage_schema.staging_rel_dw_id_mappings AS temp_table
        |ON (rel_dw_id_mappings.id = temp_table.id AND rel_dw_id_mappings.entity_type='class')
        |WHEN MATCHED THEN UPDATE SET entity_dw_created_time = temp_table.entity_dw_created_time
        |WHEN NOT MATCHED THEN INSERT (id, entity_type, entity_created_time, entity_dw_created_time) VALUES (temp_table.id, temp_table.entity_type, temp_table.entity_created_time, temp_table.entity_dw_created_time);
        |
        |
        |DROP TABLE rs_stage_schema.staging_rel_dw_id_mappings;
        |
        |END TRANSACTION;
        |""".stripMargin

    assert(sink.options("postactions") === expectedPostaction)
  }

  test("Should transform class modified to save into Redshift") {
    val sprk = spark
    import sprk.implicits._

    val inputValue = """{"eventType": "ClassModifiedEvent"}"""
    val inputDF = spark.read.json(Seq(inputValue).toDS())

    val transformer = new ClassModifiedRedshift(sprk, service)
    when(service.readOptional(classModifiedTransformSink, sprk)).thenReturn(Some(inputDF))

    val sink = transformer.transform().get.asInstanceOf[DataSink]

    assert(sink.options === setScdTypeIIPostAction(inputDF, "class"))
  }

  test("Should transform class user to save into Redshift") {
    val sprk = spark
    import sprk.implicits._

    val inputValue = """{"eventType": "ClassModifiedEvent", "classId": "TestClass"}"""
    val inputDF = spark.read.json(Seq(inputValue).toDS())

    val transformer = new ClassUserRedshift(sprk, service)
    when(service.readOptional(classUserTransformSink, sprk)).thenReturn(Some(inputDF))

    val sink = transformer.transform().get.asInstanceOf[DataSink]

    assert(sink.df.columns.toSet === Set("classId"))
  }

}
