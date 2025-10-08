package com.alefeducation.bigdata.batch.delta.operations

import com.alefeducation.bigdata.batch.delta.{Alias, Delta, SCDContext}
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import org.scalatest.matchers.must.Matchers

class DeltaSCDTests extends SparkSuite with Matchers {

  import Delta.Transformations.{DeltaDataFrameTransformations, DeltaSCDContextTransformations}

  private def defaultMatchConditions(entity: String): String = s"""
    | ${Alias.Delta}.${entity}_id = ${Alias.Events}.${entity}_id
    | and
    | ${Alias.Delta}.${entity}_created_time <= ${Alias.Events}.${entity}_created_time
    """.stripMargin

  private val customMatchConditionsIn = "some_stuff = some_other_stuff"

  private def customMatchConditionsOut(entity: String): String = s"""
    | some_stuff = some_other_stuff
    | and
    | ${Alias.Delta}.${entity}_created_time <= ${Alias.Events}.${entity}_created_time
    """.stripMargin

  private def updateFields(entity: String): Map[String, String] = Map(
    s"${entity}_active_until" -> s"${Alias.Events}.${entity}_created_time"
  )

  test("DeltaSCD.buildMatchColumns :: defaults") {
    val entity = "testEntity"
    val sut = DeltaSCD.buildMatchColumns(entity)

    sut must equal(defaultMatchConditions(entity))
  }

  test("DeltaSCD.buildMatchColumns :: custom match conditions") {
    val entity = "testEntity"
    val sut = DeltaSCD.buildMatchColumns(entity, customMatchConditionsIn)
    sut must equal(customMatchConditionsOut(entity))
  }

  test("DeltaSCD.buildUpdateColumns") {
    val entity = "testEntity"
    val sut = DeltaSCD.buildUpdateColumns(entity)

    sut must equal(updateFields(entity))
  }

  test("Delta.Transformations.DeltaDataFrameTransformations.toSCD :: empty data frame") {
    val df = spark.emptyDataFrame
    val sut = df.toSCD()

    sut mustBe empty
  }

  test("Delta.Transformations.DeltaUpdateContextTransformations.toSink :: defaults") {
    val sinkName = "testSink"
    val entity = "testEntity"
    val df = spark.emptyDataFrame
    val sut = SCDContext(df, "", Nil).toSink(sinkName, entity)

    sut mustBe a[DeltaSCDSink]
    sut.df mustBe df
    sut.name mustBe sinkName
    sut.input mustBe df
    sut.matchConditions must equal(defaultMatchConditions(entity))
    sut.updateFields must equal(updateFields(entity))
    sut.create must equal(true)
  }

  test("Delta.Transformations.DeltaUpdateContextTransformations.toSink :: custom match conditions :: do not create ") {
    val sinkName = "testSink"
    val entity = "testEntity"
    val df = spark.emptyDataFrame
    val sut = SCDContext(df, customMatchConditionsIn, Nil).toSink(sinkName, entity, create = false)

    sut mustBe a[DeltaSCDSink]
    sut.df mustBe df
    sut.name mustBe sinkName
    sut.input mustBe df
    sut.matchConditions must equal(customMatchConditionsOut(entity))
    sut.updateFields must equal(updateFields(entity))
    sut.create must equal(false)
  }

}
