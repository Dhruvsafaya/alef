package com.alefeducation.util

import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.schema.Schema
import com.alefeducation.util.Constants._
import com.alefeducation.util.DataFrameUtility._
import com.alefeducation.util.StringUtilities.replaceSpecChars
import com.amazon.redshift.util.RedshiftException
import org.apache.spark.SparkException
import org.apache.spark.sql._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.mockito.Mockito
import org.mockito.Mockito.{times, verify}
import org.scalatestplus.mockito.MockitoSugar.mock

import java.sql.{SQLException, Timestamp}
import java.util.TimeZone

class DataFrameUtilityTest extends SparkSuite {

  test("should add timestamp column to input dataFrame for user dimension") {
    val session: SparkSession = spark
    import session.implicits._
    val df: DataFrame = List("2017-07-14 02:40:00.0").toDF("occurredOn")
    val timestampColumns = addTimestampColumns(df, "guardian")
    val expectedColumnNames = List("guardian_created_time",
                                   "guardian_updated_time",
                                   "guardian_dw_created_time",
                                   "guardian_dw_updated_time",
                                   "guardian_deleted_time")
    assert(timestampColumns.columns.toSet === expectedColumnNames.toSet)
    assert(timestampColumns.head.getAs[TimestampType]("guardian_created_time").toString === "2017-07-14 02:40:00.0")
    assert(timestampColumns.head.getAs[TimestampType]("guardian_updated_time") === null)
    assert(timestampColumns.head.getAs[TimestampType]("guardian_deleted_time") === null)
  }

  test("should add timestamp column to input dataFrame for facts") {
    val session: SparkSession = spark
    import session.implicits._
    val df: DataFrame = List("2017-07-14 02:40:00.0").toDF("occurredOn")
    val timestampColumns = addTimestampColumns(df, "fact", true)
    val expectedColumnNames = List("fact_created_time", "fact_dw_created_time")
    assert(timestampColumns.columns.toSet === expectedColumnNames.toSet)
    assert(timestampColumns.head.getAs[TimestampType]("fact_created_time").toString === "2017-07-14 02:40:00.0")
  }

  test("should test process method to add occurredOn column with timestamp datatype") {
    val session: SparkSession = spark
    import session.implicits._
    val loginBody =
      """
          |{
          |"createdOn":"2018-09-08T04:09:10.861",
          |"uuid":"grade-id",
          |"schools":[],
          |"roles":["ADMIN"],
          |"outsideOfSchool":false}""".stripMargin
    val df: DataFrame = List((UserLogin, "tenant-id", loginBody, "1970-07-14 02:40:00.0"))
      .toDF("eventType", "tenantId", "body", "loadtime")
    val dateField = DateField("createdOn", TimestampType)
    val test = process(session, df, Schema.userLoggedIn, List(UserLogin), dateField)
    assert(test.head.getAs[TimestampType]("occurredOn").toString === "2018-09-08 04:09:10.861")
  }

  test("should test process method to add createdOn long type converted to UTC time for occurredOn column") {
    val session: SparkSession = spark
    import session.implicits._
    val loginBody =
      """
          |{
          |"createdOn":"1555335635873",
          |"uuid":"grade-id",
          |"schools":[],
          |"roles":["ADMIN"],
          |"outsideOfSchool":false}""".stripMargin
    val df: DataFrame = List((UserLogin, "tenant-id", loginBody, "1970-07-14 02:40:00.0"))
      .toDF("eventType", "tenantId", "body", "loadtime")
    val dateField = DateField("createdOn", LongType)
    val test = process(session, df, Schema.userLoggedIn, List(UserLogin), dateField)
    assert(test.head.getAs[StringType]("occurredOn").toString === "2019-04-15 13:40:35.873")
  }

  test("should add updatedTime column with lag") {
    val session: SparkSession = spark
    import session.implicits._
    val batchDf = Seq(
      ("FINALSAI1", "61fc064910180800ebef1b24", null, null, "2022-02-03 18:09:00.0"),
      ("FINALSAI1", "61fc064910180800ebef1b24", null, null, "2022-02-03 18:08:00.0"),
      ("FINALSAI1", "61fc064910180800ebef1b24", null, null, "2022-02-03 18:07:00.0")
    ).toDF(
      "questionCode",
      "poolId",
      "question_pool_association_updated_time",
      "question_pool_association_dw_updated_time",
      "occurredOn",
    )

    val dfWithUpdatedTimeCol =
      addUpdatedTimeColsForAssociation("question_pool_association", "occurredOn", List("poolId", "questionCode"))(batchDf)

    assert(dfWithUpdatedTimeCol.head.getAs[TimestampType]("question_pool_association_updated_time") === null)
    assert(
      dfWithUpdatedTimeCol
        .collectAsList()
        .get(1)
        .getAs[TimestampType]("question_pool_association_updated_time")
        .toString === "2022-02-03 18:09:00.0")
    assert(
      dfWithUpdatedTimeCol
        .collectAsList()
        .get(2)
        .getAs[TimestampType]("question_pool_association_updated_time")
        .toString === "2022-02-03 18:08:00.0")
  }

  test("should create query to fetch earliest rows based on time") {

    val expectedQueryString =
      """select * from
                                  |testalefdw.question_pool_association
                                  |where concat(concat(concat(question_pool_uuid, question_pool_association_question_code), question_pool_title), cast(question_pool_association_created_time as varchar)) in
                                  |(select id
                                  |from (select concat(concat(concat(question_pool_uuid, question_pool_association_question_code), question_pool_title), cast(question_pool_association_created_time as varchar)) id,
                                  |dense_rank()
                                  |over (partition by
                                  |question_pool_uuid, question_pool_association_question_code, question_pool_title
                                  |order by question_pool_association_created_time) rnk
                                  |from testalefdw.question_pool_association) a
                                  |where a.rnk = 1);""".stripMargin.trim
        .replaceAll(" +", " ")
        .replaceAll("\\R", " ")
    val entity = "question_pool_association"
    val dfWithUpdatedTimeCol = getEarliestRowsInRedshift("testalefdw",
                                                         entity,
                                                         entity,
                                                         List(s"question_pool_uuid", s"${entity}_question_code", "question_pool_title"))
    assert(dfWithUpdatedTimeCol == expectedQueryString)
  }

  test("should add _headers column in payload if includeHeaders is true") {
    val session: SparkSession = spark
    import session.implicits._
    val loginBody =
      """
          |{
          |"createdOn":"2018-09-08T04:09:10.861",
          |"uuid":"grade-id",
          |"schools":[],
          |"roles":["ADMIN"],
          |"outsideOfSchool":false}""".stripMargin
    val df: DataFrame = List((UserLogin, "tenant-id", loginBody, "1970-07-14 02:40:00.0", ""))
      .toDF("eventType", "tenantId", "body", "loadtime", "_headers")

    val dateField = DateField("createdOn", TimestampType)
    val test = process(session, df, Schema.userLoggedIn, List(UserLogin), dateField, includeHeaders = true)

    assert(test.columns.contains("_headers") === true)
  }

  test("should filter published events") {
    val arrayStructData = Seq(
      Row("body1", "2023-01-21 00:00:00", List(Row("COURSE_STATUS", "PUBLISHED"))),
      Row("body2", "2023-01-21 00:00:00", List(Row("COURSE_STATUS", "IN_REVIEW")))
    )

    val arrayStructSchema = new StructType()
      .add("value", StringType)
      .add("timestamp", StringType)
      .add("_headers",
           ArrayType(
             new StructType()
               .add("key", StringType)
               .add("value", StringType)))

    val df = spark.createDataFrame(spark.sparkContext
                                       .parallelize(arrayStructData),
                                     arrayStructSchema)

    val publishedEvents = filterPublished(df)
    assert(publishedEvents.count() === 1)
    assert(publishedEvents.head().getAs[String]("value") === "body1")
  }

  test("should filter in-review events") {
    val arrayStructData = Seq(
      Row("body1", "2023-01-21 00:00:00", List(Row("COURSE_STATUS", "PUBLISHED"))),
      Row("body2", "2023-01-21 00:00:00", List(Row("COURSE_STATUS", "IN_REVIEW")))
    )

    val arrayStructSchema = new StructType()
      .add("value", StringType)
      .add("timestamp", StringType)
      .add("_headers",
           ArrayType(
             new StructType()
               .add("key", StringType)
               .add("value", StringType)))

    val df = spark.createDataFrame(spark.sparkContext
                                       .parallelize(arrayStructData),
                                     arrayStructSchema)

    val publishedEvents = filterInReview(df)
    assert(publishedEvents.count() === 1)
    assert(publishedEvents.head().getAs[String]("value") === "body2")
  }

  test("handleSqlException should catch SQLException with code 1023 and try to run one more time until succeed") {
    val queryFunc = mock[Runnable]
    Mockito
      .when(queryFunc.run())
      .thenAnswer(_ => throw new SQLException("test exception", "exception", 1023))
      .thenAnswer(_ => ())

    handleSqlException(queryFunc.run(), 3)

    verify(queryFunc, times(2)).run()
  }

  test("handleSqlException should catch RedshiftException with code 1023 and try to run one more time until succeed") {
    val queryFunc = mock[Runnable]
    Mockito
      .when(queryFunc.run())
      .thenAnswer(_ => {
        throw new RedshiftException("ERROR: 1023", new SQLException("test exception", "exception", 1023))
      })
      .thenAnswer(_ => ())

    handleSqlException(queryFunc.run(), 3)

    verify(queryFunc, times(2)).run()
  }

  test("handleSqlException should rethrow other exception") {
    val queryFunc = mock[Runnable]
    Mockito
      .when(queryFunc.run())
      .thenAnswer(_ => throw new RuntimeException("exception"))

    assertThrows[RuntimeException] {
      handleSqlException(queryFunc.run(), 3)
    }
  }

  test("handleSqlException should rethrow SQLException because code is not equal 1023") {
    val queryFunc = mock[Runnable]
    Mockito
      .when(queryFunc.run())
      .thenAnswer(_ => throw new SQLException("test exception", "exception", 1024))

    assertThrows[SQLException] {
      handleSqlException(queryFunc.run(), 3)
    }
  }

  test("handleSqlException should rethrow SQLException") {
    val queryFunc = mock[Runnable]
    val exception = new SQLException("exc", "error ", 1023)
    Mockito
      .when(queryFunc.run())
      .thenAnswer(_ => throw exception)
      .thenAnswer(_ => throw exception)
      .thenAnswer(_ => throw exception)
      .thenAnswer(_ => throw exception)
      .thenAnswer(_ => ())

    assertThrows[SQLException] {
      handleSqlException(queryFunc.run(), 3)
    }
    verify(queryFunc, times(4)).run()
  }

  test("handleSqlException should rethrow RedshiftException") {
    val queryFunc = mock[Runnable]
    val exception = new RedshiftException("ERROR: 1023")
    Mockito
      .when(queryFunc.run())
      .thenAnswer(_ => throw exception)
      .thenAnswer(_ => throw exception)
      .thenAnswer(_ => throw exception)
      .thenAnswer(_ => throw exception)
      .thenAnswer(_ => ())

    assertThrows[SQLException] {
      handleSqlException(queryFunc.run(), 3)
    }
    verify(queryFunc, times(4)).run()
  }

  test("handleSqlException should catch SQLException with RedshiftException") {
    val queryFunc = mock[Runnable]
    val exception = new SQLException("Exception thrown in awaitResult: ", new RedshiftException("ERROR: 1023"))
    Mockito
      .when(queryFunc.run())
      .thenAnswer(_ => throw exception)
      .thenAnswer(_ => ())

    handleSqlException(queryFunc.run(), 3)

    verify(queryFunc, times(2)).run()
  }

  test("should convert time epoch long (millis ir seconds) to timestamp") {
    val session: SparkSession = spark
    import session.implicits._
    val df = Seq((1721692800213L)).toDF("epochcol")
    val resultDf = df.withColumn("timestampcol", getUTCDateFromMillisOrSeconds(col("epochcol")))
    assert(resultDf.count() === 1)
    assert(resultDf.head().getAs[String]("timestampcol") === "2024-07-23 00:00:00.213")

    val dfsecs = Seq((1721692800L)).toDF("epochcol")
    val resultSecDf = dfsecs.withColumn("timestampcol", getUTCDateFromMillisOrSeconds(col("epochcol")))
    assert(resultSecDf.count() === 1)
    assert(resultSecDf.head().getAs[String]("timestampcol") === "2024-07-23 00:00:00.000")

    val dfWrongValue = Seq((17216900L)).toDF("epochcol")
    val wrongDfRes = dfWrongValue.withColumn("timestampcol", getUTCDateFromMillisOrSeconds(col("epochcol")))

    assertThrows[SparkException] {
      wrongDfRes.show(false)
    }
  }

  test("addActiveUntilCol should add active_until column") {
    val session: SparkSession = spark
    import session.implicits._
    val df: DataFrame = List(
      (1, "user", 1001, "2017-07-14 02:40:00.0"),
      (2, "teacher", 1002, "2017-07-15 02:40:00.0"),
      (2, "teacher", 1003, "2017-07-15 03:45:00.0"),
      (2, "teacher", 1004, "2017-07-16 04:45:00.0"),
      (2, "teacher", 1005, "2017-07-16 04:45:00.0"),
    ).toDF("id", "user_type", "test_id", "occurredOn")

    val resDf = addActiveUntilCol("entity", List("id", "user_type"), "occurredOn")(df)

    val expectedColumnNames = List(
      "entity_active_until",
      "id",
      "test_id",
      "user_type",
      "occurredOn",
    )

    assert(resDf.columns.toSet === expectedColumnNames.toSet)
    assert(resDf.filter(col("test_id") === 1001).head().getAs[TimestampType]("entity_active_until") === null)
    assert(resDf.filter(col("test_id") === 1004).head().getAs[TimestampType]("entity_active_until") === null)
    assert(resDf.filter(col("test_id") === 1005).head().getAs[TimestampType]("entity_active_until") === null)
    assert(
      resDf.filter(col("test_id") === 1003).head().getAs[TimestampType]("entity_active_until") === Timestamp.valueOf(
        "2017-07-16 04:45:00.0"))
    assert(
      resDf.filter(col("test_id") === 1002).head().getAs[TimestampType]("entity_active_until") === Timestamp.valueOf(
        "2017-07-16 04:45:00.0"))
  }

  test("addHistoryStatus should update status 1 on 2 only for historical entities") {
    val session: SparkSession = spark
    import session.implicits._
    val df: DataFrame = List(
      (1, "user", 1001, "2017-07-14 02:40:00.0", 1),
      (2, "teacher", 1002, "2017-07-15 02:40:00.0", 1),
      (2, "teacher", 1003, "2017-07-15 03:45:00.0", 1),
      (2, "teacher", 1004, "2017-07-15 03:46:00.0", 3),
    ).toDF("id", "user_type", "test_id", "occurredOn", "user_status")

    val resDf = addHistoryStatus("user", List("id", "user_type"), "occurredOn")(df)

    assert(resDf.filter(col("test_id") === 1001).head().getAs[Int]("user_status") === 1)
    assert(resDf.filter(col("test_id") === 1002).head().getAs[Int]("user_status") === 2)
    assert(resDf.filter(col("test_id") === 1003).head().getAs[Int]("user_status") === 2)
    assert(resDf.filter(col("test_id") === 1004).head().getAs[Int]("user_status") === 3)
  }

  test("addHistoryStatusForSCDTypeII should update status 1 on 2") {
    val session: SparkSession = spark
    import session.implicits._
    val df: DataFrame = List(
      (1, "user", 1001, "2017-07-14 02:40:00.0", "UserCreated"),
      (2, "teacher", 1002, "2017-07-15 02:40:00.0", "UserCreated"),
      (2, "teacher", 1003, "2017-07-15 03:45:00.0", "UserUpdated"),
      (2, "teacher", 1004, "2017-07-15 03:46:00.0", "UserDeleted"),
    ).toDF("id", "user_type", "test_id", "occurredOn", "eventType")

    val resDf = addHistoryStatusForSCDTypeII("user", List("id", "user_type"), "occurredOn", deleteEvent = "UserDeleted")(df)

    assert(resDf.filter(col("test_id") === 1001).head().getAs[Int]("user_status") === 1)
    assert(resDf.filter(col("test_id") === 1002).head().getAs[Int]("user_status") === 2)
    assert(resDf.filter(col("test_id") === 1003).head().getAs[Int]("user_status") === 2)
    assert(resDf.filter(col("test_id") === 1004).head().getAs[Int]("user_status") === 4)
  }

  test("addHistoryStatusForSCDTypeII should update status 1 on 2 with the same occurredOn") {
    val session: SparkSession = spark
    import session.implicits._
    val df: DataFrame = List(
      (2, "teacher", 1001, "2017-07-15 02:40:00.0", "UserCreated"),
      (2, "teacher", 1002, "2017-07-15 03:45:00.0", "UserUpdated"),
      (2, "teacher", 1003, "2017-07-15 03:46:00.0", "UserUpdated"),
      (2, "teacher", 1004, "2017-07-15 03:46:00.0", "UserUpdated"),
    ).toDF("id", "user_type", "test_id", "occurredOn", "eventType")

    val resDf = addHistoryStatusForSCDTypeII("user", List("id", "user_type"), "occurredOn", deleteEvent = "UserDeleted")(df)

    assert(resDf.filter(col("test_id") === 1001).head().getAs[Int]("user_status") === 2)
    assert(resDf.filter(col("test_id") === 1002).head().getAs[Int]("user_status") === 2)
    assert(resDf.filter(col("test_id") === 1003).head().getAs[Int]("user_status") === 1)
    assert(resDf.filter(col("test_id") === 1004).head().getAs[Int]("user_status") === 1)
  }

  test("setInsertWithHistoryForStaging should return options") {
    val session: SparkSession = spark
    import session.implicits._
    val df: DataFrame = List(
      (1, 2, "SCHOOL", "6f6265e6-61ed-479b-a9b0-1280029fd6e6", 1, "TagCreatedEvent"),
    ).toDF("tag_id", "tag_second_id", "tag_type", "tag_association_id", "tag_status", "eventType")

    val earliestDataQuery =
      "select * from alefdw.staging_dim_tag where concat(concat(tag_id, tag_second_id), cast(tag_created_time as varchar)) in (select id from (select concat(concat(tag_id, tag_second_id), cast(tag_created_time as varchar)) id, dense_rank() over (partition by tag_id, tag_second_id order by tag_created_time) rnk from alefdw.staging_dim_tag) a where a.rnk = 1);"

    val res = setInsertWithHistoryForStaging(df, List("tag_id", "tag_second_id"), "tag", inactiveStatus = Detached, earliestDataQuery)

    assert(res("dbtable") === "alefdw.staging_dim_tag")
    assert(replaceSpecChars(res("postactions")) === replaceSpecChars(s"""
         |begin transaction;
         |CREATE TEMPORARY TABLE earliest as select * from alefdw.staging_dim_tag where concat(concat(tag_id, tag_second_id), cast(tag_created_time as varchar)) in (select id from (select concat(concat(tag_id, tag_second_id), cast(tag_created_time as varchar)) id, dense_rank() over (partition by tag_id, tag_second_id order by tag_created_time) rnk from alefdw.staging_dim_tag) a where a.rnk = 1);;
         |
         |UPDATE alefdw_stage.rel_tag
         |SET tag_status = '4', tag_updated_time = earliest.tag_created_time, tag_dw_updated_time = earliest.tag_dw_created_time
         |FROM earliest
         |WHERE rel_tag.tag_id = earliest.tag_id and rel_tag.tag_second_id = earliest.tag_second_id AND
         | rel_tag.tag_status <> '4' AND
         | rel_tag.tag_created_time < earliest.tag_created_time;
         |
         |UPDATE alefdw.dim_tag
         |SET tag_status = '4', tag_updated_time = earliest.tag_created_time, tag_dw_updated_time = earliest.tag_dw_created_time
         |FROM earliest 
         |WHERE dim_tag.tag_id = earliest.tag_id and dim_tag.tag_second_id = earliest.tag_second_id AND
         | dim_tag.tag_status <> '4' AND
         | dim_tag.tag_created_time < earliest.tag_created_time;
         |
         |insert into alefdw_stage.rel_tag (tag_id, tag_second_id, tag_type, tag_association_id, tag_status, eventType) select tag_id, tag_second_id, tag_type, tag_association_id, tag_status, eventType from alefdw.staging_dim_tag
         |  where not exists (select 1 from alefdw_stage.rel_tag where rel_tag.tag_id = staging_dim_tag.tag_id and rel_tag.tag_second_id = staging_dim_tag.tag_second_id and
         |    rel_tag.tag_status = '1' and
         |      rel_tag.tag_created_time = staging_dim_tag.tag_created_time);
         |
         |drop table alefdw.staging_dim_tag;
         |end transaction
         |""".stripMargin))
  }

  test("getOptionsForStagingSCDTypeII should return options") {
    val session: SparkSession = spark
    import session.implicits._
    val df: DataFrame = List(
      (1, 2, "SCHOOL", "6f6265e6-61ed-479b-a9b0-1280029fd6e6", 1, "TagCreatedEvent"),
    ).toDF("tag_id", "tag_second_id", "tag_type", "tag_association_id", "tag_status", "eventType")

    val res = getOptionsForStagingSCDTypeII(df, "tag", "rel_tag", "dim_tag", List("tag_id", "tag_second_id"))

    assert(res("dbtable") === "alefdw.staging_dim_tag")
    assert(replaceSpecChars(res("postactions")) === replaceSpecChars(s"""
           |begin transaction;
           |CREATE TEMPORARY TABLE earliest as select * from alefdw.staging_dim_tag where concat(concat(tag_id, tag_second_id), cast(tag_created_time as varchar)) in (select id from (select concat(concat(tag_id, tag_second_id), cast(tag_created_time as varchar)) id, dense_rank() over (partition by tag_id, tag_second_id order by tag_created_time) rnk from alefdw.staging_dim_tag) a where a.rnk = 1);
           |
           |update alefdw_stage.rel_tag set tag_status = '2' from earliest where rel_tag.tag_id = earliest.tag_id and rel_tag.tag_second_id = earliest.tag_second_id and
           |  rel_tag.tag_status = '1' and
           |  rel_tag.tag_created_time < earliest.tag_created_time;
           |update alefdw_stage.rel_tag set tag_active_until = earliest.tag_created_time from earliest where rel_tag.tag_id = earliest.tag_id and rel_tag.tag_second_id = earliest.tag_second_id and
           |  rel_tag.tag_active_until is null and
           |  rel_tag.tag_created_time < earliest.tag_created_time;
           |
           |update alefdw.dim_tag set tag_status = '2' from earliest where dim_tag.tag_id = earliest.tag_id and dim_tag.tag_second_id = earliest.tag_second_id and
           |  dim_tag.tag_status = '1' and
           |  dim_tag.tag_created_time < earliest.tag_created_time;
           |update alefdw.dim_tag set tag_active_until = earliest.tag_created_time from earliest where dim_tag.tag_id = earliest.tag_id and dim_tag.tag_second_id = earliest.tag_second_id and
           |  dim_tag.tag_active_until is null and
           |  dim_tag.tag_created_time < earliest.tag_created_time;
           |
           |insert into alefdw_stage.rel_tag (tag_id, tag_second_id, tag_type, tag_association_id, tag_status, eventType) select tag_id, tag_second_id, tag_type, tag_association_id, tag_status, eventType from alefdw.staging_dim_tag
           |  where not exists (select 1 from alefdw_stage.rel_tag where rel_tag.tag_id = staging_dim_tag.tag_id and rel_tag.tag_second_id = staging_dim_tag.tag_second_id and
           |    rel_tag.tag_status = '1' and
           |      rel_tag.tag_created_time = staging_dim_tag.tag_created_time);
           |
           |drop table alefdw.staging_dim_tag;
           |end transaction""".stripMargin))
  }

  test("mkIdConditions should create condition string") {
    val res = mkIdConditions("dim_tag", List("tag_id", "tag_second_id"))

    assert(res === "dim_tag.tag_id = earliest.tag_id and dim_tag.tag_second_id = earliest.tag_second_id")
  }

  test("mkIdConditionsForInsert should create condition string") {
    val res = mkIdConditionsForInsert("dim_tag", "rel_tag", List("tag_id", "tag_second_id"))

    assert(res === "dim_tag.tag_id = rel_tag.tag_id and dim_tag.tag_second_id = rel_tag.tag_second_id")
  }

  test("getOptionsForInsertIfNotExists should return options for Insert if not exists case") {
    val session: SparkSession = spark
    import session.implicits._
    val df: DataFrame = List(
      (1, 2, "SCHOOL", "6f6265e6-61ed-479b-a9b0-1280029fd6e6", 1, "TagCreatedEvent"),
    ).toDF("tag_id", "tag_second_id", "tag_type", "tag_association_id", "tag_status", "eventType")

    val res = getOptionsForInsertIfNotExists(df,
                                             "rel_tag",
                                             "tag_id = t.tag_id AND NVL(tag_second_id, '') = NVL(t.tag_second_id, '')",
                                             isStaging = true)

    assert(res("dbtable") === "alefdw_stage.staging_rel_tag_tmp")
    assert(replaceSpecChars(res("postactions")) === replaceSpecChars(s"""
          |INSERT INTO alefdw_stage.rel_tag (tag_id, tag_second_id, tag_type, tag_association_id, tag_status, eventType) select tag_id, tag_second_id, tag_type, tag_association_id, tag_status, eventType from alefdw_stage.staging_rel_tag_tmp t
          |     WHERE NOT EXISTS (SELECT 1 FROM alefdw_stage.rel_tag WHERE tag_id = t.tag_id AND NVL(tag_second_id, '') = NVL(t.tag_second_id, ''));
          |DROP table alefdw_stage.staging_rel_tag_tmp;
      """.stripMargin))
  }

}
