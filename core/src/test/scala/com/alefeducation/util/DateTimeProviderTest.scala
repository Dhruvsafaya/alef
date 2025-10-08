package com.alefeducation.util

import com.alefeducation.bigdata.commons.testutils.{SparkTest, TestBaseSpec}

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.{ZoneId, ZonedDateTime}
import java.util.TimeZone

class DateTimeProviderTest extends TestBaseSpec with SparkTest {

  val tenSecondInMillis = 10000

  describe("currentDateString") {
    it("should provide the current timestamp in the default format in UTC as string") {
      val currentTime = new DateTimeProvider().currentDateTime()

      val expectedEpoch = ZonedDateTime.now(ZoneId.of("UTC")).toInstant.toEpochMilli

      Timestamp.valueOf(currentTime).getTime shouldBe (expectedEpoch +- tenSecondInMillis)
    }

    it("should allow to get timestamp in a different format") {
      val dateTimeFormat = "yyyy/MM/dd HH:mm:ss"
      val currentTime = new DateTimeProvider(defaultDateTimeFormat = dateTimeFormat).currentDateTime()

      val expectedFormat = new SimpleDateFormat(dateTimeFormat)
      expectedFormat.setTimeZone(TimeZone.getTimeZone("UTC"))

      val timeNow = ZonedDateTime.now(ZoneId.of("UTC")).toInstant.toEpochMilli

      val sixtySeconds = tenSecondInMillis * 6
      expectedFormat.parse(currentTime).getTime shouldBe (timeNow +- sixtySeconds)
    }

    it("should provide the current timestamp in a different time zone") {
      val currentTime = new DateTimeProvider(defaultTimeZone = "GMT").currentDateTime()

      val expectedEpoch = ZonedDateTime.now(ZoneId.of("GMT")).toInstant.toEpochMilli

      Timestamp.valueOf(currentTime).getTime shouldBe (expectedEpoch +- tenSecondInMillis)
    }
  }

  describe("currentUTCTimestamp") {
    it("should provide current time in UTC as Column data type") {
      val currentUTCTimestamp = new DateTimeProvider().currentUtcTimestamp()
      import spark.implicits._

      val currentTimestamp = Seq(1).toDF("index")
        .withColumn("current_timestamp", currentUTCTimestamp)
        .first().getAs[Timestamp]("current_timestamp")

      val expectedEpoch = ZonedDateTime.now(ZoneId.of("UTC")).toInstant.toEpochMilli

      currentTimestamp.getTime shouldBe (expectedEpoch +- tenSecondInMillis)
    }
  }

  describe("toUtcTimestamp") {
    it("should convert the provided timestamp column values in string into utc timestamp") {
      val dateTimeProvider = new DateTimeProvider()

      import spark.implicits._
      val actualTimestamp = Seq("2021-11-23 02:40:00.0").toDF("str_timestamp")
        .withColumn("utc_timestamp", dateTimeProvider.toUtcTimestamp("str_timestamp"))
        .first()
        .getAs[Timestamp]("utc_timestamp")

      actualTimestamp shouldBe Timestamp.valueOf("2021-11-23 02:40:00.0")
    }
  }
}
