package com.alefeducation.util

import com.alefeducation.util.DateTimeProvider._
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, lit, to_utc_timestamp}

import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import java.time.{ZoneId, ZonedDateTime}
import java.util.Calendar

class DateTimeProvider(
                        defaultDateTimeFormat: String = DefaultDateTimeFormat,
                        defaultTimeZone: String = DefaultTimeZone) {

  def currentDateTime(format: String = defaultDateTimeFormat, timeZone: String = defaultTimeZone): String = {
    val formatter = DateTimeFormatter.ofPattern(format).withZone(ZoneId.of(timeZone))
    formatter.format(ZonedDateTime.now)
  }

  def currentUtcTimestamp(): Column = to_utc_timestamp(lit(currentDateTime()), Calendar.getInstance().getTimeZone.getID)

  def toUtcTimestamp(column: String): Column = to_utc_timestamp(col(column), Calendar.getInstance().getTimeZone.getID)

  def currentUtcJavaTimestamp(): Timestamp = {
    val zdt = ZonedDateTime.now.withZoneSameInstant(ZoneId.of("UTC"))
    Timestamp.valueOf(zdt.toLocalDateTime)
  }
}

object DateTimeProvider {
  val DefaultDateTimeFormat = "yyyy-MM-dd HH:mm:ss.SSS"
  val DefaultTimeZone = "UTC"
  val defaultTimestamp: Timestamp = Timestamp.valueOf("1970-01-01 00:00:00")
}
