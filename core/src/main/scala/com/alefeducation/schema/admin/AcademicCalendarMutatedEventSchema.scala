package com.alefeducation.schema.admin

import org.apache.spark.sql.types.{ArrayType, BooleanType, LongType, StringType, StructField, StructType}

object AcademicCalendarMutatedEventSchema {
  private val teachingPeriodOutputSchema: StructType = StructType(
    Seq(
      StructField("uuid", StringType),
      StructField("title", StringType),
      StructField("startDate", LongType),
      StructField("endDate", LongType),
      StructField("current", BooleanType),
      StructField("createdBy", StringType),
      StructField("updatedBy", StringType),
      StructField("createdOn", LongType),
      StructField("updatedOn", LongType)
    )
  )

  val academicCalendarMutatedEventSchema: StructType = StructType(
    Seq(
      StructField("occurredOn", LongType),
      StructField("uuid", StringType),
      StructField("title", StringType),
      StructField("default", BooleanType),
      StructField("type", StringType),
      StructField("academicYearId", StringType),
      StructField("schoolId", StringType),
      StructField("organization", StringType),
      StructField("teachingPeriods", ArrayType(teachingPeriodOutputSchema)),
      StructField("createdBy", StringType),
      StructField("updatedBy", StringType),
      StructField("createdOn", LongType),
      StructField("updatedOn", LongType)
    )
  )
}
