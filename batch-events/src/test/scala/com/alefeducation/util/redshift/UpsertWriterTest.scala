package com.alefeducation.util.redshift

import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.dimensions.BaseDimensionSpec

class UpsertWriterTest extends SparkSuite with BaseDimensionSpec {
  test("Should get updated columns for create") {
    val columnsToUpdate = UpsertWriter.getColumnsToUpdate("redshift-academic-calendar-created-service", Map.empty)
    columnsToUpdate shouldBe Map("academic_calendar_dw_updated_time" -> "temp_table.academic_calendar_dw_created_time")
  }
  test("Should get updated columns for update") {
    val columnsToUpdate = UpsertWriter.getColumnsToUpdate("redshift-academic-calendar-updated-service", Map.empty)
    columnsToUpdate shouldBe Map("academic_calendar_dw_updated_time" -> "temp_table.academic_calendar_dw_created_time", "academic_calendar_updated_time" -> "temp_table.academic_calendar_created_time")
  }
  test("Should get updated columns for delete") {
    val columnsToUpdate = UpsertWriter.getColumnsToUpdate("redshift-academic-calendar-deleted-service", Map.empty)
    columnsToUpdate shouldBe Map("academic_calendar_dw_updated_time" -> "temp_table.academic_calendar_dw_created_time", "academic_calendar_deleted_time" -> "temp_table.academic_calendar_deleted_time", "academic_calendar_status" -> "temp_table.academic_calendar_status")
  }
}
