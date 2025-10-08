package com.alefeducation.facts.announcement

import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.facts.announcement.AnnouncementUtils.GuardiansAnnTypeVal
import org.apache.spark.sql.DataFrame

object AnnouncementUtilsTest extends SparkSuite {

  val expectedColumns = Set(
    "fa_recipient_id",
    "fa_admin_id",
    "fa_id",
    "fa_role_id",
    "fa_status",
    "fa_recipient_type_description",
    "fa_recipient_type",
    "fa_tenant_id",
    "fa_has_attachment",
    "eventdate",
    "fa_date_dw_id",
    "fa_created_time",
    "fa_dw_created_time",
    "fa_type"
  )

  def assertCommonFields(roleName: String, status: Int, df: DataFrame): Unit = {
    assert[String](df, "fa_created_time", "2022-11-17 09:29:24.43")
    assert[String](df, "fa_admin_id", "c3bed03b-19ff-4718-b6bf-17a359a349cd")
    assert[String](df, "fa_id", "65cf3dd8-b819-446d-b0bf-b7216b7bbba9")
    assert[String](df, "fa_role_id", roleName)
    assert[Int](df, "fa_status", status)
    assert[String](df, "fa_tenant_id", "93e4949d-7eff-4707-9201-dac917a5e013")
    assert[Boolean](df, "fa_has_attachment", false)
    assert[String](df, "eventdate", "2022-11-17")
    assert[String](df, "fa_date_dw_id", "20221117")
    assert[Int](df, "fa_type", GuardiansAnnTypeVal)
  }

}
