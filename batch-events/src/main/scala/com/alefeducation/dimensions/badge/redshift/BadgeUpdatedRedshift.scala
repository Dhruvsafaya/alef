package com.alefeducation.dimensions.badge.redshift

import com.alefeducation.base.SparkBatchService
import com.alefeducation.dimensions.badge.transform.BadgeUpdatedTransform.{BadgeInactiveStatus, BadgeUpdatedTransformed}
import com.alefeducation.util.{Resources, SparkSessionUtils}
import org.apache.spark.sql.functions.{col, current_timestamp, lit, when}
import com.alefeducation.util.BatchTransformerUtility._
import org.apache.spark.sql.DataFrame

object BadgeUpdatedRedshift {
  val BadgeUpdatedService: String = "redshift-badge-updated-service"
  val BadgeUpdatedRedshift: String = "redshift-badge"

  private val session = SparkSessionUtils.getSession(BadgeUpdatedService)
  val service = new SparkBatchService(BadgeUpdatedService, session)

  def main(args: Array[String]): Unit = {

    service.run {
      val updatedSource = service.readOptional(BadgeUpdatedTransformed, session).map(
        _.withColumn("bdg_dw_updated_time",
          when(col("bdg_status") === lit(BadgeInactiveStatus), current_timestamp())
        )
      )

      updatedSource.map(
        _.drop("bdg_tenant_id").drop("bdg_deleted_time").drop("bdg_updated_time").drop("bdg_dw_updated_time")
        .toRedshiftSCDSink(
          BadgeUpdatedRedshift,
          "",
          "badge",
          setScdTypeIIUpdate
        )
      )
    }
  }

  def setScdTypeIIUpdate(dataFrame: DataFrame, name: String = "badge"): Map[String, String] = {
    val targetTableDim = s"dim_$name"
    val dimSchema = Resources.redshiftSchema()
    val stagingTable = s"staging_$targetTableDim"
    val insertCols = dataFrame.columns.mkString(", ")

    val updateStatusAndActiveUntil =
      s"""
       begin transaction;
       update $dimSchema.$targetTableDim set bdg_status = 2,
       bdg_active_until = staging.bdg_created_time
       from $dimSchema.$targetTableDim target
       join $dimSchema.$stagingTable staging
       on staging.bdg_id = target.bdg_id and
         staging.bdg_grade = target.bdg_grade and
         staging.bdg_tier = target.bdg_tier
       where target.bdg_created_time < staging.bdg_created_time
       and target.bdg_active_until is null;

       insert into $dimSchema.$targetTableDim ($insertCols)
       (select $insertCols from $dimSchema.$stagingTable);
       DROP table $dimSchema.$stagingTable;
       end transaction
     """.stripMargin
    Map("dbtable" -> s"$dimSchema.$stagingTable", "postactions" -> updateStatusAndActiveUntil)
  }

}
