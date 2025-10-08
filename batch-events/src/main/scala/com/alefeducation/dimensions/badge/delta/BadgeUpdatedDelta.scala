package com.alefeducation.dimensions.badge.delta

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.batch.delta.Alias
import com.alefeducation.dimensions.badge.transform.BadgeUpdatedTransform.{BadgeEntityPrefix, BadgeUpdatedTransformed}
import com.alefeducation.util.SparkSessionUtils

object BadgeUpdatedDelta {

  import com.alefeducation.bigdata.batch.delta.Delta.Transformations._

  val BadgeUpdatedDeltaService = "delta-badge-updated"
  val BadgeUpdatedDeltaSink: String = "delta-badge"

  private val session = SparkSessionUtils.getSession(BadgeUpdatedDeltaService)
  val service = new SparkBatchService(BadgeUpdatedDeltaService, session)

  def main(args: Array[String]): Unit = {

    service.run {
      val updatedSource = service.readOptional(BadgeUpdatedTransformed, session)
      updatedSource.flatMap(
        _.drop("bdg_tenant_dw_id").drop("bdg_updated_time").drop("bdg_deleted_time").drop("bdg_dw_updated_time")
          .toSCD(
            matchConditions =
              s"""
                 |${Alias.Delta}.bdg_id = ${Alias.Events}.bdg_id
                 |AND ${Alias.Delta}.bdg_tier = ${Alias.Events}.bdg_tier
                 |AND ${Alias.Delta}.bdg_grade = ${Alias.Events}.bdg_grade
             """.stripMargin
            ,
            uniqueIdColumns = List("bdg_id", "bdg_tier", "bdg_grade")
          )
          .map(_.toSink(BadgeUpdatedDeltaSink, BadgeEntityPrefix))
      )
    }
  }
}
