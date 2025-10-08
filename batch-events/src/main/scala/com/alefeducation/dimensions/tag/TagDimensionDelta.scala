package com.alefeducation.dimensions.tag

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.batch.delta.Alias
import com.alefeducation.dimensions.tag.TagDimensionTransform.{TagEntityPrefix, TagInactiveStatus, uniqueIdCols}
import com.alefeducation.util.Helpers.TagEntity
import com.alefeducation.util.Resources.{getSink, getSource}
import com.alefeducation.util.SparkSessionUtils

object TagDimensionDelta {

  import com.alefeducation.bigdata.batch.delta.Delta.Transformations._

  val TagDeltaService = "delta-tag-mutated-service"

  val deltaMatchCondition: String =
    s"""
       |${Alias.Delta}.${TagEntity}_id = ${Alias.Events}.${TagEntity}_id
       | and
       |${Alias.Delta}.${TagEntity}_association_id = ${Alias.Events}.${TagEntity}_association_id
       | and
       |${Alias.Delta}.${TagEntity}_association_type = ${Alias.Events}.${TagEntity}_association_type
     """.stripMargin

  private val session = SparkSessionUtils.getSession(TagDeltaService)
  val service = new SparkBatchService(TagDeltaService, session)

  def main(args: Array[String]): Unit = {
    service.run {
      val TransformedTag = service.readOptional(getSource(TagDeltaService).head, session)
      TransformedTag.map(
          _.toIWHContext(matchConditions = deltaMatchCondition, uniqueIdColumns = uniqueIdCols, inactiveStatus = TagInactiveStatus)
          .toSink(getSink(TagDeltaService).head, TagEntityPrefix)
      )
    }
  }
}
