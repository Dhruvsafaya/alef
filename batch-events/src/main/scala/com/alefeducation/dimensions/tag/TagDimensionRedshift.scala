package com.alefeducation.dimensions.tag

import com.alefeducation.base.SparkBatchService
import com.alefeducation.dimensions.tag.TagDimensionTransform.{TagEntityPrefix, TagInactiveStatus, uniqueIdCols}
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Resources.{getSink, getSource}
import com.alefeducation.util.SparkSessionUtils

object TagDimensionRedshift {
  private val TagRedshiftService = "redshift-tag-mutated-service"

  private val session = SparkSessionUtils.getSession(TagRedshiftService)
  val service = new SparkBatchService(TagRedshiftService, session)

  def main(args: Array[String]): Unit = {
    service.run {
      val TagTransformed = service.readOptional(getSource(TagRedshiftService).head, session)

      TagTransformed.map(_.toRedshiftIWHSink(
          getSink(TagRedshiftService).head,
          TagEntityPrefix,
          ids = uniqueIdCols,
          inactiveStatus = TagInactiveStatus,
          isStagingSink = true
        )
      )
    }
  }
}