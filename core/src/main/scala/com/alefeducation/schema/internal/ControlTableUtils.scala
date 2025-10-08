package com.alefeducation.schema.internal

import com.alefeducation.util.DateTimeProvider

object ControlTableUtils {

  val dateTimeProvider = new DateTimeProvider

  sealed trait CSStatus {
    override def toString: String = {
      val className = this.getClass.getSimpleName
      className.toLowerCase().replace("$", "")
    }
  }
  final case object Started extends CSStatus
  final case object Processing extends CSStatus
  final case object Failed extends CSStatus
  final case object Completed extends CSStatus


  sealed trait UpdateType
  final case object ProductMaxIdType extends UpdateType
  final case object DataOffsetType extends UpdateType
}
