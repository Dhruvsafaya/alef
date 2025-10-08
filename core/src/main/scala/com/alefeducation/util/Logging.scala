package com.alefeducation.util

import org.apache.log4j.Logger

trait Logging extends Serializable {
  @transient lazy val log = Logger.getLogger(getClass.getName)
}
