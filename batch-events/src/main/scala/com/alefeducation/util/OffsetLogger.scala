package com.alefeducation.util

import java.io.ByteArrayInputStream
import com.alefeducation.util.AwsUtils
import com.alefeducation.util.AwsUtils.putDataToS3
import com.amazonaws.services.s3.model.ObjectMetadata

class OffsetLogger {
  def log(offset: String, offsetLogPath: String, silverTableName: String): Unit = {
    val jsonBytes = offset.getBytes("UTF-8")
    val inputStream = new ByteArrayInputStream(jsonBytes)

    val metadata = new ObjectMetadata()
    metadata.setContentLength(jsonBytes.length)
    metadata.setContentType("application/json")

    putDataToS3(inputStream, s"$offsetLogPath$silverTableName/offset.json", metadata)
  }
}
