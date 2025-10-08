package com.alefeducation.exception

object NoParquetFileException extends Exception

final case class NoParquetFileException(private val message: String = "", private val cause: Throwable = None.orNull) extends Exception(message, cause)

final case class MissingDimException(private val message: String = "", private val cause: Throwable = None.orNull) extends Exception(message, cause)

final case class SinkNotConfiguredException(private val message: String = "", private val cause: Throwable = None.orNull) extends Exception(message, cause)

final case class MoveFilesFailedException(private val message: String = "", private val cause: Throwable = None.orNull) extends Exception(message, cause)

final case class DeleteFilesFailedException(private val message: String = "", private val cause: Throwable = None.orNull) extends Exception(message, cause)

final case class MissedOffsetColumnException(private val message: String = "", private val cause: Throwable = None.orNull)
  extends Exception(message, cause)

final case class MissedEntityException(private val message: String = "", private val cause: Throwable = None.orNull)
  extends Exception(message, cause)