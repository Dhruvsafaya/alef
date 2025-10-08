package com.alefeducation.dimensions.question

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.dimensions.question.QuestionDimensionTransform.QuestionMutatedEntityPrefix
import com.alefeducation.dimensions.question.QuestionDwIdMappingTransform.QuestionMutatedDwIdMappingService
import com.alefeducation.service.DataSink
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Resources.{getSink, getSource}
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.{DataFrame, SparkSession}


class QuestionDwIdMappingTransform(val session: SparkSession, val service: SparkBatchService) {

  def transform(): List[Option[Sink]] = {

    val QuestionMutated: Option[DataFrame] = service.readOptional(getSource(QuestionMutatedDwIdMappingService).head, session)

    val dwIdMappingDf = QuestionMutated.flatMap(
      _.dropDuplicates("questionId").select("questionId","occurredOn")
        .transformForInsertDwIdMapping("entity", QuestionMutatedEntityPrefix)
        .checkEmptyDf
    )

    List(
      dwIdMappingDf.map(DataSink(getSink(QuestionMutatedDwIdMappingService).head, _)),
    )
  }
}

object QuestionDwIdMappingTransform {
  val QuestionMutatedDwIdMappingService = "dw-id-mapping-ccl-question-mutated-service"

  val session: SparkSession = SparkSessionUtils.getSession(QuestionMutatedDwIdMappingService)
  val service = new SparkBatchService(QuestionMutatedDwIdMappingService, session)

  def main(args: Array[String]): Unit = {
    val transformer = new QuestionDwIdMappingTransform(session, service)
    service.runAll(transformer.transform().flatten)
  }
}