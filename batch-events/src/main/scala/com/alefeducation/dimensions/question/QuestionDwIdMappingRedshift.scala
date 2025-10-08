package com.alefeducation.dimensions.question

import com.alefeducation.base.SparkBatchService
import com.alefeducation.dimensions.question.QuestionDwIdMappingTransform.QuestionMutatedDwIdMappingService
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Resources.getSink
import com.alefeducation.util.SparkSessionUtils
import com.alefeducation.util.redshift.options.UpsertOptions


object QuestionDwIdMappingRedshift {
  val QuestionDwIdMappingService = "redshift-ccl-question-dw-id-mapping-service"


  private val session = SparkSessionUtils.getSession(QuestionDwIdMappingService)
  val service = new SparkBatchService(QuestionDwIdMappingService, session)

  def main(args: Array[String]): Unit = {
    service.run {
      val QuestionDwIdMappingDf = service.readUniqueOptional(getSink(QuestionMutatedDwIdMappingService).head, session, uniqueColNames = List("questionId", "entity_type"))
      QuestionDwIdMappingDf.map(
        _.withColumnRenamed("questionId","id")
          .toRedshiftUpsertSink(
          getSink(QuestionDwIdMappingService).head,
          UpsertOptions.RelDwIdMappings.targetTableName,
          matchConditions = UpsertOptions.RelDwIdMappings.matchConditions("question"),
          columnsToUpdate = UpsertOptions.RelDwIdMappings.columnsToUpdate,
          columnsToInsert = UpsertOptions.RelDwIdMappings.columnsToInsert,
          isStaging = true
        )
      )
    }
  }

}
