package com.alefeducation.util.ktgame

import com.alefeducation.util.Helpers._
import com.alefeducation.util.ktgame.KTGameHelper._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

object KTGameUtility {

  implicit class DFUtils(inputDf: DataFrame) {

    import com.alefeducation.util.BatchTransformerUtility._

    val df: DataFrame = inputDf.withColumn("occurredOn", regexp_replace(col("occurredOn"), "T", " "))

    def selectStartedColumns: DataFrame = {
      sessionCommonColumns
        .withColumn("score", lit(DefaultScore))
        .withColumn("stars", lit(DefaultStars))
        .withColumn("isStart", lit(true))
    }

    def selectFinishedColumns: DataFrame = {
      sessionCommonColumns
        .withColumn("score", col("score"))
        .withColumn("stars", col("stars"))
        .withColumn("isStart", lit(false))
    }

    def sessionCommonColumns: DataFrame = df
      .withColumn("ktGameLoId", lit(NULL).cast(StringType))
      .withColumn("questionTime", lit(MinusOneDecimal))
      .withColumn("scoreBreakdownAnswer", lit(NULL).cast(StringType))
      .withColumn("scoreBreakdownIsAttended", lit(false))
      .withColumn("scoreBreakdownMaxScore", lit(MinusOne).cast(StringType))
      .withColumn("questionId", lit(NULL).cast(StringType))
      .withColumn("isStartProcessed", lit(false))
      .withColumn("attempts", lit(MinusOne))
      .withColumn("keyTermId", lit(NULL).cast(StringType))
      .withColumn("ktEventType", lit(KtGameSessionType))

    def selectQuestionStartedColumns: DataFrame = {
      df
        .withColumn("ktGameLoId", col("learningObjectiveId"))
        .withColumn("questionTime", col("time"))
        .withColumn("scoreBreakdownAnswer", lit(NULL).cast(StringType))
        .withColumn("scoreBreakdownIsAttended", lit(false))
        .withColumn("scoreBreakdownMaxScore", lit(MinusOne).cast(StringType))
        .withColumn("score", lit(DefaultScore))
        .withColumn("stars", lit(DefaultStars))
        .withColumn("questionId", col("questionId"))
        .withColumn("isStart", lit(true))
        .withColumn("isStartProcessed", lit(false))
        .withColumn("attempts", lit(MinusOne))
        .withColumn("keyTermId", col("keyTermId"))
        .withColumn("ktEventType", lit(KtGameQuestionSessionType))
    }

    def selectQuestionFinishedColumns: DataFrame = {
      df
        .withColumn("ktGameLoId", col("learningObjectiveId"))
        .withColumn("questionTime", col("time"))
        .withColumn("scoreBreakdownAnswer",
          when(col("scoreBreakdown.answer").isNotNull, col("scoreBreakdown.answer"))
            .otherwise(concat_ws(", ", col("scoreBreakdown.answers")))
        )
        .withColumn("scoreBreakdownIsAttended", col("scoreBreakdown.isAttended"))
        .withColumn("scoreBreakdownMaxScore", col("scoreBreakdown.maxScore"))
        .withColumn("score", col("scoreBreakdown.score"))
        .withColumn("stars", lit(DefaultStars))
        .withColumn("questionId", col("questionId"))
        .withColumn("isStart", lit(false))
        .withColumn("isStartProcessed", lit(false))
        .withColumn("attempts", lit(size(col("scoreBreakdown.answers"))))
        .withColumn("keyTermId", col("keyTermId"))
        .withColumn("ktEventType", lit(KtGameQuestionSessionType))
    }

    def selectGameColumns: DataFrame = {
      df
        .withColumn("consideredLO", explode(col("consideredLos")))
        .selectLatestByRowNumber(List("learnerId", "consideredLO.id"))
        .withColumn("ktGameLoId", col("consideredLO.id"))
        .withColumn("ktGameLoKtCollectionId", col("consideredLO.ktCollectionId"))
        .withColumn("ktGameLoNumberOfKeyTerms", col("consideredLO.numberOfKeyTerms"))
        .withColumn("ktGameLoNumberOfKeyTerms",
          when(col("ktGameLoNumberOfKeyTerms").isNotNull, col("ktGameLoNumberOfKeyTerms"))
            .otherwise(lit(MinusOne))
        )
        .withColumn("questionType", col("gameConfig.question.type"))
        .withColumn("questionMin", col("gameConfig.question.min"))
        .withColumn("questionMax", col("gameConfig.question.max"))
        .withColumn("questionTime", col("gameConfig.question.time"))
        .withColumn("gameType", col("gameConfig.type"))
    }
  }

}
