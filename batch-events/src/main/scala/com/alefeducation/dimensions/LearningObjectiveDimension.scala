package com.alefeducation.dimensions

import com.alefeducation.service.{DataSink, SparkBatchService}
import com.alefeducation.util.BatchUtility.transformDataFrame
import com.alefeducation.util.Constants._
import com.alefeducation.util.Helpers._
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.SparkSession

class LearningObjectiveDimensionTransformer(override val name: String, override val session: SparkSession) extends SparkBatchService {

  override def transform(): List[DataSink] = {
    import session.implicits._
    val learningObjectiveCreatedDf = read(ParquetLearningObjectiveCreatedSource, session)
    val learningObjectiveMutatedDf = read(ParquetLearningObjectiveMutatedSource, session)
      .withColumn("uuid", $"id")

    val createSink = transformDataFrame(session,
      learningObjectiveCreatedDf,
      List(LearningObjectiveCreated),
      LoDimensionCols,
      LearningObjectiveEntity,
      RedshiftLearningObjectiveSink,
      ParquetLearningObjectiveCreatedSource)

    val mutatedSink = transformDataFrame(session,
      learningObjectiveMutatedDf,
      List(LearningObjectiveUpdated,LearningObjectiveDeleted),
      LoDimensionCols,
      LearningObjectiveEntity,
      RedshiftLearningObjectiveSink,
      ParquetLearningObjectiveMutatedSource)

    createSink ++ mutatedSink
  }
}

object LearningObjectiveDimension {
  def main(args: Array[String]): Unit = {
    new LearningObjectiveDimensionTransformer(LearningObjectiveDimensionName, SparkSessionUtils.getSession(LearningObjectiveDimensionName)).run
  }
}
