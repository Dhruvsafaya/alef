package com.alefeducation.facts

import com.alefeducation.bigdata.Sink
import com.alefeducation.bigdata.batch.delta.{Alias, Delta}
import com.alefeducation.service.SparkBatchService
import com.alefeducation.util.SparkSessionUtils
import com.alefeducation.util.ktgame.KTGameHelper._
import com.alefeducation.util.ktgame.KTGameUtility
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, date_format}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType}

class KTGameTransformer(override val name: String, override implicit val session: SparkSession) extends SparkBatchService {

  private val filterNot = List("eventdate")

  private val deltaMatchCondition =
    s"""
       |${Alias.Delta}.${FactKtGame}_id = ${Alias.Events}.${FactKtGame}_id
     """.stripMargin

  private val deltaKtgSkippedMatchCondition =
    s"""
       |${Alias.Delta}.${FactKtGameSkipped}_tenant_id <=> ${Alias.Events}.${FactKtGameSkipped}_tenant_id AND
       |${Alias.Delta}.${FactKtGameSkipped}_student_id <=> ${Alias.Events}.${FactKtGameSkipped}_student_id AND
       |${Alias.Delta}.${FactKtGameSkipped}_school_id <=> ${Alias.Events}.${FactKtGameSkipped}_school_id AND
       |${Alias.Delta}.${FactKtGameSkipped}_grade_id <=> ${Alias.Events}.${FactKtGameSkipped}_grade_id AND
       |${Alias.Delta}.${FactKtGameSkipped}_section_id <=> ${Alias.Events}.${FactKtGameSkipped}_section_id AND
       |${Alias.Delta}.${FactKtGameSkipped}_lo_id <=> ${Alias.Events}.${FactKtGameSkipped}_lo_id AND
       |${Alias.Delta}.${FactKtGameSkipped}_trimester_id <=> ${Alias.Events}.${FactKtGameSkipped}_trimester_id AND
       |${Alias.Delta}.${FactKtGameSkipped}_date_dw_id = ${Alias.Events}.${FactKtGameSkipped}_date_dw_id AND
       |${Alias.Delta}.${FactKtGameSkipped}_created_time = ${Alias.Events}.${FactKtGameSkipped}_created_time
       |""".stripMargin

  private val redshiftMatchCondition =
    s"""
       |${FactKtGame}_id = t.${FactKtGame}_id AND
       |NVL(lo_uuid, '') = NVL(t.lo_uuid, '')
       |""".stripMargin

  private val redshiftKtgSkippedMatchCondition =
    s"""
       |NVL(tenant_uuid, '') = NVL(t.tenant_uuid, '') AND
       |NVL(student_uuid, '') = NVL(t.student_uuid, '') AND
       |NVL(subject_uuid, '') = NVL(t.subject_uuid, '') AND
       |NVL(school_uuid, '') = NVL(t.school_uuid, '') AND
       |NVL(grade_uuid, '') = NVL(t.grade_uuid, '') AND
       |NVL(section_uuid, '') = NVL(t.section_uuid, '') AND
       |NVL(lo_uuid, '') = NVL(t.lo_uuid, '') AND
       |NVL(ktgskipped_trimester_id, '') = NVL(t.ktgskipped_trimester_id, '') AND
       |ktgskipped_date_dw_id = t.ktgskipped_date_dw_id AND
       |ktgskipped_created_time = t.ktgskipped_created_time
       |""".stripMargin

  private val ktgameTableName = "staging_ktg"
  private val ktgameSkippedTableName = "staging_ktgskipped"


  override def transform(): List[Sink] = {
    import Delta.Transformations._
    import KTGameUtility._
    import com.alefeducation.util.BatchTransformerUtility._

    val ktGameCreatedDF = readOptional(ParquetKTGameSource, session)
    val ktGameCreationSkippedDF = readOptional(ParquetKTGameSkippedSource, session, isMandatory = false)

    val ktgCreatedTransDF = ktGameCreatedDF.map(
      _.selectGameColumns
        .appendFactDateColumns(FactKtGame, isFact = true)
        .selectColumnsWithMapping(StagingKtGame)
        .appendTimestampColumns(FactKtGame, isFact = true)
        .withColumn("ktg_kt_collection_id", col("ktg_kt_collection_id").cast(LongType))
        .withColumn("ktg_min_question", col("ktg_min_question").cast(IntegerType))
        .withColumn("ktg_max_question", col("ktg_max_question").cast(IntegerType))
        .withColumn("ktg_num_key_terms", col("ktg_num_key_terms").cast(IntegerType))
        .withColumn("ktg_date_dw_id", col("ktg_date_dw_id").cast(IntegerType))
        .withColumn("ktg_question_time_allotted", col("ktg_question_time_allotted").cast(IntegerType))
    )

    val ktGCreationSkippedTransDF = ktGameCreationSkippedDF.map(
      _.selectGameColumns
        .appendFactDateColumns(FactKtGameSkipped, isFact = true)
        .selectColumnsWithMapping(StagingKtGameSkipped)
        .appendTimestampColumns(FactKtGameSkipped, isFact = true)
        .withColumn("ktgskipped_kt_collection_id", col("ktgskipped_kt_collection_id").cast(LongType))
        .withColumn("ktgskipped_min_question", col("ktgskipped_min_question").cast(IntegerType))
        .withColumn("ktgskipped_max_question", col("ktgskipped_max_question").cast(IntegerType))
        .withColumn("ktgskipped_num_key_terms", col("ktgskipped_num_key_terms").cast(IntegerType))
        .withColumn("ktgskipped_date_dw_id", col("ktgskipped_date_dw_id").cast(IntegerType))
        .withColumn("ktgskipped_question_time_allotted", col("ktgskipped_question_time_allotted").cast(IntegerType))
    )

    val ktgCreatedParquetSink = ktGameCreatedDF.map(_.toParquetSink(ParquetKTGameSource))
    val ktgCreatedRedshiftSink = ktgCreatedTransDF.map(_.toInsertIfNotExistsSink(RedshiftKTGameSink, ktgameTableName, uniqueIdsStatement = redshiftMatchCondition, isStaging = true, filterNot = filterNot))
    val ktgCreatedDeltaSink = ktgCreatedTransDF.map(
      _.renameUUIDcolsForDelta(Some("ktg_"))
        .withColumn("eventdate", date_format(col("ktg_created_time"), "yyyy-MM-dd"))
        .withColumn("ktg_date_dw_id", col("ktg_date_dw_id").cast(StringType))
        .toInsertIfNotExists(matchConditions = deltaMatchCondition)
        .toSink(DeltaKTGame, FactKtGame)
    )

    val ktGCreationSkippedParquetSink = ktGameCreationSkippedDF.map(_.toParquetSink(ParquetKTGameSkippedSource))
    val ktGCreationSkippedRedshiftSink = ktGCreationSkippedTransDF.map(_.toInsertIfNotExistsSink(RedshiftKTGameSkippedSink, ktgameSkippedTableName, uniqueIdsStatement = redshiftKtgSkippedMatchCondition, isStaging = true, filterNot = filterNot))
    val ktGCreationSkippedDeltaSink = ktGCreationSkippedTransDF.map(
      _.renameUUIDcolsForDelta(Some("ktgskipped_"))
        .withColumn("eventdate", date_format(col("ktgskipped_created_time"), "yyyy-MM-dd"))
        .withColumn("ktgskipped_date_dw_id", col("ktgskipped_date_dw_id").cast(StringType))
        .toInsertIfNotExists(matchConditions = deltaKtgSkippedMatchCondition)
        .toSink(DeltaKTGameSkippedSource, FactKtGameSkipped)
    )

    (ktgCreatedParquetSink ++ ktgCreatedRedshiftSink ++ ktgCreatedDeltaSink
      ++ ktGCreationSkippedParquetSink ++ ktGCreationSkippedRedshiftSink ++ ktGCreationSkippedDeltaSink).toList

  }

}

object KTGame {
  def main(args: Array[String]): Unit = {
    new KTGameTransformer(ktGameService, SparkSessionUtils.getSession(ktGameService)).run
  }
}
