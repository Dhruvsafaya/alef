package com.alefeducation.dimensions.student

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.dimensions.student.StudentRedshift.{StudentRedshiftSink, StudentTransformedSource}
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Constants.AdminStudent
import com.alefeducation.util.Helpers.StudentEntity
import com.alefeducation.util.{Resources, SparkSessionUtils}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.MetadataBuilder
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.immutable.Map

class StudentRedshift(val session: SparkSession, val service: SparkBatchService) {
  private val colLength = 512
  private val metadata = new MetadataBuilder().putLong("maxlength", colLength).build()

  def transform(): Option[Sink] = {
    val createdSource = service.readOptional(StudentTransformedSource, session)
    val transformedSource = createdSource.map(_.withColumn("student_special_needs", col("student_special_needs")
      .as("student_special_needs", metadata)))
    transformedSource.map(_.toRedshiftSCDSink(StudentRedshiftSink, AdminStudent, StudentEntity, scdUpdateOption))
  }

  private def scdUpdateOption(dataFrame: DataFrame, name: String): Map[String, String] = {
    val targetTableDim = s"dim_$name"
    val targetTableRel = s"rel_$name"
    val idColRel = "student_uuid"
    val idColDim = "student_dw_id"
    val dimSchema = Resources.redshiftSchema()
    val relSchema = Resources.redshiftStageSchema()
    val stagingTable = s"staging_$targetTableDim"
    val insertCols = dataFrame.columns.mkString(", ")
    val updateStatusAndActiveUntil =
      s"""
       begin transaction;
       update $relSchema.$targetTableRel set ${name}_status = 2
       from $relSchema.$targetTableRel target
       join $dimSchema.$stagingTable staging on staging.$idColRel = target.$idColRel
       where target.${name}_created_time <= staging.${name}_created_time
       and target.${name}_status = 1 and target.${name}_active_until is null;

       update $relSchema.$targetTableRel set ${name}_active_until = staging.${name}_created_time
       from $relSchema.$targetTableRel target
       join $dimSchema.$stagingTable staging on staging.$idColRel = target.$idColRel
       where target.${name}_created_time <= staging.${name}_created_time
       and target.${name}_active_until is null;

       update $dimSchema.$targetTableDim set ${name}_status = 2
       from $dimSchema.$targetTableDim target
       join $relSchema.rel_user user1 on user1.user_dw_id = target.$idColDim
       join $dimSchema.$stagingTable staging on user1.user_id = staging.$idColRel
       where target.${name}_created_time <= staging.${name}_created_time and
       target.${name}_status = 1 and target.${name}_active_until is null;

       update $dimSchema.$targetTableDim set ${name}_active_until = staging.${name}_created_time
       from $dimSchema.$targetTableDim target
       join $relSchema.rel_user user1 on user1.user_dw_id = target.$idColDim
       join $dimSchema.$stagingTable staging on user1.user_id = staging.$idColRel
       where target.${name}_created_time <= staging.${name}_created_time
       and target.${name}_active_until is null;

       insert into $relSchema.$targetTableRel ($insertCols)
       (select $insertCols from $dimSchema.$stagingTable);
       DROP table $dimSchema.$stagingTable;
       end transaction
     """.stripMargin
    Map("dbtable" -> s"$dimSchema.$stagingTable", "postactions" -> updateStatusAndActiveUntil)
  }
}

object StudentRedshift {
  val StudentRedshiftService = "redshift-student"
  val StudentTransformedSource = "transform-student-rel"
  val StudentRedshiftSink = "redshift-student-rel"

  private val session = SparkSessionUtils.getSession(StudentRedshiftService)
  val service = new SparkBatchService(StudentRedshiftService, session)

  def main(args: Array[String]): Unit = {
    val transformer = new StudentRedshift(session, service)
    service.run(transformer.transform())
  }
}
