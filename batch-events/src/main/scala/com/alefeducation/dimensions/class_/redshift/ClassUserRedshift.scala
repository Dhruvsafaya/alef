package com.alefeducation.dimensions.class_.redshift

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.dimensions.class_.redshift.ClassUserRedshift.{classUserSinkName, classUserSourceName}
import com.alefeducation.dimensions.class_.transform.ClassUserTransform.classUserTransformSink
import com.alefeducation.service.DataSink
import com.alefeducation.util.Helpers.RedshiftClassUserSink
import com.alefeducation.util.{Resources, SparkSessionUtils}
import org.apache.spark.sql.{DataFrame, SparkSession}

class ClassUserRedshift(val session: SparkSession, val service: SparkBatchService) {

  import session.implicits._
  def transform(): Option[Sink] = {
    val classUser = service.readOptional(classUserSourceName, session)

    classUser.map(_ => {
      val userHead = classUser.head.drop($"eventType")
      val scdOptions = setScdTypeIIUpdate(userHead)
      DataSink(classUserSinkName, userHead, scdOptions)
    })
  }

  private def setScdTypeIIUpdate(userDf: DataFrame): Map[String, String] = {
    val dataFrame = userDf

    val stagingTable = s"staging_class_user"
    val targetTableRel = s"rel_class_user"
    val targetTableDim = s"dim_class_user"

    val idClassCol = "class_uuid"
    val idUserCol = "user_uuid"
    val roleCol = "role_uuid"

    val dimSchema = Resources.redshiftSchema()
    val relSchema = Resources.redshiftStageSchema()
    val dfCols = dataFrame.columns.mkString(", ")

    val updateActiveUntil =
      s"""
        begin transaction;

        UPDATE $relSchema.$targetTableRel
        SET
          class_user_status = 2,
          class_user_active_until = st.class_user_created_time
        FROM $relSchema.$targetTableRel t
          JOIN (
            SELECT $idClassCol, $roleCol, MAX(class_user_created_time) as class_user_created_time
            FROM $dimSchema.$stagingTable
            GROUP BY $idClassCol, $roleCol
          ) st ON st.$roleCol = t.$roleCol and st.$idClassCol = t.$idClassCol
        WHERE t.class_user_created_time <= st.class_user_created_time
          and t.class_user_active_until is null
          and t.$roleCol = 'TEACHER';

        UPDATE $relSchema.$targetTableRel
        SET
          class_user_status = 2,
          class_user_active_until = st.class_user_created_time
        FROM $relSchema.$targetTableRel t
          JOIN (
            SELECT $roleCol, $idClassCol, $idUserCol, MAX(class_user_created_time) as class_user_created_time
            FROM $dimSchema.$stagingTable
            GROUP BY $roleCol, $idClassCol, $idUserCol
          ) st ON st.$roleCol = t.$roleCol
            and st.$idClassCol = t.$idClassCol
            and st.$idUserCol = t.$idUserCol
        WHERE t.class_user_created_time <= st.class_user_created_time
          and t.class_user_active_until is null
          and t.$roleCol = 'STUDENT';

        update $dimSchema.$targetTableDim
        set
          class_user_status = 2,
          class_user_active_until = st.class_user_created_time
        from $dimSchema.$targetTableDim t
        join $relSchema.rel_dw_id_mappings c on t.class_user_class_dw_id = c.dw_id and c.entity_type = 'class'
        join $dimSchema.dim_role r on t.class_user_role_dw_id = r.role_dw_id
        join(
          SELECT $idClassCol, $roleCol, MAX(class_user_created_time) as class_user_created_time
          FROM $dimSchema.$stagingTable
          GROUP BY $idClassCol, $roleCol
        ) st on c.id = st.$idClassCol and r.role_name = st.$roleCol
        where t.class_user_created_time <= st.class_user_created_time
          and t.class_user_active_until is null and
        r.role_name = 'TEACHER';

        update $dimSchema.$targetTableDim
        set
          class_user_status = 2,
          class_user_active_until = st.class_user_created_time
        from $dimSchema.$targetTableDim t
        join $relSchema.rel_dw_id_mappings c on t.class_user_class_dw_id = c.dw_id and c.entity_type = 'class'
        join $dimSchema.dim_role r on t.class_user_role_dw_id = r.role_dw_id
        join $relSchema.rel_user u on t.class_user_user_dw_id = u.user_dw_id
        join(
          SELECT $idClassCol, $idUserCol, $roleCol, MAX(class_user_created_time) as class_user_created_time
          FROM $dimSchema.$stagingTable
          GROUP BY $idClassCol, $idUserCol, $roleCol
        ) st on c.id = st.$idClassCol and u.user_id = st.$idUserCol and r.role_name = st.$roleCol
        where t.class_user_created_time <= st.class_user_created_time
          and t.class_user_active_until is null and
        r.role_name = 'STUDENT';

        insert into $relSchema.$targetTableRel($dfCols)
        select $dfCols
        from $dimSchema.$stagingTable;
        drop table $dimSchema.$stagingTable;
        end transaction
       """.stripMargin

    Map("dbtable" -> s"$dimSchema.$stagingTable", "postactions" -> updateActiveUntil)
  }

}
object ClassUserRedshift {
  val classUserSourceName: String = classUserTransformSink
  val classUserSinkName: String = RedshiftClassUserSink
  private val classUserServiceName: String = "redshift-class-user"

  private val classUserSession: SparkSession = SparkSessionUtils.getSession(classUserServiceName)
  private val classUserService = new SparkBatchService(classUserServiceName, classUserSession)

  def main(args: Array[String]): Unit = {
    val transformer = new ClassUserRedshift(classUserSession, classUserService)
    classUserService.run(transformer.transform())
  }
}
