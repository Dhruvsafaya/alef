package com.alefeducation.dimensions.admin

import com.alefeducation.bigdata.Sink
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcType}
import org.apache.spark.sql.types._
import com.alefeducation.util.DefaultAzureTokenProvider
import com.alefeducation.models.AdminUserModel._
import com.alefeducation.models.StudentModel._
import com.alefeducation.models.UserModel.User
import com.alefeducation.service.SparkBatchService
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Constants.UserCreated
import com.alefeducation.util.DataFrameUtility._
import com.alefeducation.util.Helpers._
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.alefeducation.bigdata.batch.{DataSink=>DataSinkAdapter}


  class AdminUsersDimensionTransformer(override val name: String, override implicit val session: SparkSession) extends SparkBatchService {

    object CustomSQLServerDialect extends JdbcDialect {
      override def canHandle(url: String): Boolean = url.startsWith("jdbc:sqlserver")

      override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
        case StringType => Some(JdbcType("VARCHAR(MAX)", java.sql.Types.VARCHAR))
        case TimestampType => Some(JdbcType("DATETIME2(2)", java.sql.Types.TIMESTAMP))
        case _ => None
      }
    }
    org.apache.spark.sql.jdbc.JdbcDialects.registerDialect(CustomSQLServerDialect)
  import session.implicits._
  def test() : Unit = {
    val allSource = readWithExtraProps(ParquetUserSource, session, isMandatory = false, extraProps = List(("mergeSchema", "true"))).map(_.addColIfNotExists("excludeFromReport", BooleanType)).map(_.na.fill(value = false, Array("excludeFromReport")))
//    val adminSchoolChangeSource = readWithExtraProps(ParquetAdminSchoolMovedSource, session, isMandatory = false, extraProps = List(("mergeSchema", "true"))).map(_.addColIfNotExists("excludeFromReport", BooleanType)).map(_.na.fill(value = false, Array("excludeFromReport")))
    val adminUserSource = allSource.map(_.filter($"role" =!= "TEACHER" and $"role" =!= "STUDENT" and $"role" =!= "GUARDIAN"))
//    val deletedAdmin = readWithExtraProps(ParquetUserDeletedSource, session, isMandatory = false, extraProps = List(("mergeSchema", "true"))).map(_.addColIfNotExists("excludeFromReport", BooleanType)).map(_.na.fill(value = false, Array("excludeFromReport")))
//    val redshiftRelUser: DataFrame = readFromFabric[User](RedshiftUserSink)
//
//    val emptyDf = Seq.empty[UserId].toDF
//    val requiredUUIDS = AdminHelper.getAllIncomingIds(adminUserSource, adminSchoolChangeSource, deletedAdmin, emptyDf)
//
//    val adminUserUpdated = adminUserSource.flatMap(_.filter($"eventType" =!= UserCreated).checkEmptyDf)
//    //To handle the case when update event arrives without created event
//
//    //Transform
//    val adminCreated = new AdminCreatedTransformer()(session)
//    val adminsNotCreatedYet = adminCreated.getAdminsNotCreatedYet(adminUserUpdated, redshiftRelUser).map(_.cache())
//    val userCreatedTransformed = adminCreated.transformAdminCreated(adminUserSource, adminsNotCreatedYet)
//    val cachedDf = combineCreatedRelAndDimData(userCreatedTransformed)
//      .join(requiredUUIDS, $"admin_uuid" === $"uuid").drop("uuid").cache()
//
//    val userCreatedSinks = adminCreated.getAdminCreatedSinks(userCreatedTransformed, adminsNotCreatedYet)
//
//    val adminUsersOnlyForUpdates = adminUserSource
//      .map(df1 =>
//        adminsNotCreatedYet match {
//          case Some(df2) => df1.join(df2, Seq("uuid"), "leftanti").select(df1("*"))
//          case None => df1.select(df1("*"))
//        })

//    val userSCDSinks = new AdminSCDTransformer()(session)
//      .transformAdminSCD(adminUsersOnlyForUpdates, adminSchoolChangeSource, cachedDf)
//    val userDeletedSinks = new AdminDeletedTransformer()(session).transformAdminDeleted(deletedAdmin, cachedDf)

    //Write
    val adminUserParquetSink = adminUserSource.map(_.toParquetSink(ParquetUserSource))
//    val adminSchoolChangeParquetSink = adminSchoolChangeSource.map(_.toParquetSink(ParquetAdminSchoolMovedSource))

//    val (key, url) = com.alefeducation.util.AWSSecretManager.getFabricUrlProp
//    val (token, value) = com.alefeducation.util.AWSSecretManager.getFabricToken
//    val resource = com.alefeducation.util.Resources.getResource(name, "sink").find(_.name == "redshift-admin-rel").get
//    val userSCDSink = userSCDSinks.collect { case s: com.alefeducation.service.DataSink => s }.map { s =>
//      DataSinkAdapter(session, s.sinkName, s.dataFrame, writerOptions = s.options.updated(key, url).updated(token, value))
//    }
//    val props = (resource.props ++ userSCDSink(0).config.writerOptions)
//    println(userSCDSink(0).write(resource))
//    props.get("postactions") match {
//      case Some(v) => {
//        println("performing post actions")
//        com.alefeducation.util.AWSSecretManager.getFabricDriverManager.execute(v)
//      }
//      case None    => println("No post actions found")
//    }
    println(adminUserParquetSink)
  }
  override def transform(): List[Sink] = {
    //Read
    val allSource = readWithExtraProps(ParquetUserSource, session, isMandatory = false, extraProps = List(("mergeSchema", "true"))).map(_.addColIfNotExists("excludeFromReport", BooleanType)).map(_.na.fill(value = false,Array("excludeFromReport")))
    val adminSchoolChangeSource = readWithExtraProps(ParquetAdminSchoolMovedSource, session, isMandatory = false, extraProps = List(("mergeSchema", "true"))).map(_.addColIfNotExists("excludeFromReport", BooleanType)).map(_.na.fill(value = false,Array("excludeFromReport")))
    val adminUserSource = allSource.map(_.filter($"role" =!= "TEACHER" and $"role" =!= "STUDENT" and $"role" =!= "GUARDIAN"))
    val deletedAdmin = readWithExtraProps(ParquetUserDeletedSource, session, isMandatory = false, extraProps = List(("mergeSchema", "true"))).map(_.addColIfNotExists("excludeFromReport", BooleanType)).map(_.na.fill(value = false,Array("excludeFromReport")))
    val redshiftRelUser: DataFrame = readFromFabric[User](RedshiftUserSink)

    val emptyDf = Seq.empty[UserId].toDF
    val requiredUUIDS = AdminHelper.getAllIncomingIds(adminUserSource, adminSchoolChangeSource, deletedAdmin, emptyDf)

    val adminUserUpdated = adminUserSource.flatMap(_.filter($"eventType" =!= UserCreated).checkEmptyDf)
    //To handle the case when update event arrives without created event

    //Transform
    val adminCreated = new AdminCreatedTransformer()(session)
    val adminsNotCreatedYet = adminCreated.getAdminsNotCreatedYet(adminUserUpdated, redshiftRelUser).map(_.cache())
    val userCreatedTransformed = adminCreated.transformAdminCreated(adminUserSource, adminsNotCreatedYet)
    val cachedDf = combineCreatedRelAndDimData(userCreatedTransformed)
      .join(requiredUUIDS, $"admin_uuid" === $"uuid").drop("uuid").cache()

    val userCreatedSinks = adminCreated.getAdminCreatedSinks(userCreatedTransformed, adminsNotCreatedYet)

    val adminUsersOnlyForUpdates = adminUserSource
      .map(df1 =>
        adminsNotCreatedYet match {
          case Some(df2) => df1.join(df2, Seq("uuid"), "leftanti").select(df1("*"))
          case None =>  df1.select(df1("*"))
    })

    val userSCDSinks = new AdminSCDTransformer()(session)
      .transformAdminSCD(adminUsersOnlyForUpdates, adminSchoolChangeSource, cachedDf)
    val userDeletedSinks = new AdminDeletedTransformer()(session).transformAdminDeleted(deletedAdmin, cachedDf)

    //Write
    val adminUserParquetSink = adminUserSource.map(_.toParquetSink(ParquetUserSource))
    val adminSchoolChangeParquetSink = adminSchoolChangeSource.map(_.toParquetSink(ParquetAdminSchoolMovedSource))
    (adminUserParquetSink ++ adminSchoolChangeParquetSink).toList ++ userCreatedSinks ++ userSCDSinks ++ userDeletedSinks
  }

  private def combineCreatedRelAndDimData(createdSource: Option[DataFrame]): DataFrame = {
    val createdDf = createdSource.getOrElse(Seq.empty[AdminRel].toDF.drop("rel_admin_id"))
    val adminRSDimSource = readFromFabric[AdminDim](RedshiftAdminUserDimSink).cache()
    println(adminRSDimSource.count())
    val adminRSDimDf = {
      val roleDf = readFromFabric[RoleDim](RedshiftRoleSource)
      val schoolDf = readFromFabric[SchoolDim](RedshiftSchoolSink).select($"school_dw_id", $"school_id")
      adminRSDimSource
        .join(schoolDf, col("admin_school_dw_id") === col("school_dw_id"), "inner")
        .join(roleDf, col("admin_role_dw_id") === col("role_dw_id"), "inner")
        .filter($"admin_active_until".isNull)
        .addColIfNotExists("admin_exclude_from_report", BooleanType)
        .select(
          $"admin_created_time",
          $"admin_updated_time",
          $"admin_deleted_time",
          $"admin_dw_created_time",
          $"admin_dw_updated_time",
          $"admin_active_until",
          $"admin_status",
          $"admin_id".as("admin_uuid"),
          $"admin_avatar",
          $"admin_onboarded",
          $"school_id".as("school_uuid"),
          $"role_name".as("role_uuid"),
          $"admin_expirable",
          $"admin_exclude_from_report"
        )
    }
    val adminRSRelSource = readFromFabric[AdminRel](RedshiftAdminUserSink).addColIfNotExists("admin_exclude_from_report", BooleanType).cache
    val adminRSRelDf = if (!isEmpty(adminRSRelSource)) {
      adminRSRelSource
        .filter($"admin_active_until".isNull)
        .drop("rel_admin_id")
    } else {
      Seq.empty[AdminRel].toDF.drop("rel_admin_id")
    }
    val redshiftDf = adminRSDimDf.unionByName(createdDf.addColIfNotExists("admin_exclude_from_report", BooleanType)).unionByName(createdDf.addColIfNotExists("admin_exclude_from_report", BooleanType))
    redshiftDf
  }

}

object AdminUsersDimension {

  val session: SparkSession = SparkSessionUtils.getSession(AdminUsersDimensionName)

  def main(args: Array[String]): Unit = {

    new AdminUsersDimensionTransformer(AdminUsersDimensionName, session).run

    //    import java.util.jar.JarFile
    import java.io.InputStreamReader

    //            val jarPath = "C:\\Users\\DhruvSafaya\\OneDrive - Xebia\\Documents\\Alef codebase test\\Alef codebase test\\alef-data-platform-develop\\alef-data-platform-develop\\batch-events\\target\\scala-2.12\\batch-events-assembly-0.2.0-SNAPSHOT.jar"

    //        val jar = new JarFile(jarPath)

    //    val jarFile = new JarFile(jarPath)
    //
    //    val entries = jarFile.entries()
    //    while (entries.hasMoreElements) {
    //      val entry = entries.nextElement()
    //      if (entry.getName.contains("META")) {
    //        println(entry.getName)
    //      }
    //    }
    //
    //    jarFile.close()
    //  }
    //    import java.util.jar.JarFile
    //    import java.io.InputStreamReader
    //    import com.typesafe.config.{Config, ConfigFactory}
    //
    //
    //        val jarPath = "C:\\Users\\DhruvSafaya\\OneDrive - Xebia\\Documents\\Alef codebase test\\Alef codebase test\\alef-data-platform-develop\\alef-data-platform-develop\\batch-events\\target\\scala-2.12\\batch-events-assembly-0.2.0-SNAPSHOT.jar"
    //
    //    val jar = new JarFile(jarPath)
    //
    //    // find application.conf inside the jar
    //    val entry = jar.getJarEntry("application.conf")
    //    if (entry == null) {
    //      println("application.conf not found in JAR!")
    //      return
    //    }
    //
    //    val inputStream = jar.getInputStream(entry)
    //
    //    // Parse with Typesafe Config
    //    val reader = new InputStreamReader(inputStream)
    //    val config: Config = ConfigFactory.parseReader(reader).resolve()
    //
    //    println("=== Parameters inside application.conf ===")
    //    config.entrySet().forEach { entry =>
    //      println(s"${entry.getKey} = ${entry.getValue.unwrapped()}")
    //    }
    //
    //    reader.close()
    //    jar.close()


  }
}
