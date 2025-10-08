package com.alefeducation.dimensions.school.transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.dimensions.school.transform.SchoolOrganizationTransform.SchoolOrganizationTransformSink
import com.alefeducation.schema.admin.{ContentRepositoryRedshift, DwIdMapping, OrganizationRedshift}
import com.alefeducation.service.DataSink
import com.alefeducation.util.Helpers._
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{lit, when}


class SchoolOrganizationTransform(val session: SparkSession, val service: SparkBatchService) {

  import com.alefeducation.util.BatchTransformerUtility._
  import com.alefeducation.util.DataFrameUtility._
  import session.implicits._

  def transform(): Option[DataSink] = {
    val schoolMutatedSource = service.readOptional(ParquetSchoolSource, session)

    val dwIdMapping = service
      .readFromRedshift[ContentRepositoryRedshift](RedshiftContentRepositorySource)
      .cache()

    val organization = service.readFromRedshift[OrganizationRedshift](RedshiftOrganizationSource).cache()

    val schoolWithOrganization = schoolMutatedSource.map(
      _.transformIfNotEmpty(enrichSchool(_, dwIdMapping, organization))
    )

    schoolWithOrganization.map(df => { DataSink(SchoolOrganizationTransformSink, df) })
  }

  private def enrichSchool(school: DataFrame, dwIdMappingDf: DataFrame, organization: DataFrame): DataFrame = {
    val enrichedSchool = school
      .addColIfNotExists("contentRepositoryId", StringType)
      .addColIfNotExists("organisationGlobal", StringType)

    val schoolEnrichedWithDwIds = enrichedSchool
      .join(dwIdMappingDf, enrichedSchool("contentRepositoryId") === dwIdMappingDf("content_repository_id"), "left")
      .join(organization, enrichedSchool("organisationGlobal") === organization("organization_code"), "left")

    schoolEnrichedWithDwIds
      .withColumn("_is_complete",
        when(
          $"content_repository_dw_id".isNotNull and $"organization_dw_id".isNotNull, lit(true))
          .otherwise(false)
      )
  }

}

object SchoolOrganizationTransform {
  val SchoolOrganizationTransformService = "transform-school-organization"
  val SchoolOrganizationTransformSink = "transformed-school-organization-sink"

  val session: SparkSession = SparkSessionUtils.getSession(SchoolOrganizationTransformService)
  val service = new SparkBatchService(SchoolOrganizationTransformService, session)

  def main(args: Array[String]): Unit = {
    val transformer = new SchoolOrganizationTransform(session, service)
    service.run(transformer.transform())
  }

}
