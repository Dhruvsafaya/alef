package com.alefeducation.util

import com.alefeducation.schema.secretmanager.KafkaPassCodec._
import com.alefeducation.schema.secretmanager.{InfluxDBCred, JiraCred, KafkaPass, LaunchDarklySdkKey, RedshiftConnectionDesc, ServiceDeskCred}
import com.alefeducation.util.Resources.conf
import io.circe._
import io.circe.generic.auto._
import org.apache.log4j.Logger
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest
import com.azure.identity.DefaultAzureCredentialBuilder
import com.azure.core.credential.TokenRequestContext
import com.microsoft.sqlserver.jdbc.SQLServerStatement

import java.io.{File, FileOutputStream}
import java.nio.file.{Files, Paths}
import java.time.Duration

/**
  * Class for manage secrets from AWS Secret Manager
  *   - certificates
  *   - username/passwords
  */
object AWSSecretManager extends SecretManager {

  val log: Logger = Logger.getLogger(AWSSecretManager.getClass.getName)

  val SSL_SECURITY_PROTOCOL = "SSL"

  private[util] var client: SecretsManagerClient = _
  private[util] var truststorePath: String = ""
  private[util] var keystorePath: String = ""
  private[util] var jobName = ""

  override def init(name: String): Unit = {
    val region: Region = Region.of(conf.getString("region"))
    client = SecretsManagerClient.builder().region(region).build()
    jobName = name
  }

  override def loadCertificates(): Unit = {
    log.info(s"kafka.security.protocol: $getKafkaSecurityProtocol" )
    if (isKafkaSSL) {
      val certsLoc: String = conf.getConfig("kafka").getString("\"certs.loc\"")
      truststorePath = loadCertificate("kafka.truststore", s"$certsLoc/$jobName-truststore.jks")
      log.info(s"Certificate truststore is loaded to $truststorePath")
      keystorePath = loadCertificate("kafka.keystore", s"$certsLoc/$jobName-keystore.p12")
      log.info(s"Certificate keystore is loaded to $keystorePath")
    }
  }

  def certificateProps: List[(String, String)] = List(
    "kafka.ssl.truststore.location" -> truststorePath,
    "kafka.ssl.keystore.location" -> keystorePath
  )

  override def getKafkaProps: List[(String, String)] = {
    if (isKafkaSSL) getKafkaPassProps ++ certificateProps
    else Nil
  }

  override def getInfluxCred: InfluxDBCred = {
    getSecretString("influxdb.cred").map(parseInfluxCred) match {
      case Some(x) => x
      case None => throw new RuntimeException("Error: influxdb.cred is required")
    }
  }

  override def getRedshift: Option[RedshiftConnectionDesc] = {
    getSecretString("redshift.urlkey").map(parseRedshift)
  }

  override def getServiceDeskSecret: ServiceDeskCred = {
    getSecretString("serviceDeskSecretName").map(secret => {
      parser.decode[ServiceDeskCred](secret) match {
        case Right(x) => x
        case Left(e) =>
          log.error("Not been able to parse service desk cred", e)
          throw e
      }
    }).getOrElse(throw new RuntimeException("Can not load service desk credentials from AWS Secret Manager"))
  }

  def getJiraSecret: JiraCred = {
    getSecretString("jiraSecretName").map(secret => {
      parser.decode[JiraCred](secret) match {
        case Right(x) => x
        case Left(e) =>
          log.error("Not been able to parse JIRA credentials", e)
          throw e
      }
    }).getOrElse(throw new RuntimeException("Can not load service desk credentials from AWS Secret Manager"))
  }

  override def getRedshiftUrlProp: (String, String) = {
    getRedshift.map(createRedshiftUrl).getOrElse(("", ""))
  }

  def getFabricUrlProp: (String, String) = {
    createFabricUrl
  }

  override def disposeCertificates(): Unit = {
    if (isKafkaSSL && Files.deleteIfExists(Paths.get(truststorePath))) {
      log.info(s"Certificate truststore is deleted from $truststorePath")
    }
    if (isKafkaSSL && Files.deleteIfExists(Paths.get(keystorePath))) {
      log.info(s"Certificate keystore is deleted from $keystorePath")
    }
  }

  override def getLaunchDarklySdkKey: Option[String] = {
    val response = getSecretString("launch_darkly.sdk.key")
    response.map(parser.decode[LaunchDarklySdkKey](_) match {
      case Right(x) => x.sdkKey
      case Left(e) =>
        log.error("Can not load Launch darkly sdk key from AWS Secret Manager", e)
        e.printStackTrace()
        throw e
    })
  }

  override def close(): Unit = {
    client.close()
  }

  private def parseInfluxCred(secret: String) = {
    val cred = parser.decode[InfluxDBCred](secret) match {
      case Right(x) => x
      case Left(e) =>
        log.error("Can not load Influx credentials from AWS Secret Manager", e)
        throw e
    }
    cred
  }

  private def parseRedshift(secret: String): RedshiftConnectionDesc = {
    parser.decode[RedshiftConnectionDesc](secret) match {
      case Right(x) => x
      case Left(e) =>
        log.error("Can not load Redshift connection description from AWS Secret Manager", e)
        throw e
    }
  }

  def createFabricUrl(): (String, String) = {
    val database = conf.getString("fabric_warehouse")
    val timeOutDurationInSec = Duration.ofMinutes(Resources.conf.getInt("redshift-connection-timeout-min")).getSeconds
    "url" -> s"jdbc:sqlserver://fkif6v2cnurehb3aehge4sy4ui-n6bhxelsnarevo3fmc2j35iht4.datawarehouse.fabric.microsoft.com:1433;databaseName=$database;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.datawarehouse.fabric.microsoft.com;loginTimeout=30;"
  }

  def getFabricDriverManager(): java.sql.Statement = {
    val (token, value) = getFabricToken
    val (url, urlval) = createFabricUrl
    val props = new java.util.Properties()
    props.put("accessToken", value)
    val conn = java.sql.DriverManager.getConnection(urlval, props)
    val stmt = conn.createStatement()
    stmt
  }

  def getFabricToken(): (String, String) = {
    val credential = new DefaultAzureCredentialBuilder().build()

    val scope = "https://database.windows.net/.default"
    val requestContext = new TokenRequestContext().addScopes(scope)

    val token = credential.getToken(requestContext).block()

    "accessToken" -> token.getToken
  }

  private def createRedshiftUrl(rcd: RedshiftConnectionDesc): (String, String) = {
    val database = conf.getString("redshift.database")
    val timeOutDurationInSec = Duration.ofMinutes(Resources.conf.getInt("redshift-connection-timeout-min")).getSeconds
    "url" -> s"jdbc:redshift://${rcd.host}:${rcd.port}/$database?user=${rcd.username}&password=${rcd.password}&LoginTimeout=${timeOutDurationInSec}"
  }

  private def isKafkaSSL = getKafkaSecurityProtocol == SSL_SECURITY_PROTOCOL

  private def getKafkaSecurityProtocol: String = {
    conf.getConfig("kafka").getString("\"kafka.security.protocol\"")
  }

  private def getSecretString(name: String): Option[String] = {
    val secretId = conf.getString(name)
    if (secretId.isEmpty) None
    else {
      val getSecretValueRequest = GetSecretValueRequest.builder()
        .secretId(secretId)
        .build()
      val getSecretValueResponse = client.getSecretValue(getSecretValueRequest)
      Some(getSecretValueResponse.secretString())
    }
  }

  private def loadCertificate(key: String, fileName: String): String = {
    val request = GetSecretValueRequest.builder()
      .secretId(conf.getString(key))
      .build()
    val response = client.getSecretValue(request)
    val byteBuffer = response.secretBinary().asByteBuffer()
    val file = new File(fileName)
    val fc = new FileOutputStream(file).getChannel
    try {
      fc.write(byteBuffer)
    } catch {
      case e: Exception =>
        log.error(s"Can not load $fileName certificate from AWS Secret Manager", e)
        throw e
    } finally {
      fc.close()
    }
    file.getAbsolutePath
  }

  private def getKafkaPassProps: List[(String, String)] = {
    getSecretString("kafka.pass").map(parseSecret).getOrElse(Nil)
  }

  private def parseSecret(secret: String): List[(String, String)] = {
    val kafkaPass = parser.decode[KafkaPass](secret) match {
      case Right(x) => x
      case Left(e) =>
        log.error("Can not load Kafka passwords from AWS Secret Manager", e)
        throw e
    }

    List(
      "kafka.ssl.truststore.password" -> kafkaPass.truststorePass,
      "kafka.ssl.keystore.password" -> kafkaPass.keystorePass,
      "kafka.ssl.key.password" -> kafkaPass.sslKeyPass
    )
  }
}
