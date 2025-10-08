package com.alefeducation.util

import com.alefeducation.exception.{DeleteFilesFailedException, MoveFilesFailedException}
import com.alefeducation.util.Resources._
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.amazonaws.services.s3.model.{CopyObjectRequest, DeleteObjectRequest, ListObjectsV2Request, ListObjectsV2Result}
import com.azure.identity.DefaultAzureCredentialBuilder
import org.apache.spark.sql.SparkSession
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder

import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Try}

object AwsUtils {

  lazy val S3: AmazonS3 = getS3Client

  lazy val S3_BUCKET: String = getString("s3.bucket")

  lazy val stgacc: String = getString("adls.stgacc")

  lazy val container: String = getString("adls.container")

  lazy val ROOT_PATH: String = s"${getString("adls.root")}/$env/"

  lazy val adls_endpoint :String = s"https://$stgacc.dfs.core.windows.net"

  lazy val serviceClient = new DataLakeServiceClientBuilder()
    .endpoint(adls_endpoint)
    .credential(new DefaultAzureCredentialBuilder().build())
    .buildClient()

  def getS3Client: AmazonS3 = {
    Resources.executionEnv() match {
      case "local" =>
        val host = Resources.localS3Endpoint()
        val region = conf.getString("region")
        val configuration = new EndpointConfiguration(s"http://$host/", region)
        AmazonS3ClientBuilder.standard().withEndpointConfiguration(configuration).withPathStyleAccessEnabled(true)
          .withChunkedEncodingDisabled(true).build()
      case _ => AmazonS3ClientBuilder.defaultClient
    }
  }

  def listFiles(path: String, folder: String): Set[String] = {
    import collection.JavaConverters._

    val fileSystemClient = serviceClient.getFileSystemClient(container)

    val directoryClient = fileSystemClient.getDirectoryClient(ROOT_PATH + folder + "/" + path + "/")

    val paths = directoryClient.listPaths().asScala.map(_.getName).toSet.filterNot(_.contains("_spark_metadata"))
    paths
      // import collection.JavaConverters._
//       val listReq = new ListObjectsV2Request().withBucketName(S3_BUCKET).withPrefix(ROOT_PATH + folder + "/" + path + "/")

      // var listObjRes: ListObjectsV2Result = null
      // val result: ArrayBuffer[String] = ArrayBuffer.empty[String]
      // do {
      //   listObjRes = S3.listObjectsV2(listReq)
      //   val lst = listObjRes.getObjectSummaries.asScala.map(_.getKey).filterNot(_.contains("_spark_metadata"))
      //   val token = listObjRes.getNextContinuationToken
      //   listReq.setContinuationToken(token)
      //   result.appendAll(lst)
      // } while (listObjRes.isTruncated)

      // result.toSet
  }

  def moveFiles(files: Set[String], src: String = "incoming", dest: String = "processing"): Unit = {

    val fileSystemClient = serviceClient.getFileSystemClient(container)

    Try(
      files.foreach{file => 
      fileSystemClient.getFileClient(file).rename(container, file.replace(src, dest))
      }) match {
        case Failure(x) => throw MoveFilesFailedException(s"move failed for $files due to " + x.getMessage)
        case _ => println("moved files successfully")
      }
    
    // sourceFileClient.rename(fileSystemName, destPath)

    // println(s"Moved $sourcePath → $destPath successfully")
    // Try(files.toVector.par.foreach { file =>
    //   val copyReq = new CopyObjectRequest(S3_BUCKET, file, S3_BUCKET, file.replace(src, dest))
    //   S3.copyObject(copyReq)
    //   val deleteReq = new DeleteObjectRequest(S3_BUCKET, file)
    //   S3.deleteObject(deleteReq)
    // }) match {
    //   case Failure(x) => throw MoveFilesFailedException(s"move failed for $files due to " + x.getMessage)
    //   case _ =>
    // }
  }

  def deleteFiles(files: Set[String]): Unit = {

    val fileSystemClient = serviceClient.getFileSystemClient(container)

    Try(
      files.foreach{file => 
      fileSystemClient.getFileClient(file).delete()
      }) match {
        case Failure(x) => throw DeleteFilesFailedException(s"delete failed for $files due to " + x.getMessage)
        case _ => println("deleted files successfully")
      }
      
    // Try(files.toVector.par.foreach { file =>
    //   val deleteReq = new DeleteObjectRequest(S3_BUCKET, file)
    //   S3.deleteObject(deleteReq)
    // }) match {
    //   case Failure(x) => throw DeleteFilesFailedException(s"delete failed for $files due to " + x.getMessage)
    //   case _ =>
    // }
  }

  def putDataToS3(dataStream: java.io.InputStream, path: String, metadata: com.amazonaws.services.s3.model.ObjectMetadata): Unit = {
    try {
      AwsUtils.getS3Client.putObject(S3_BUCKET, path, dataStream, metadata)
    }
    catch {
      case e: Exception => {
        throw new RuntimeException(s"Failed to log offset to S3: ${e.getMessage}", e)
      }
    }
  }


  def isConnectionEnabled(session: SparkSession): Boolean = {
    val enableConnection = session.conf.get("_enableConnection", "true")
    enableConnection match {
      case "true" => true
      case _ => false
    }
  }
}
