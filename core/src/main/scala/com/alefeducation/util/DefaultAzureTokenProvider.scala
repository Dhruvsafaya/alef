package com.alefeducation.util

import java.util.Date
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.azurebfs.extensions.CustomTokenProviderAdaptee
import com.azure.identity.DefaultAzureCredentialBuilder
import com.azure.core.credential.{AccessToken, TokenRequestContext}

class DefaultAzureTokenProvider extends CustomTokenProviderAdaptee {

  private val Scope = "https://storage.azure.com/.default"
  @volatile private var credential = new DefaultAzureCredentialBuilder().build()
  @volatile private var lastToken: AccessToken = _

  override def initialize(conf: Configuration, accountName: String): Unit = {
    Option(conf.get("fs.azure.account.oauth2.client.id"))
      .foreach(id => credential = new DefaultAzureCredentialBuilder().managedIdentityClientId(id).build())
  }

  override def getAccessToken(): String = this.synchronized {
    if (lastToken == null || !lastToken.getExpiresAt.isAfter(java.time.OffsetDateTime.now().plusSeconds(120)))
      lastToken = credential.getToken(new TokenRequestContext().addScopes(Scope)).block()
    lastToken.getToken
  }

  override def getExpiryTime(): Date =
    if (lastToken == null) new Date(0L)
    else Date.from(lastToken.getExpiresAt.toInstant)
}


