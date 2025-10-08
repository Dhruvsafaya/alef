package com.alefeducation.util

import com.launchdarkly.sdk.LDContext
import com.launchdarkly.sdk.server.{LDClient, LDConfig}
import org.apache.log4j.Logger

class FeatureService(ldClient: LDClient) {

  def isEnabled(featureName: String): Boolean = {
    val context = LDContext.builder("alef-data-platform").build()
    ldClient.boolVariation(featureName, context, false)
  }

  def isEnabledWithContext(featureName: String, context: LDContext): Boolean = {
    ldClient.boolVariation(featureName, context, false)
  }

  def isProductMaxIdConsistencyCheckEnabled: Boolean = isEnabled("ALEF-75213-product-max-id-consistency-check")
}


object FeatureService {

  val log: Logger = Logger.getLogger(FeatureService.getClass.getName)

  /**
   * Creates a FeatureService instance for feature toggling.
   *
   * @param secretManager Provides the LaunchDarkly SDK key.
   * @param fn Function to create an LDClient instance, defaults to creating a new LDClient.
   * @return Option[FeatureService] if initialized successfully, None if initialization fails or FeatureService is disabled.
   */
  def apply(secretManager: SecretManager, fn: (String, LDConfig) => LDClient = (s, c) => new LDClient(s, c)): Option[FeatureService] = {
    val sdkKey = secretManager.getLaunchDarklySdkKey
    val config: LDConfig = new LDConfig.Builder().build()
    val ldClient = sdkKey.map(fn(_, config))
    if (!ldClient.exists(_.isInitialized)) {
      val errorMsg = "LaunchDarkly client is NOT initialize."
      log.error(errorMsg)
      None
    } else {
      val fs = ldClient.map(new FeatureService(_))
      log.info("FeatureService with LaunchDarkly client is created and initialized")
      fs
    }
  }
}
