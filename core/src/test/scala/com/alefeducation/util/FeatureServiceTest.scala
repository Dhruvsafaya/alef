package com.alefeducation.util

import com.launchdarkly.sdk.LDContext
import com.launchdarkly.sdk.server.LDClient
import org.mockito.Mockito.{verify, when}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

class FeatureServiceTest extends AnyFunSuite with Matchers with MockitoSugar {

  val mockLdClient: LDClient = mock[LDClient]
  val mockSecretManager: SecretManager = mock[SecretManager]
  val featureService = new FeatureService(mockLdClient)

  test("FeatureService should evaluate flag") {
    val context = LDContext.builder("alef-data-platform").build()

    when(mockLdClient.boolVariation("new-feature-flag", context, false)).thenReturn(true)

    val result = featureService.isEnabled("new-feature-flag")

    result shouldBe true
    verify(mockLdClient).boolVariation("new-feature-flag", context, false)
  }

  test("FeatureService should evaluate flag with context") {
    val context = LDContext.builder("alef-data-platform")
      .name("test_name")
      .set("attr1", "attr1_value")
      .build()

    when(mockLdClient.boolVariation("new-feature-flag", context, false)).thenReturn(true)

    val result = featureService.isEnabledWithContext("new-feature-flag", context)

    result shouldBe true
    verify(mockLdClient).boolVariation("new-feature-flag", context, false)
  }

  test("should create FeatureService") {
    when(mockSecretManager.getLaunchDarklySdkKey).thenReturn(Some("sdk-key-001"))
    when(mockLdClient.isInitialized).thenReturn(true)

    val fs = FeatureService(mockSecretManager, (_, _) => mockLdClient)

    fs.isDefined shouldBe true
  }

  test("should Not create FeatureService") {
    when(mockSecretManager.getLaunchDarklySdkKey).thenReturn(Some("sdk-key-001"))
    when(mockLdClient.isInitialized).thenReturn(false)

    val fs = FeatureService(mockSecretManager, (_, _) => mockLdClient)

    fs.isEmpty shouldBe true
  }

  test("should return true") {
    val context = LDContext.builder("alef-data-platform").build()
    when(mockLdClient.boolVariation("ALEF-75213-product-max-id-consistency-check", context, false)).thenReturn(true)

    featureService.isProductMaxIdConsistencyCheckEnabled shouldBe true

    verify(mockLdClient).boolVariation("ALEF-75213-product-max-id-consistency-check", context, false)
  }
}
