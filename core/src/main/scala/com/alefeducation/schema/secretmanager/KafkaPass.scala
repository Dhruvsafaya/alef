package com.alefeducation.schema.secretmanager

import io.circe.{Decoder, Encoder}

final case class KafkaPass(truststorePass: String, keystorePass: String, sslKeyPass: String)

object KafkaPassCodec {
  implicit val decodeUser: Decoder[KafkaPass] =
    Decoder.forProduct3("truststore_pass", "keystore_pass", "ssl_key_pass")(KafkaPass.apply)

  implicit val encodeUser: Encoder[KafkaPass] =
    Encoder.forProduct3("truststore_pass", "keystore_pass", "ssl_key_pass")(kp =>
      (kp.truststorePass, kp.keystorePass, kp.sslKeyPass))
}