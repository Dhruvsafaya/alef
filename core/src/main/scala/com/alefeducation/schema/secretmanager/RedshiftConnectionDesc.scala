package com.alefeducation.schema.secretmanager

final case class RedshiftConnectionDesc(username: String, password: String, engine: String, host: String, port: Int, dbClusterIdentifier: String)