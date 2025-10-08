package com.alefeducation.schema.secretmanager

final case class InfluxDBCred(user: String, pass: String)

final case class ServiceDeskCred(refresh_token: String, client_id: String, client_secret: String)

final case class JiraCred(jira_access_token: String)
