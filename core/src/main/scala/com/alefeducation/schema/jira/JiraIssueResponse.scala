package com.alefeducation.schema.jira

import io.circe.Json

case class JiraIssueResponse(
                         startAt: Int,
                         maxResults: Int,
                         total: Int,
                         issues: List[Issue]
                       )

case class Issue(expand: String, id: String, self: String, key: String, fields: Json)
