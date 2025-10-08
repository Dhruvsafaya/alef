package com.alefeducation.schema.auth

case class LoggedInEvent(createdOn: String, uuid: String, schools: List[School], roles: List[String], outsideOfSchool: Boolean)

case class School(uuid: String)
