package com.alefeducation.schema.xapi

import java.sql.Timestamp

case class XAPIStatement(
                          id: String,
                          actor: Actor,
                          verb: Verb,
                          `object`: XApiObject,
                          context: Context,
                          timestamp: Timestamp
                        )

case class Actor(openid: String, objectType: String, account: Map[String, String])

case class Verb(id: String, display: String)

case class XApiObject(id: String, objectType: String, definition: Definition)

case class Context(extensions: Map[String, String])

case class Definition(`type`: String, extensions: Map[String, String])
