package com.alefeducation.schema.ccl

case class CategoryCreated(id: String, code: String, name: String, description: String, occurredOn: Long)
case class CategoryUpdated(id: String, name: String, description: String, occurredOn: Long)
case class CategoryDeleted(id: String, occurredOn: Long)
