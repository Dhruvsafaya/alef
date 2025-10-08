package com.alefeducation.schema.skill

case class SkillCreatedEvent(id: String, name: String, code: String, occurredOn: String)

case class SkillDeletedEvent(id: String, occurredOn: String)