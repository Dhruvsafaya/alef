package com.alefeducation.schema.ccl

case class ContentRepository(
                              id: String,
                              name: String,
                              organisation: String,
                              createdAt: Long,
                              occurredOn: Long,
                            )

case class ContentRepositoryCreated(
                              id: String,
                              name: String,
                              organisation: String,
                              createdAt: Long,
                              createdById: String,
                              createdByName: String,
                              updatedAt: Long,
                              updatedById: String,
                              updatedByName: String,
                              occurredOn: Long,
                              description: String,
                              forSchool: String,
                              contentTypes: List[String],
                              subjects: List[String],
                              grades: List[String],
                              contentYear: String
                            )

case class ContentRepositoryDeleted(
                              id: String,
                              name: String,
                              organisation: String,
                              createdAt: Long,
                              createdById: String,
                              createdByName: String,
                              updatedAt: Long,
                              updatedById: String,
                              updatedByName: String,
                              occurredOn: Long
                            )

case class ContentRepositoryMaterialAttachedEvent(
                                                   id: String,
                                                   occurredOn: Long,
                                                   addedMaterials: List[Material],
                                                   updatedAt: Long,
                                                   organisation: String,
                                                   updatedById: String,
                                                 )

case class ContentRepositoryMaterialDetachedEvent(
                                                   id: String,
                                                   occurredOn: Long,
                                                   removedMaterials: List[Material],
                                                   updatedAt: Long,
                                                   organisation: String,
                                                   updatedById: String,
                                                 )

case class Material(id: String, `type`: String)