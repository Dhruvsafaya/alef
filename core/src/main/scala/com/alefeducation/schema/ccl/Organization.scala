package com.alefeducation.schema.ccl

case class Organization(id: String,
                        name: String,
                        country: String,
                        code: String,
                        tenantCode: String,
                        isActive: Boolean,
                        createdAt: String,
                        updatedAt: String,
                        createdBy: String,
                        updatedBy: String,
                        occurredOn: Long
                             )

