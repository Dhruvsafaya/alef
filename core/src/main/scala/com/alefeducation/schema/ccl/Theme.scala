package com.alefeducation.schema.ccl

case class ThemeCreated(id: Long,
                        name: String,
                        description: String,
                        boardId: Long,
                        gradeId: Long,
                        subjectId: Long,
                        thumbnailFileName: String,
                        thumbnailContentType: String,
                        thumbnailFileSize: Long,
                        thumbnailLocation: String,
                        thumbnailUpdatedAt: Long,
                        createdAt: Long,
                        occurredOn: Long)

case class ThemeUpdated(id: Long,
                        name: String,
                        description: String,
                        boardId: Long,
                        gradeId: Long,
                        subjectId: Long,
                        thumbnailFileName: String,
                        thumbnailContentType: String,
                        thumbnailFileSize: Long,
                        thumbnailLocation: String,
                        thumbnailUpdatedAt: Long,
                        updatedAt: Long,
                        occurredOn: Long)

case class ThemeDeleted(id: Long, occurredOn: Long)
