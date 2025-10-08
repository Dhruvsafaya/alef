package com.alefeducation.schema.challengeGame

case class AlefGameChallengeProgressEvent(id: String,
                                    state: String,
                                    studentId: String,
                                    gameId: String,
                                    schoolId: String,
                                    grade: String,
                                    organization: String,
                                    academicYearTag: String,
                                    score: Int,
                                    academicYearId: String,
                                    occurredOn: String,
                                    playerSessionId: String,
                                    gameConfig: String)