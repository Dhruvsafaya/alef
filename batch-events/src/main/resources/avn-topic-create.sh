#!/bin/bash

for i in \
"alef.base.learningObjectiveMutated" \
"alef.data.connector.xApiStatementPublished" \
"alef.dataconnector.v2.xapi.published" \
"alef.iam.auth" \
"alef.learning.learningPath" \
"alef.learning.session" \
"alef.outcome.awardedStar" \
"alef.practice.session" \
"alef.school.classMutated" \
"alef.school.gradeMutated" \
"alef.school.guardianAssociationsUpdated" \
"alef.school.guardianDeleted" \
"alef.school.schoolMutated" \
"alef.school.studentMutated" \
"alef.school.subjectMutated" \
"alef.school.teacherMutated" \
"alef.tutordhabi.conversationOccurred"
 do
   avn service topic-create kafka-prod --partitions 3 --replication 3 --retention 168 --cleanup-policy delete $i
   echo "topic $i created!"
 done
