package com.alefeducation.schema.ccl

case class CCLSKillTranslation(uuid: String, languageCode: String, name: String, description: String)
case class CCLSkillMutated(id: String, code: String, name: String, description: String, subjectId: Long, translations: List[CCLSKillTranslation], occurredOn: Long)
case class CCLSkillDeleted(id: String, occurredOn: Long)
case class CCLSkillLinkToggle(skillId: String, skillCode: String, nextSkillId: String, nextSkillCode: String, occurredOn: Long)
case class CCLSkillLinkToggleV2(skillId: String, skillCode: String, previousSkillId: String, previousSkillCode: String, occurredOn: Long)
case class CCLSkillCategoryLinkToggle(skillId: String, skillCode: String, categoryId: String, occurredOn: Long)
