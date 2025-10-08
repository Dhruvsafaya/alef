package com.alefeducation.util

object StringUtilities {

  def lowerCamelToSnakeCase(name: String): String =
    "[A-Z\\d]".r.replaceAllIn(name, { m =>
      "_" + m.group(0).toLowerCase()
    })

  def snakeToLowerCamelCase(name: String): String = {
    "_([a-z\\d])".r.replaceAllIn(name, { m =>
      m.group(1).toUpperCase()
    })
  }

  def isAlpha(name: String): Boolean = name.matches("[a-zA-Z]+")

  def replaceSpecChars(s: String): String =
    s.trim.replaceAll("""\s+""", " ")
}
