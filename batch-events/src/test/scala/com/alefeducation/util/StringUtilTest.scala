package com.alefeducation.util

import org.scalatest.funsuite.AnyFunSuite
import com.alefeducation.util.StringUtilities._

class StringUtilTest extends AnyFunSuite {


  test("testing cameCase to snakeCase function" ) {

    assert(lowerCamelToSnakeCase("thisIsA1Test").equals("this_is_a_1_test"))

    assert(lowerCamelToSnakeCase("camelCaseF").equals("camel_case_f"))
  }

  test("testing a string is it has all alphabets" ) {

    assert(isAlpha("thisIsA1Test").equals(false))
    assert(isAlpha("thisIsAOneTest").equals(true))
    assert(isAlpha("**SomeText").equals(false))
    assert(isAlpha("Some@Test-Text").equals(false))
    assert(isAlpha("somelowercasetext").equals(true))
  }

  test("replaceSpecChars should reduce empty characters and trim string") {

    assert(replaceSpecChars(" hello     world!   ").equals("hello world!"))
  }
}
