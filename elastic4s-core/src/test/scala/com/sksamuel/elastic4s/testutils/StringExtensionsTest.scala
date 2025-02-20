package com.sksamuel.elastic4s.testutils

import com.sksamuel.elastic4s.testutils.StringExtensions.StringOps
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class StringExtensionsTest extends AnyFlatSpec with Matchers {

  it should "convert line endings to Windows style" in {
    "one\r\ntwo\nthree\n".withWindowsLineEndings shouldBe "one\r\ntwo\r\nthree\r\n"
  }

  it should "convert line endings to Unix style" in {
    "one\r\ntwo\nthree\r\n".withUnixLineEndings shouldBe "one\ntwo\nthree\n"
  }

  it should "convert JSON string to compact JSON string without whitespace" in {
    s"""
       | {
       |    "test" : 123,
       |    "test2" : "sdsd sds",
       |     "test array": [1,2,  3 ]
       | }
       |""".stripMargin.toCompactJson shouldBe """{"test":123,"test2":"sdsd sds","test array":[1,2,3]}"""
  }

}
