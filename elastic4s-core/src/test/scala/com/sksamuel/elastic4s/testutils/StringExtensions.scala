package com.sksamuel.elastic4s.testutils

import com.sksamuel.elastic4s.JacksonSupport
import com.sksamuel.elastic4s.json.{
  ObjectValue,
  XContentBuilder,
  XContentFactory
}

object StringExtensions {
  private val LineEndingRegex = s"""(\r\n|\n)"""

  private val WindowsLE = "\r\n"
  private val UnixLE = "\n"

  implicit class StringOps(val target: String) extends AnyVal {
    def withWindowsLineEndings: String =
      target.replaceAll(LineEndingRegex, WindowsLE)

    def withUnixLineEndings: String = target.replaceAll(LineEndingRegex, UnixLE)

    def withoutSpaces: String = target.replaceAll("\\s+", "")

    def toCompactJson: String = new XContentBuilder(
      XContentFactory
        .jsonBuilder()
        .autofield(
          "test-data-root-key",
          JacksonSupport.mapper
            .readValue(target)
        )
        .value
        .asInstanceOf[ObjectValue]
        .map("test-data-root-key")
    ).string
  }

}
