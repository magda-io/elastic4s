package com.sksamuel.elastic4s.requests.searchPipeline

import com.sksamuel.elastic4s.json.XContentFactory
import com.sksamuel.elastic4s.{ElasticRequest, HttpEntity}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PutSearchPipelineRequestHandlerTest
    extends AnyFlatSpec
    with SearchPipelineHandlers
    with Matchers {

  import PutSearchPipelineRequestHandler._

  it should "build a search pipeline request with a version and no processors" in {
    val req = PutSearchPipelineRequest(data =
      SearchPipeline(
        "empty",
        description = Some("Do nothing"),
        version = Some(1)
      )
    )
    val correctJson =
      XContentFactory
        .parse("""{"description":"Do nothing","version":1}""")
        .string
    build(req) shouldBe ElasticRequest(
      "PUT",
      "/_search/pipeline/empty",
      HttpEntity(correctJson)
    )
  }

  it should "build a search pipeline request with a normalization-processor processor using the NormalizationProcessor case class" in {
    val req = PutSearchPipelineRequest(
      data = SearchPipeline(
        "nlp-pipeline",
        description = Some("Post processor for hybrid search"),
        version = Some(2332),
        processors = Seq(
          NormalizationProcessor(
            normalizationTechnique = Some(NormalizationTechniqueType.minMax),
            combinationTechnique = Some(
              CombinationTechnique(
                Some(CombinationTechniqueType.arithmeticMean),
                Some(Seq(0.3, 0.7))
              )
            )
          )
        )
      )
    )
    val correctJson =
      XContentFactory
        .parse(
          """
            |{
            |    "version": 2332,
            |    "description": "Post processor for hybrid search",
            |    "phase_results_processors": [
            |      {
            |        "normalization-processor": {
            |          "normalization": {
            |            "technique": "min_max"
            |          },
            |          "combination": {
            |            "technique": "arithmetic_mean",
            |            "parameters": {
            |              "weights": [
            |                0.3,
            |                0.7
            |              ]
            |            }
            |          }
            |        }
            |      }
            |    ]
            |}
            |""".stripMargin.replace("\n", "")
        )
        .string
    build(req) shouldBe ElasticRequest(
      "PUT",
      "/_search/pipeline/nlp-pipeline",
      HttpEntity(correctJson)
    )
  }

  it should "build a search pipeline with custom processer" in {
    val req = PutSearchPipelineRequest(
      SearchPipeline(
        "test-custom",
        processors = Seq(
          CustomSearchPipelineProcessor(
            SearchPipelineProcessorType.SearchRequestProcessor, {
              val b = XContentFactory.jsonBuilder()
              b.startObject("rename_field")
              b.field("field", "message")
              b.field("target_field", "notification")
              b.endObject()
              b.string
            }
          )
        )
      )
    )
    val correctJson = XContentFactory
      .parse(
        """
          |{
          |    "response_processors": [
          |      {
          |       "rename_field": {
          |        "field": "message",
          |        "target_field": "notification"
          |       }
          |      }
          |    ]
          |}
          |""".stripMargin.replace("\n", "")
      )
      .string

    build(req) shouldBe ElasticRequest(
      "PUT",
      "/_search/pipeline/test-custom",
      HttpEntity(correctJson)
    )
  }

}
