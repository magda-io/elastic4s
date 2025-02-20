package com.sksamuel.elastic4s.requests.searchPipeline

import com.sksamuel.elastic4s.HttpEntity.StringEntity
import com.sksamuel.elastic4s.json.XContentFactory
import com.sksamuel.elastic4s.{ElasticRequest, HttpResponse}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class GetSearchPipelineRequestHandlerTest
    extends AnyFlatSpec
    with SearchPipelineHandlers
    with Matchers {

  import GetSearchPipelineRequestHandler._

  it should "build a get search pipeline request" in {
    val req = GetSearchPipelineRequest("test-pipeline")

    build(req) shouldBe ElasticRequest("GET", "/_search/pipeline/test-pipeline")
  }

  it should "parse a get search pipeline response" in {
    val responseBody =
      """
        |{
        |  "nlp-search-pipeline": {
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
        |    ],
        |    "request_processors": [
        |    {
        |      "filter_query" : {
        |        "tag" : "tag1",
        |        "description" : "This processor is going to restrict to publicly visible documents",
        |        "query" : {
        |          "term": {
        |            "visibility": "public"
        |          }
        |        }
        |      }
        |    }
        |  ],
        |  "response_processors": [
        |    {
        |      "rename_field": {
        |        "field": "message",
        |        "target_field": "notification"
        |      }
        |    }
        |  ]
        |  }
        |}
        |""".stripMargin
    val response =
      HttpResponse(200, Some(StringEntity(responseBody, None)), Map.empty)

    val result = responseHandler.handle(response).right.get.data
    val expectResult = GetSearchPipelineResponse(
      data = SearchPipeline(
        "nlp-search-pipeline",
        version = Some(2332),
        description = Some("Post processor for hybrid search"),
        processors = Seq(
          NormalizationProcessor(
            normalizationTechnique = Some(NormalizationTechniqueType.minMax),
            combinationTechnique = Some(
              CombinationTechnique(
                Some(CombinationTechniqueType.arithmeticMean),
                Some(Seq(0.3, 0.7))
              )
            )
          ),
          CustomSearchPipelineProcessor(
            SearchPipelineProcessorType.SearchRequestProcessor, {
              val b = XContentFactory.jsonBuilder()
              b.startObject("filter_query")
              b.field("tag", "tag1")
              b.field(
                "description",
                "This processor is going to restrict to publicly visible documents"
              )
              b.startObject("query")
              b.startObject("term")
              b.field("visibility", "public")
              b.endObject()
              b.endObject()
              b.endObject()
              b.string
            }
          ),
          CustomSearchPipelineProcessor(
            SearchPipelineProcessorType.SearchResponseProcessor, {
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

    result.copy(processors = Seq.empty) shouldBe expectResult.data
      .copy(processors = Seq.empty)

    val resultProcessors = result.processors.map(_.toString).sorted.toList
    val expectedPRocessors = expectResult.data.processors.map(_.toString).sorted.toList

    resultProcessors shouldBe expectedPRocessors
  }

  it should "parse a get search pipeline response with minimal values" in {
    val responseBody =
      """
        |{
        |  "test-pipeline" : {
        |    "description" : "describe pipeline"
        |  }
        |}
        |""".stripMargin
    val response =
      HttpResponse(200, Some(StringEntity(responseBody, None)), Map.empty)

    responseHandler.handle(response).right.get shouldBe
      GetSearchPipelineResponse(
        data = SearchPipeline(
          "test-pipeline",
          description = Some("describe pipeline")
        )
      )
  }
}
