package com.sksamuel.elastic4s.requests.searchPipeline

import com.sksamuel.elastic4s.HttpEntity.StringEntity
import com.sksamuel.elastic4s.{ElasticRequest, HttpResponse}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DeleteSearchPipelineRequestHandlerTest extends AnyFlatSpec with SearchPipelineHandlers with Matchers {

  import DeleteSearchPipelineRequestHandler._

  it should "build a delete search pipeline" in {
    val req = DeleteSearchPipelineRequest("test-pipeline")

    build(req) shouldBe ElasticRequest("DELETE", "/_search/pipeline/test-pipeline")
  }

  it should "parse a delete pipeline response" in {
    val responseBody =
      """
        |{
        |  "acknowledged" : true
        |}
        |""".stripMargin
    val response = HttpResponse(200, Some(StringEntity(responseBody, None)), Map.empty)

    responseHandler.handle(response).right.get shouldBe DeleteSearchPipelineResponse(true)
  }
}
