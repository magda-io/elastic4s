package com.sksamuel.elastic4s.requests.searchPipeline

import com.sksamuel.elastic4s._
import com.sksamuel.elastic4s.handlers.ElasticErrorParser

trait SearchPipelineHandlers {

  implicit object GetSearchPipelineRequestHandler
      extends Handler[GetSearchPipelineRequest, GetSearchPipelineResponse] {
    override def build(request: GetSearchPipelineRequest): ElasticRequest = {
      val endpoint = s"/_search/pipeline/${request.id}"
      ElasticRequest("GET", endpoint)
    }

    override def responseHandler: ResponseHandler[GetSearchPipelineResponse] =
      new ResponseHandler[GetSearchPipelineResponse] {
        override def handle(
            response: HttpResponse
        ): Either[ElasticError, GetSearchPipelineResponse] =
          response.statusCode match {
            case 200 =>
              val raw = ResponseHandler.fromResponse[Map[String, Any]](response)

              Right(
                GetSearchPipelineResponse(data =
                  SearchPipeline.fromRawResponse(raw)
                )
              )
            case _ =>
              Left(ElasticErrorParser.parse(response))
          }
      }
  }

  implicit object PutSearchPipelineRequestHandler
      extends Handler[PutSearchPipelineRequest, PutSearchPipelineResponse] {

    override def build(request: PutSearchPipelineRequest): ElasticRequest = {
      val xcb = request.data.builderFn()
      ElasticRequest(
        "PUT",
        s"/_search/pipeline/${request.data.id}",
        HttpEntity(xcb.string)
      )
    }
  }

  implicit object DeleteSearchPipelineRequestHandler
      extends Handler[
        DeleteSearchPipelineRequest,
        DeleteSearchPipelineResponse
      ] {
    override def build(request: DeleteSearchPipelineRequest): ElasticRequest = {
      val endpoint = s"/_search/pipeline/${request.id}"
      ElasticRequest("DELETE", endpoint)
    }
  }

}
