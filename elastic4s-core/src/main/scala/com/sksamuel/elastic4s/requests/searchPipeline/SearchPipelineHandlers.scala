package com.sksamuel.elastic4s.requests.searchPipeline

import com.sksamuel.elastic4s.json.{XContentBuilder, XContentFactory}
import com.sksamuel.elastic4s._
import com.sksamuel.elastic4s.handlers.ElasticErrorParser


trait SearchPipelineHandlers {

  implicit object GetSearchPipelineRequestHandler extends Handler[GetSearchPipelineRequest, GetSearchPipelineResponse] {
    override def build(request: GetSearchPipelineRequest): ElasticRequest = {
      val endpoint = s"/_ingest/pipeline/${request.id}"
      ElasticRequest("GET", endpoint)
    }

    override def responseHandler: ResponseHandler[GetSearchPipelineResponse] = new ResponseHandler[GetSearchPipelineResponse] {
      override def handle(response: HttpResponse): Either[ElasticError, GetSearchPipelineResponse] = response.statusCode match {
        case 200 =>
          val raw = ResponseHandler.fromResponse[Map[String, Map[String, Any]]](response)
          val resp = raw.map { case (id, types) =>
            GetSearchPipelineResponse(
              id,
              types("description").asInstanceOf[String],
              types.get("version").asInstanceOf[Option[Int]],
              types("processors").asInstanceOf[Seq[Map[String, Map[String, Any]]]].map { processor =>
                val name = processor.keys.head
                name match {
                  case GeoIPProcessor.name =>
                    val mapping = processor(name)
                    GeoIPProcessor(
                      mapping("field").asInstanceOf[String],
                      mapping.get("target_field").asInstanceOf[Option[String]],
                      mapping.get("database_file").asInstanceOf[Option[String]],
                      mapping.get("properties").asInstanceOf[Option[Seq[String]]],
                      mapping.get("ignore_missing").asInstanceOf[Option[Boolean]],
                      mapping.get("first_only").asInstanceOf[Option[Boolean]]
                    )
                  case _ =>
                    val b = XContentFactory.jsonBuilder()
                    processor(name).foreach { case (k, v) =>
                      b.autofield(k, v)
                    }
                    b.endObject()
                    CustomSearchPipelineProcessor(name, b.string)
                }
              }
            )
          }
          Right(resp.head)
        case _ =>
          Left(ElasticErrorParser.parse(response))
      }
    }
  }

  implicit object PutPipelineRequestHandler extends Handler[PutSearchPipelineRequest, PutSearchPipelineResponse] {
    private def processorToXContent(p: SearchPipelineProcessor): XContentBuilder = {
      val xcb = XContentFactory.jsonBuilder()
      xcb.rawField(p.name, p.buildProcessorBody())
      xcb
    }

    override def build(request: PutSearchPipelineRequest): ElasticRequest = {
      val xcb = XContentFactory.jsonBuilder()
      xcb.field("description", request.description)
      request.version.map(xcb.field("version", _))
      xcb.array("processors", request.processors.map(processorToXContent).toArray)
      xcb.endObject()
      ElasticRequest("PUT", s"/_ingest/pipeline/${request.id}", HttpEntity(xcb.string))
    }
  }

  implicit object DeletePipelineRequestHandler extends Handler[DeleteSearchPipelineRequest, DeleteSearchPipelineResponse] {
    override def build(request: DeleteSearchPipelineRequest): ElasticRequest = {
      val endpoint = s"/_ingest/pipeline/${request.id}"
      ElasticRequest("DELETE", endpoint)
    }
  }

}
