package com.sksamuel.elastic4s.requests.searchPipeline

import com.sksamuel.elastic4s.json.{XContentBuilder, XContentFactory}

case class SearchPipeline(
    id: String,
    version: Option[Int] = None,
    description: Option[String] = None,
    phaseResultsProcessors: Seq[SearchPipelineProcessor] = Seq(),
    requestProcessors: Seq[SearchPipelineProcessor] = Seq(),
    responseProcessors: Seq[SearchPipelineProcessor] = Seq()
) {
  def builderFn(): XContentBuilder = {
    val xcb = XContentFactory.jsonBuilder()
    version.foreach(v => xcb.field("version", v))
    description.foreach(v => xcb.field("description", v))
    if (phaseResultsProcessors.length > 0) {
      xcb.array(
        "phase_results_processors",
        phaseResultsProcessors.map(_.builderFn()).toArray
      )
    }
    if (requestProcessors.length > 0) {
      xcb.array(
        "request_processors",
        requestProcessors.map(_.builderFn()).toArray
      )
    }
    if (responseProcessors.length > 0) {
      xcb.array(
        "response_processors",
        responseProcessors.map(_.builderFn()).toArray
      )
    }
    xcb
  }
}

object SearchPipeline {
  def apply(
      id: String,
      version: Option[Int] = None,
      description: Option[String] = None,
      phaseResultsProcessors: Seq[SearchPipelineProcessor] = Seq(),
      requestProcessors: Seq[SearchPipelineProcessor] = Seq(),
      responseProcessors: Seq[SearchPipelineProcessor] = Seq()
  ): SearchPipeline = {
    if (
      phaseResultsProcessors.length + requestProcessors.length + responseProcessors.length == 0
    ) {
      throw new IllegalArgumentException(
        "SearchPipeline should contain at least one processor"
      )
    }
    new SearchPipeline(
      id,
      version,
      description,
      phaseResultsProcessors,
      requestProcessors,
      responseProcessors
    )
  }

  def fromRawResponse(resp: Map[String, Any]): SearchPipeline = {
    val (id, pipelineData) = resp.head
    val data = pipelineData.asInstanceOf[Map[String, Any]]
    SearchPipeline(
      id = id,
      version = data.get("version").map(_.asInstanceOf[Int]),
      description = data.get("description").map(_.asInstanceOf[String]),
      phaseResultsProcessors = data.get("phase_results_processors").toSeq.flatMap{
        case items: Map[String, Any] => Seq()
        case _ => Seq()
      }
    )
  }

}
