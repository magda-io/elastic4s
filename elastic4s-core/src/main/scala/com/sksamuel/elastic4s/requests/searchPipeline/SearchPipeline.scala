package com.sksamuel.elastic4s.requests.searchPipeline

import com.sksamuel.elastic4s.json.{XContentBuilder, XContentFactory}

case class SearchPipeline(
    id: String,
    version: Option[Int] = None,
    description: Option[String] = None,
    processors: Seq[SearchPipelineProcessor] = Seq()
) {
  def builderFn(): XContentBuilder = {
    val xcb = XContentFactory.jsonBuilder()
    version.foreach(v => xcb.field("version", v))
    description.foreach(v => xcb.field("description", v))
    val phaseResultsProcessors = processors.filter(p =>
      p.processorType == SearchPipelineProcessorType.SearchPhaseResultsProcessor
    )
    val requestProcessors = processors.filter(p =>
      p.processorType == SearchPipelineProcessorType.SearchRequestProcessor
    )
    val responseProcessors = processors.filter(p =>
      p.processorType == SearchPipelineProcessorType.SearchResponseProcessor
    )
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
  def fromRawResponse(resp: Map[String, Any]): SearchPipeline = {
    val (id, pipelineData) = resp.head
    val data = pipelineData.asInstanceOf[Map[String, Any]]
    val processors = SearchPipelineProcessorType.values.flatMap { pType =>
      data
        .get(pType.name)
        .toSeq
        .flatMap(_.asInstanceOf[Seq[Any]])
        .map(v =>
          SearchPipelineProcessor
            .fromRawResponse(v.asInstanceOf[Map[String, Any]], pType)
        )
    }.toSeq
    SearchPipeline(
      id = id,
      version = data.get("version").map(_.asInstanceOf[Int]),
      description = data.get("description").map(_.asInstanceOf[String]),
      processors = processors
    )
  }
}
