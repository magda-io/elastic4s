package com.sksamuel.elastic4s.requests.searchPipeline

import com.sksamuel.elastic4s.json.{XContentBuilder, XContentFactory}

sealed trait SearchPipelineProcessorType {
  def name: String
}

object SearchPipelineProcessorType {
  val values = Set(
    SearchRequestProcessor,
    SearchResponseProcessor,
    SearchPhaseResultsProcessor
  )

  def withName(name: String): Option[SearchPipelineProcessorType] =
    values.map(v => v.name -> v).toMap.get(name)

  case object SearchRequestProcessor extends SearchPipelineProcessorType {
    val name = "request_processors"
  }
  case object SearchResponseProcessor extends SearchPipelineProcessorType {
    val name = "response_processors"
  }
  case object SearchPhaseResultsProcessor extends SearchPipelineProcessorType {
    val name = "phase_results_processors"
  }
}

/** Abstract representation of a [search pipeline processor](https://opensearch.org/docs/latest/search-plugins/search-pipelines/index/)
  * there are three types: request_processors, response_processors & phase_results_processors
  */
trait SearchPipelineProcessor {
  def processorType: SearchPipelineProcessorType
  def tag: Option[String]
  def description: Option[String]
  def ignoreFailure: Option[Boolean]
  def builderFn(): XContentBuilder
}

object SearchPipelineProcessor {
  def fromRawResponse(
      resp: Map[String, Any],
      processorType: SearchPipelineProcessorType
  ): SearchPipelineProcessor = {
    resp.head._1 match {
      case "normalization-processor" =>
        NormalizationProcessor.fromRawResponse(resp)
      case _ =>
        CustomSearchPipelineProcessor.fromRawResponse(resp, processorType)
    }
  }
}

/** Processor defined by its name, type and raw Json options.
  */
case class CustomSearchPipelineProcessor(
    processorType: SearchPipelineProcessorType,
    rawJsonOptions: String
) extends SearchPipelineProcessor {
  // for CustomSearchPipelineProcessor, tag, description & ignoreFailure will be supplied via `rawJsonOptions`
  val tag = None
  val description = None
  val ignoreFailure = None
  def builderFn(): XContentBuilder = XContentFactory.parse(rawJsonOptions)
}

object CustomSearchPipelineProcessor {
  def fromRawResponse(
      resp: Map[String, Any],
      processorType: SearchPipelineProcessorType
  ): CustomSearchPipelineProcessor = {
    val (id, pipelineData) = resp.head
    val builder = XContentFactory.jsonBuilder()
    builder.autofield(id, pipelineData)
    CustomSearchPipelineProcessor(
      processorType = processorType,
      rawJsonOptions = builder.string
    )
  }
}
