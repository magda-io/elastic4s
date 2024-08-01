package com.sksamuel.elastic4s.requests.searchPipeline

case class GetSearchPipelineResponse(id: String, description: String, version: Option[Int], processors: Seq[SearchPipelineProcessor])
