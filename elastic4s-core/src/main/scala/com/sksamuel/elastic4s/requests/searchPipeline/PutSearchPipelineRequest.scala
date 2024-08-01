package com.sksamuel.elastic4s.requests.searchPipeline

case class PutSearchPipelineRequest(id: String,
                              description: String,
                              processors: Seq[SearchPipelineProcessor] = Seq.empty,
                              version: Option[Int] = None)

object PutSearchPipelineRequest {

  def apply(id: String, description: String, processor: SearchPipelineProcessor): PutSearchPipelineRequest =
    PutSearchPipelineRequest(id, description, Seq(processor))
}
