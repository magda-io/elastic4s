package com.sksamuel.elastic4s.requests.searchPipeline

import com.sksamuel.elastic4s.json.{XContentBuilder, XContentFactory}

sealed trait NormalizationTechniqueType {
  def name: String
}

object NormalizationTechniqueType {

  val defaultValue: NormalizationTechniqueType = minMax

  val values = Set(minMax, l2)

  def withName(name: String): Option[NormalizationTechniqueType] =
    values.map(v => v.name -> v).toMap.get(name)

  case object minMax extends NormalizationTechniqueType {
    val name = "min_max"
  }

  case object l2 extends NormalizationTechniqueType {
    val name = "l2"
  }
}

sealed trait CombinationTechniqueType {
  def name: String
}

object CombinationTechniqueType {

  val defaultValue: CombinationTechniqueType = arithmeticMean

  val values = Set(arithmeticMean, geometricMean, harmonicNean)

  def withName(name: String): Option[CombinationTechniqueType] =
    values.map(v => v.name -> v).toMap.get(name)

  case object arithmeticMean extends CombinationTechniqueType {
    val name = "arithmetic_mean"
  }

  case object geometricMean extends CombinationTechniqueType {
    val name = "geometric_mean"
  }

  case object harmonicNean extends CombinationTechniqueType {
    val name = " harmonic_mean"
  }
}

case class CombinationTechnique(
    techniqueType: Option[CombinationTechniqueType] = None,
    weights: Option[Seq[Float]] = None
)

case class NormalizationProcessor(
    normalizationTechnique: Option[NormalizationTechniqueType] = None,
    combinationTechnique: Option[CombinationTechnique] = None,
    tag: Option[String] = None,
    description: Option[String] = None
) extends SearchPipelineProcessor {
  val processorType = SearchPipelineProcessorType.SearchPhaseResultsProcessor
  // for NormalizationProcessor, this value is ignored. If the processor fails, the pipeline always fails and returns an error.
  val ignoreFailure = None

  def builderFn(): XContentBuilder = {
    val xcb = XContentFactory.jsonBuilder()
    xcb.startObject("normalization-processor")
    tag.foreach(v => xcb.field("tag", v))
    description.foreach(v => xcb.field("description", v))
    normalizationTechnique.foreach(v => {
      xcb.startObject("normalization")
      xcb.field("technique", v.name)
      xcb.endObject()
    })
    combinationTechnique.foreach(ct => {
      xcb.startObject("combination")
      ct.techniqueType.foreach(t => xcb.field("technique", t.name))
      if (ct.weights.nonEmpty && ct.weights.get.length > 0) {
        xcb.startObject("parameters")
        xcb.array("weights", ct.weights.get.toArray)
        xcb.endObject()
      }
      xcb.endObject()
    })
    xcb.endObject()
  }
}

object NormalizationProcessor {
  def fromRawResponse(resp: Map[String, Any]): NormalizationProcessor = {
    if (
      resp.isEmpty || resp.get("normalization-processor").isEmpty || !resp
        .get("normalization-processor")
        .isInstanceOf[Map[String, Any]]
    ) {
      NormalizationProcessor()
    } else {
      val pData =
        resp.get("normalization-processor").asInstanceOf[Map[String, Any]]
      NormalizationProcessor(
        tag = pData.get("tag").map(_.asInstanceOf[String]),
        description = pData.get("description").map(_.asInstanceOf[String]),
        normalizationTechnique = pData
          .get("normalization")
          .flatMap {
            case v: Map[String, String] =>
              v.get("technique")
            case _ => None.asInstanceOf[Option[String]]
          }
          .flatMap(NormalizationTechniqueType.withName(_)),
        combinationTechnique = pData
          .get("combination")
          .flatMap {
            case v: Map[String, Any] =>
              Some(
                CombinationTechnique(
                  techniqueType = v.get("technique").flatMap {
                    case v: String => CombinationTechniqueType.withName(v)
                    case _         => None
                  },
                  weights = v.get("parameters").flatMap {
                    case p: Map[String, Any] =>
                      p.get("weights").map {
                        case ws: Seq[Any] => ws.map(_.asInstanceOf[Float])
                        case _            => Seq()
                      }
                    case _ => None
                  }
                )
              )
            case _ => None
          }
      )
    }
  }
}
