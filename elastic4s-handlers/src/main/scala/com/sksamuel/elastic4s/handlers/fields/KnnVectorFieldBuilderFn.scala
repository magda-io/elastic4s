package com.sksamuel.elastic4s.handlers.fields

import com.sksamuel.elastic4s.fields.{
  FaissEncoder,
  FaissEncoderName,
  FaissScalarQuantizationType,
  HnswParameters,
  IvfParameters,
  KnnEngine,
  KnnVectorField,
  SpaceType
}
import com.sksamuel.elastic4s.json.{XContentBuilder, XContentFactory}

object KnnVectorFieldBuilderFn {
  def toField(name: String, values: Map[String, Any]): KnnVectorField = {
    KnnVectorField(
      name,
      values.get("dimension").map(_.asInstanceOf[Int]).get,
      (values.get("method") match {
        case Some(v) =>
          val methodFields: Map[String, Any] = v.asInstanceOf[Map[String, Any]]
          val methodName = methodFields.get("name").map(_.asInstanceOf[String])
          val spaceType = methodFields
            .get("space_type")
            .flatMap(v => SpaceType.withName(v.asInstanceOf[String]))
          val engine = methodFields
            .get("engine")
            .flatMap(v => KnnEngine.withName(v.asInstanceOf[String]))

          val efSearch = methodFields
            .get("parameters")
            .flatMap(v =>
              v.asInstanceOf[Map[String, Any]]
                .get("ef_search")
                .map(_.asInstanceOf[Int])
            )
          val efConstruction = methodFields
            .get("parameters")
            .flatMap(v =>
              v.asInstanceOf[Map[String, Any]]
                .get("ef_construction")
                .map(_.asInstanceOf[Int])
            )
          val hnswM = methodFields
            .get("parameters")
            .flatMap(v =>
              v.asInstanceOf[Map[String, Any]]
                .get("m")
                .map(_.asInstanceOf[Int])
            )
          val encoder = methodFields
            .get("parameters")
            .flatMap(v =>
              v.asInstanceOf[Map[String, Any]]
                .get("encoder")
                .map(_.asInstanceOf[Map[String, Any]])
            ) map { e =>
            val name = e
              .get("name")
              .map(_.asInstanceOf[String])
              .flatMap(v => FaissEncoderName.withName(v))
            val m = e.get("m").map(_.asInstanceOf[Int])
            val codeSize = e.get("code_size").map(_.asInstanceOf[Int])
            val sqClip = e.get("clip").map(_.asInstanceOf[Boolean])
            val sqType = e
              .get("type")
              .map(_.asInstanceOf[String])
              .flatMap(v => FaissScalarQuantizationType.withName(v))
            FaissEncoder(
              name = name,
              m = m,
              codeSize = codeSize,
              sqClip = sqClip,
              sqType = sqType
            )
          }

          methodName match {
            case Some("hnsw") =>
              HnswParameters(
                engine,
                spaceType,
                m = hnswM,
                efSearch = efSearch,
                efConstruction = efConstruction,
                encoder = encoder
              )
            case Some("ivf") =>
              IvfParameters(
                engine,
                spaceType,
                encoder = encoder,
                nlist = methodFields
                  .get("parameters")
                  .flatMap(v =>
                    v.asInstanceOf[Map[String, Any]]
                      .get("nlist")
                      .map(_.asInstanceOf[Int])
                  ),
                nprobes = methodFields
                  .get("parameters")
                  .flatMap(v =>
                    v.asInstanceOf[Map[String, Any]]
                      .get("nprobes")
                      .map(_.asInstanceOf[Int])
                  )
              )
            case None =>
              HnswParameters(
                engine,
                spaceType,
                m = hnswM,
                efSearch = efSearch,
                efConstruction = efConstruction,
                encoder = encoder
              )
            case Some(invalidName) =>
              throw new IllegalArgumentException(
                s"""Invalid knn_vector field method name: ${invalidName}"""
              )
          }
        case None => HnswParameters()
      })
    )
  }

  def build(field: KnnVectorField): XContentBuilder = {

    val builder = XContentFactory.jsonBuilder()
    builder.field("type", field.`type`)
    builder.field("dimension", field.dimension)

    // start of `method` field
    builder.startObject("method")

    val parameters = field.parameters
    builder.field("name", parameters.name)
    parameters.engine.foreach(v => builder.field("engine", v.name))
    parameters.spaceType.foreach(v => builder.field("space_type", v.name))

    // start of `parameters` field
    builder.startObject("parameters")
    parameters match {
      case p: HnswParameters =>
        p.efConstruction.foreach(v => builder.field("ef_construction", v))
        p.m.foreach(v => builder.field("m", v))
        p.efSearch.foreach(v => builder.field("ef_search", v))
        p.encoder.foreach { encoder =>
          // start of `encoder` field
          builder.startObject("encoder")
          encoder.name.foreach(v => builder.field("name", v.name))

          // start of encoder `parameters` field
          builder.startObject("encoder")
          encoder match {
            case e: FaissEncoder =>
              e.m.foreach(v => builder.field("m", v))
              e.codeSize.foreach(v => builder.field("code_size", v))
              e.sqClip.foreach(v => builder.field("clip", v))
              e.sqType.foreach(v => builder.field("type", v.name))
          }
          // end of encoder `parameters` field
          builder.endObject()
          // end of `encoder` field
          builder.endObject()
        }
      case p: IvfParameters =>
        p.nlist.foreach(v => builder.field("nlist", v))
        p.nprobes.foreach(v => builder.field("nprobes", v))
        p.encoder.foreach { encoder =>
          // start of `encoder` field
          builder.startObject("encoder")
          encoder.name.foreach(v => builder.field("name", v.name))

          // start of encoder `parameters` field
          builder.startObject("encoder")
          encoder match {
            case e: FaissEncoder =>
              e.m.foreach(v => builder.field("m", v))
              e.codeSize.foreach(v => builder.field("code_size", v))
              e.sqClip.foreach(v => builder.field("clip", v))
              e.sqType.foreach(v => builder.field("type", v.name))
          }
          // end of encoder `parameters` field
          builder.endObject()
          // end of `encoder` field
          builder.endObject()
        }
    }
    // end of `parameters` field
    builder.endObject()

    // end of `method` field
    builder.endObject()
    builder.endObject()
  }
}
